package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	regcore "github.com/oekit/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Subscriber 是 regcore.Subscriber 的 etcd 实现：Watch 服务前缀，任一实例
// 键增删改后重新拉取全量快照回调。订阅建立时立即回调一次当前快照。
type Subscriber struct {
	cli *clientv3.Client
	opt *Option

	mu   sync.Mutex
	subs map[string]context.CancelFunc // 服务名 → 取消函数
}

var _ regcore.Subscriber = (*Subscriber)(nil)

// NewSubscriber 创建 etcd 订阅器。
//
// client 为 nil 时返回 ErrNilClient（构造期 fail-fast，避免首次订阅时在
// cli.Get 上空指针崩溃）。
func NewSubscriber(cli *clientv3.Client, opts ...OptionFunc) (*Subscriber, error) {
	if cli == nil {
		return nil, ErrNilClient
	}
	return &Subscriber{cli: cli, opt: applyOptions(opts), subs: make(map[string]context.CancelFunc)}, nil
}

// Subscribe 订阅服务实例变更：先同步回调一次当前快照，随后后台 Watch 推送。
// 回调在订阅专属 goroutine 中串行执行（不会并发调用 cb），回调内阻塞会
// 延迟后续事件处理。同一服务重复订阅会替换旧回调。订阅生命周期与传入
// ctx 无关，由 Unsubscribe 终止。
func (s *Subscriber) Subscribe(ctx context.Context, serviceName string, cb func([]regcore.Instance)) error {
	if serviceName == "" {
		return fmt.Errorf("etcdx registry: service name is empty")
	}
	if cb == nil {
		return fmt.Errorf("etcdx registry: subscribe callback is nil")
	}
	dir := serviceDir(s.opt.Prefix, s.opt.Group, serviceName)

	// 先取一次快照并记录 revision，Watch 从 revision+1 起不漏事件
	resp, err := s.cli.Get(ctx, dir, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("etcdx registry: subscribe get %s: %w", serviceName, err)
	}
	startRev := resp.Header.Revision + 1

	wctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	if old := s.subs[serviceName]; old != nil {
		old()
	}
	s.subs[serviceName] = cancel
	s.mu.Unlock()

	cb(decodeAll(serviceName, s.opt.Group, resp))

	// 观察循环（#55）：watch 被取消（ErrCompacted——长驻订阅 + auto-compaction
	// 是生产常态；etcd-server 重启等）或通道关闭后**指数退避重建**（重取快照 +
	// 新 revision），对齐注册侧「丢约指数退避自动重注册」的对等韧性。此前只打
	// 一条 Warn 就 return：订阅留在 subs map、Unsubscribe「正常」返回，但此后
	// 永不回调——服务实例列表/配置推送几天后无声停更，消费方拿过期快照继续跑。
	async.GoSafe(func() {
		const maxBackoff = 30 * time.Second
		backoff := time.Second
		rev := startRev
		for wctx.Err() == nil {
			wch := s.cli.Watch(wctx, dir, clientv3.WithPrefix(), clientv3.WithRev(rev))
			canceled := false
			for wresp := range wch {
				if wresp.Canceled {
					// Compacted 时 WithRev(startRev) 会被服务端取消——
					// clientv3 不自动重建，必须自己重建
					log.Warn("watch canceled, rebuilding with exponential backoff",
						"service", serviceName, "err", wresp.Err())
					canceled = true
					break
				}
				if len(wresp.Events) == 0 {
					continue
				}
				insts, err := fetchInstances(wctx, s.cli, s.opt.Prefix, s.opt.Group, serviceName)
				if err != nil {
					if wctx.Err() != nil {
						return
					}
					log.Warn("refresh after watch event failed", "service", serviceName, "err", err)
					continue
				}
				cb(insts)
			}
			if wctx.Err() != nil {
				return // Unsubscribe/Close：正常退出，不重建
			}
			if !canceled && wch != nil {
				// 通道关闭且未见 Canceled（client 断连等）：同样走重建
				log.Warn("watch channel closed, rebuilding", "service", serviceName)
			}
			// 退避等待（可被取消打断），重取快照拿新 revision，避免重建即 Compacted
			select {
			case <-wctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			resp, err := s.cli.Get(wctx, dir, clientv3.WithPrefix())
			if err != nil {
				log.Warn("snapshot refresh for watch rebuild failed, retrying",
					"service", serviceName, "err", err)
				continue // 快照失败：下轮再试（rev 不变，可能仍 Compacted，会再退避）
			}
			rev = resp.Header.Revision + 1
			// 重建成功前先回调一次快照（期间可能有实例变更，全量刷新一次视图）
			cb(decodeAll(serviceName, s.opt.Group, resp))
		}
	})
	return nil
}

// Unsubscribe 取消订阅（未订阅为 no-op）。
func (s *Subscriber) Unsubscribe(_ context.Context, serviceName string) error {
	s.mu.Lock()
	cancel := s.subs[serviceName]
	delete(s.subs, serviceName)
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// Close 取消全部订阅。
func (s *Subscriber) Close(_ context.Context) error {
	s.mu.Lock()
	subs := s.subs
	s.subs = make(map[string]context.CancelFunc)
	s.mu.Unlock()
	for _, cancel := range subs {
		cancel()
	}
	return nil
}

func decodeAll(service, group string, resp *clientv3.GetResponse) []regcore.Instance {
	out := make([]regcore.Instance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		inst, err := decodeInstance(service, group, kv.Value)
		if err != nil {
			log.Warn("skip malformed instance", "key", string(kv.Key), "err", err)
			continue
		}
		out = append(out, inst)
	}
	return out
}
