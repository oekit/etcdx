package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/oekit/concurrency/async"
	regcore "github.com/oekit/registry/registry"
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
func NewSubscriber(cli *clientv3.Client, opts ...OptionFunc) *Subscriber {
	return &Subscriber{cli: cli, opt: applyOptions(opts), subs: make(map[string]context.CancelFunc)}
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

	async.GoSafe(func() {
		wch := s.cli.Watch(wctx, dir, clientv3.WithPrefix(), clientv3.WithRev(startRev))
		for wresp := range wch {
			if wresp.Canceled {
				log.Warn("watch canceled", "service", serviceName, "err", wresp.Err())
				return
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
