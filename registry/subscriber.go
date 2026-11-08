package registry

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	subs map[string]*watchHandle // 服务名 → 订阅句柄
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
	return &Subscriber{cli: cli, opt: applyOptions(opts), subs: make(map[string]*watchHandle)}, nil
}

// Subscribe 订阅服务实例变更：订阅建立后先回调一次当前快照，随后后台 Watch
// 推送增量——首次快照与后续事件走同一条订阅 goroutine 串行投递路径（#84）。
// 回调在订阅专属 goroutine 中串行执行（**不会并发调用 cb**），回调内阻塞会
// 延迟后续事件处理。同一服务重复订阅会替换旧订阅：旧观察 goroutine 被标记
// 取代并取消，其未投递的过期快照被丢弃；新订阅等旧 goroutine 退出后才开始
// 投递，新旧回调从不错峰并发（#84）。替换不阻塞 Subscribe 调用方，但旧回调
// 永久阻塞会拖住新订阅的首次投递——不要在回调内阻塞，也不要在回调内对同一
// 服务再次 Subscribe。订阅生命周期与传入 ctx 无关，由 Unsubscribe 终止。
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

	h := newWatchHandle()
	s.mu.Lock()
	old := s.subs[serviceName]
	if old != nil {
		old.supersede() // 标记取代 + cancel：旧 goroutine 不再投递（仅 cancel 不交接，#84）
	}
	s.subs[serviceName] = h
	s.mu.Unlock()

	// 观察循环（#55）：watch 被取消（ErrCompacted——长驻订阅 + auto-compaction
	// 是生产常态；etcd-server 重启等）或通道关闭后**指数退避重建**（重取快照 +
	// 新 revision），对齐注册侧「丢约指数退避自动重注册」的对等韧性。此前只打
	// 一条 Warn 就 return：订阅留在 subs map、Unsubscribe「正常」返回，但此后
	// 永不回调——服务实例列表/配置推送几天后无声停更，消费方拿过期快照继续跑。
	//
	// #84：首次快照投递前先等被替换的旧 goroutine 退出（回调不可能并发），
	// 之后所有投递前检查 superseded，被取代/退订后的过期快照一律丢弃。
	async.GoSafe(func() {
		defer h.close()
		if old != nil {
			select {
			case <-old.done: // 旧 goroutine 已退出：此后才允许本订阅投递
			case <-h.ctx.Done():
				return // 等待期间本订阅又被取代/退订：不投递
			}
		}
		if h.superseded.Load() {
			return
		}
		// 首次快照：与后续事件同一条串行路径（#84），不再是「同步第一次 +
		// 异步后续」两条交错路径
		cb(decodeAll(serviceName, s.opt.Group, resp))

		// 退避由**连续重建失败次数**派生（#85）：每轮快照重取失败 attempts++，
		// 成功即归零——「重建成功」这个事实在结构上无法被忘记。旧实现把
		// backoff 声明在循环外只增不减，成功重建后仍按累计值退避，几次抖动
		// 后涨满 30s，此后任意一次瞬时中断都要吃满最长窗口才恢复推送，
		// 韧性机制自身成了延迟源（实例下线最多 30s 才被订阅方看到）。
		attempts := 0
		rev := startRev
		for h.ctx.Err() == nil {
			wch := s.cli.Watch(h.ctx, dir, clientv3.WithPrefix(), clientv3.WithRev(rev))
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
				insts, err := fetchInstances(h.ctx, s.cli, s.opt.Prefix, s.opt.Group, serviceName)
				if err != nil {
					if h.ctx.Err() != nil {
						return
					}
					log.Warn("refresh after watch event failed", "service", serviceName, "err", err)
					continue
				}
				if h.superseded.Load() {
					return // 已被新订阅/退订取代：过期投递一律丢弃（#84）
				}
				cb(insts)
			}
			if h.ctx.Err() != nil {
				return // Unsubscribe/Close：正常退出，不重建
			}
			if !canceled && wch != nil {
				// 通道关闭且未见 Canceled（client 断连等）：同样走重建
				log.Warn("watch channel closed, rebuilding", "service", serviceName)
			}
			// 退避等待（可被取消打断），重取快照拿新 revision，避免重建即 Compacted
			select {
			case <-h.ctx.Done():
				return
			case <-time.After(watchBackoff(attempts)):
			}
			resp, err := s.cli.Get(h.ctx, dir, clientv3.WithPrefix())
			if err != nil {
				attempts++
				log.Warn("snapshot refresh for watch rebuild failed, retrying",
					"service", serviceName, "err", err, "attempts", attempts)
				continue // 快照失败：下轮再试（rev 不变，可能仍 Compacted，会再退避）
			}
			rev = resp.Header.Revision + 1
			attempts = 0 // 重建成功：退避归零（#85），单次瞬时中断不吃历史累计窗口
			if h.superseded.Load() {
				return
			}
			// 重建成功前先回调一次快照（期间可能有实例变更，全量刷新一次视图）
			cb(decodeAll(serviceName, s.opt.Group, resp))
		}
	})
	return nil
}

// Unsubscribe 取消订阅（未订阅为 no-op）。
func (s *Subscriber) Unsubscribe(_ context.Context, serviceName string) error {
	s.mu.Lock()
	h := s.subs[serviceName]
	delete(s.subs, serviceName)
	s.mu.Unlock()
	if h != nil {
		h.supersede()
	}
	return nil
}

// Close 取消全部订阅。
func (s *Subscriber) Close(_ context.Context) error {
	s.mu.Lock()
	subs := s.subs
	s.subs = make(map[string]*watchHandle)
	s.mu.Unlock()
	for _, h := range subs {
		h.supersede()
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

// watchHandle 记录一个活跃 watch 订阅（Subscriber 与 ConfigStore 共用）：
// cancel 终止 watch；done 在观察 goroutine 退出时关闭——替换方以此建立
// 「旧 goroutine 不会再回调」的事实（#84：cancel 只会打断阻塞中的 Watch/
// Get，不会等待正在执行的回调返回）；superseded 标记本订阅已被同 key 的新
// 订阅或停止操作取代，被取代的 goroutine 丢弃一切未投递的快照直接退出。
type watchHandle struct {
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	superseded atomic.Bool
}

func newWatchHandle() *watchHandle {
	ctx, cancel := context.WithCancel(context.Background())
	return &watchHandle{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// supersede 标记本订阅被取代：丢弃后续投递并终止观察 goroutine。
func (h *watchHandle) supersede() {
	h.superseded.Store(true)
	h.cancel()
}

// close 由观察 goroutine defer 调用：退出即关闭 done，交接给新订阅。
func (h *watchHandle) close() {
	close(h.done)
}

// watch 重建的退避参数（Subscriber 与 ConfigStore 共用口径，#85）。
const (
	watchBackoffBase  = time.Second // 首轮退避
	watchBackoffMax   = 30 * time.Second
	watchBackoffSteps = 5 // 1s<<5 = 32s ≥ 上限，attempts 封顶即不再增长
)

// watchBackoff 由连续重建失败次数派生单轮退避：min(上限, 基数<<attempts)。
// 成功重建后调用方将 attempts 归零 ⇒ 退避随之归零（#85）。
func watchBackoff(attempts int) time.Duration {
	if attempts > watchBackoffSteps {
		attempts = watchBackoffSteps
	}
	d := watchBackoffBase << uint(attempts)
	if d > watchBackoffMax {
		return watchBackoffMax
	}
	return d
}
