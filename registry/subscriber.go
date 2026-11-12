package registry

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
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

	// retired 承载已 Unsubscribe/Close 摘除、但观察 goroutine 尚未退出的句柄
	// （#149）：#84 的交接（新订阅等旧 goroutine 退出后才投递）不能依赖
	// 「旧句柄还在 subs 里」——先删 map 再 supersede 的旧实现让退订后重建的
	// 订阅读到 old==nil，交接整段被绕过，新旧回调并发。同一名字至多存在于
	// subs/retired 之一（Subscribe 转移时原子完成）；槽位由观察 goroutine
	// 退出时按身份自清（dropRetiredHandle），无订阅重建时也不残留。
	retired map[string]*watchHandle
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
	return &Subscriber{
		cli: cli, opt: applyOptions(opts),
		subs: make(map[string]*watchHandle), retired: make(map[string]*watchHandle),
	}, nil
}

// Subscribe 订阅服务实例变更：订阅建立后先回调一次当前快照，随后后台 Watch
// 推送增量——首次快照与后续事件走同一条订阅 goroutine 串行投递路径（#84）。
// 回调在订阅专属 goroutine 中串行执行（**不会并发调用 cb**），回调内阻塞会
// 延迟后续事件处理。cb panic 不会终结观察循环：panic 被就地 recover 转
// Error 日志（含调用栈），本次投递跳过，后续变更照常推送（#87）。同一服务
// 重复订阅会替换旧订阅：旧观察 goroutine 被标记
// 取代并取消，其未投递的过期快照被丢弃；新订阅等旧 goroutine 退出后才开始
// 投递，新旧回调从不错峰并发（#84）。替换不阻塞 Subscribe 调用方，但旧回调
// 永久阻塞会拖住新订阅的首次投递——不要在回调内阻塞，也不要在回调内对同一
// 服务再次 Subscribe。订阅生命周期与传入 ctx 无关，由 Unsubscribe 终止。
// 退订（Unsubscribe）或 Close 之后立即重建订阅同样遵守 #84 交接：被摘除的
// 旧句柄保留在 retired 待退出槽位，新订阅仍能等到其退出后才投递（#149——
// 此前先删 map 再 supersede，重建订阅读到 old==nil，交接被绕过）。
func (s *Subscriber) Subscribe(ctx context.Context, serviceName string, cb func([]regcore.Instance)) error {
	if serviceName == "" {
		return fmt.Errorf("etcdx registry: service name is empty")
	}
	// 键层级边界 fail-fast（#110）：含 "/" 的服务/分组名拼出的 watch 前缀
	// 会越界命中其他服务的键空间，快照里混入解码成 Service="order" 的
	// order/v2 实例
	if err := checkName("service", serviceName); err != nil {
		return err
	}
	if err := checkName("group", s.opt.Group); err != nil {
		return err
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
	if old == nil {
		// 退订/关停后的重建订阅（#149）：旧句柄已从 subs 摘除但可能仍在
		// 退出中，交接等待改读 retired 槽位——「先删 map」不得让 #84 的
		// 等旧 goroutine 退出整段失效
		old = s.retired[serviceName]
		delete(s.retired, serviceName)
	}
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
		defer func() {
			h.close()
			// #149：观察 goroutine 退出后从 retired 槽位摘除自己（身份
			// 校验：名字可能已被后续订阅/退订重新占用）。先 close(done) 再
			// 摘除，保持「槽位还在 ⇒ goroutine 可能仍在回调」的不变式。
			s.dropRetired(serviceName, h)
		}()
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
		// 异步后续」两条交错路径。cb panic 由 safeNotify 就地收编（#87）。
		snap := decodeAll(serviceName, s.opt.Group, dir, resp)
		safeNotify(serviceName, func() { cb(snap) })

		// 观察循环骨架单源见 runWatchLoop（#122）：Canceled/通道关闭 → 指数
		// 退避重建（重取快照 + 新 revision）→ 成功补投递一次快照。退避由
		// 连续重建失败次数派生（#85），成功即归零。本侧差异点：事件批到达
		// 后全量重拉实例（fetchInstances）再投递，取数窗口内被取代的过期
		// 快照丢弃（#84）；cb panic 由 safeNotify 就地收编（#87），不炸穿
		// 重建循环。
		runWatchLoop(
			h,
			serviceName,
			startRev,
			func(ctx context.Context, rev int64) clientv3.WatchChan {
				return s.cli.Watch(ctx, dir, clientv3.WithPrefix(), clientv3.WithRev(rev))
			},
			func(wresp clientv3.WatchResponse) {
				if len(wresp.Events) == 0 {
					return
				}
				insts, err := fetchInstances(h.ctx, s.cli, s.opt.Prefix, s.opt.Group, serviceName)
				if err != nil {
					if h.ctx.Err() != nil {
						return // 取数窗口内被退订：runWatchLoop 统一退出
					}
					log.Warn("refresh after watch event failed", "service", serviceName, logx.ErrKey, err)
					return
				}
				if h.superseded.Load() {
					return // 取数窗口内被取代：过期快照丢弃（#84）
				}
				safeNotify(serviceName, func() { cb(insts) }) // cb panic 不终结循环（#87）
			},
			func(ctx context.Context) (int64, func(), error) {
				resp, err := s.cli.Get(ctx, dir, clientv3.WithPrefix())
				if err != nil {
					return 0, nil, err
				}
				snap := decodeAll(serviceName, s.opt.Group, dir, resp)
				return resp.Header.Revision + 1,
					func() { safeNotify(serviceName, func() { cb(snap) }) },
					nil
			},
		)
	})
	return nil
}

// Unsubscribe 取消订阅（未订阅为 no-op）。
//
// 摘除的句柄先进入 retired 待退出槽位而非直接丢弃（#149）：其观察 goroutine
// 可能仍在执行消费方回调，后续同服务 Subscribe 靠该槽位拿到 old.done 完成
// #84 交接——先删 map 再 supersede 会让交接整段失效（退订后重建订阅与旧回调
// 并发）。本方法不等待旧回调返回（不阻塞退订方）；槽位由观察 goroutine 退出
// 时自清，不会随退订次数累积。
func (s *Subscriber) Unsubscribe(_ context.Context, serviceName string) error {
	s.mu.Lock()
	h := s.subs[serviceName]
	if h != nil {
		delete(s.subs, serviceName)
		s.retired[serviceName] = h // #149：摘除后仍可被重建订阅读到（同名至多一个）
	}
	s.mu.Unlock()
	if h != nil {
		h.supersede()
	}
	return nil
}

// Close 取消全部订阅。
//
// 被取消的句柄同样转入 retired 待退出槽位（#149，与 Unsubscribe 同口径）：
// Close 之后立即重建订阅仍须等旧 goroutine 退出，回调不并发。
func (s *Subscriber) Close(_ context.Context) error {
	s.mu.Lock()
	subs := s.subs
	s.subs = make(map[string]*watchHandle)
	for name, h := range subs {
		s.retired[name] = h // subs/retired 同名不相交（见 retired 字段注释）
	}
	s.mu.Unlock()
	for _, h := range subs {
		h.supersede()
	}
	return nil
}

// dropRetired 观察 goroutine 退出时把自身从 retired 槽位摘除（身份校验：
// 非本句柄则不动——名字可能已被后续订阅/退订重新占用）。Subscriber 与
// ConfigStore 各持自己的 map，逻辑同构（#149）。
func (s *Subscriber) dropRetired(name string, h *watchHandle) {
	s.mu.Lock()
	if s.retired[name] == h {
		delete(s.retired, name)
	}
	s.mu.Unlock()
}

func decodeAll(service, group, dir string, resp *clientv3.GetResponse) []regcore.Instance {
	out := make([]regcore.Instance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if !validInstanceKey(kv.Key, dir) {
			log.Warn("skip out-of-boundary instance key (legacy slash name)",
				"key", string(kv.Key), "dir", dir)
			continue
		}
		inst, err := decodeInstance(service, group, kv.Value)
		if err != nil {
			log.Warn("skip malformed instance", "key", string(kv.Key), logx.ErrKey, err)
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

// safeNotify 就地收编消费者回调的 panic（#87）：观察循环跑在 async.GoSafe
// 的 goroutine 里，其兜底 recover 之后 goroutine 直接退出——回调一次 panic
// 就杀死整条观察循环（#55/#64 修出的重建、退避、快照投递全部失效），订阅
// 静默死亡而 Unsubscribe/StopListenConfig 仍返回 nil。观察循环语义天然允许
// 「跳过一次投递」：panic 转 Error 日志（含调用栈）后循环继续，后续变更
// 照常投递。target 仅用于日志定位（服务名 / 配置 dataID）。
// Subscriber 与 ConfigStore 共用。
func safeNotify(target string, notify func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("consumer callback panicked, skip this delivery and keep watching",
				"target", target, "panic", r, "stack", string(debug.Stack()))
		}
	}()
	notify()
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
