package registry

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/oekit/concurrency/retry"
	"github.com/oekit/logx"
	errors "github.com/oekit/pkg/errors"
	regcore "github.com/oekit/registry"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oekit/etcdx/internal/probe"
)

// Subscriber 是 regcore.Subscriber 的 etcd 实现：Watch 服务前缀，任一实例
// 键增删改后重新拉取全量快照回调。订阅建立时立即回调一次当前快照。
type Subscriber struct {
	cli *clientv3.Client
	opt *RegistryOption

	// reg 登记订阅句柄（#185 单源）：登记/替换/supersede 交接/摘除/关闭与
	// retired 待退出槽位（#149）与 ConfigStore 同构，只此一份实现，见
	// watchRegistry。
	reg *watchRegistry[string]
}

var _ regcore.Subscriber = (*Subscriber)(nil)

// NewSubscriber 创建 etcd 订阅器。
//
// client 为 nil 时返回 ErrNilClient（构造期 fail-fast，避免首次订阅时在
// cli.Get 上空指针崩溃）。
func NewSubscriber(cli *clientv3.Client, opts ...RegistryOptionFunc) (*Subscriber, error) {
	if cli == nil {
		return nil, ErrNilClient
	}
	return &Subscriber{
		cli: cli, opt: applyOptions(opts),
		reg: newWatchRegistry[string](),
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
		return errors.New("etcdx registry: service name is empty")
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
		return errors.New("etcdx registry: subscribe callback is nil")
	}
	dir := serviceDir(s.opt.Prefix, s.opt.Group, serviceName)

	// 先取一次快照并记录 revision，Watch 从 revision+1 起不漏事件
	resp, err := s.cli.Get(ctx, dir, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrapf(err, "etcdx registry: subscribe get %s", serviceName)
	}
	startRev := resp.Header.Revision + 1

	h := newWatchHandle()
	// 登记与 supersede 交接单源到 watchRegistry.install（#185）：同 key 旧
	// 句柄（活跃表或 retired 槽位，#149——退订/关停后的重建）被标记取代，
	// 新订阅须等其 goroutine 退出后才投递（#84）。
	old := s.reg.install(serviceName, h)

	// 观察循环（#55）：watch 被取消（ErrCompacted——长驻订阅 + auto-compaction
	// 是生产常态；etcd-server 重启等）或通道关闭后**指数退避重建**（重取快照 +
	// 新 revision），对齐注册侧「丢约指数退避自动重注册」的对等韧性。此前只打
	// 一条 Warn 就 return：订阅留在登记表、Unsubscribe「正常」返回，但此后
	// 永不回调——服务实例列表/配置推送几天后无声停更，消费方拿过期快照继续跑。
	//
	// #84/#149 的 goroutine 交接块（等 old.done → superseded 检查 →
	// defer{close(done); dropRetired}）单源在 watchRegistry.run（#185）：
	// 首次快照投递前先等被替换的旧 goroutine 退出（回调不可能并发），之后
	// 所有投递前检查 superseded，被取代/退订后的过期快照一律丢弃。
	s.reg.run(serviceName, h, old, func() {
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
				// 事件批取数套探活窗口（#202）：h.ctx 由 WithCancel(Background)
				// 派生、无 deadline——etcd 半开连接/网络黑洞下 fetchInstances 内
				// 的 Get 无限挂起，onEvent 卡死不返回，后续事件全部积压，订阅
				// 静默停更（与 #196 resnap 修复同型）。窗口数值单源 internal/probe
				// （distlock#111 立值、#196 watch resnap 接入）；窗口到期 ⇒ Get
				// 以 DeadlineExceeded 失败 ⇒ 走下方既有 Warn 丢弃分支，watch
				// 循环与退避闭环不变（形态与 watchloop.go 的 resnap 窗口一致）。
				fctx, fcancel := context.WithTimeout(h.ctx, probe.Window)
				insts, err := fetchInstances(fctx, s.cli, s.opt.Prefix, s.opt.Group, serviceName)
				fcancel()
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
// 摘除的句柄先进入 retired 待退出槽位而非直接丢弃（#149；摘除/槽位逻辑
// 单源见 watchRegistry.remove，#185）：其观察 goroutine 可能仍在执行消费方
// 回调，后续同服务 Subscribe 靠该槽位拿到 old.done 完成 #84 交接——先删表
// 再 supersede 会让交接整段失效（退订后重建订阅与旧回调并发）。本方法不
// 等待旧回调返回（不阻塞退订方）；槽位由观察 goroutine 退出时自清，不会随
// 退订次数累积。
func (s *Subscriber) Unsubscribe(_ context.Context, serviceName string) error {
	s.reg.remove(serviceName)
	return nil
}

// Close 取消全部订阅。
//
// 被取消的句柄同样转入 retired 待退出槽位（#149，与 Unsubscribe 同口径；
// 单源见 watchRegistry.closeAll）：Close 之后立即重建订阅仍须等旧 goroutine
// 退出，回调不并发。
func (s *Subscriber) Close(_ context.Context) error {
	s.reg.closeAll()
	return nil
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

// 指数退避的共享参数（#186 单源：注册侧 reRegister 与 watch 重建——
// Subscriber/ConfigStore 共用 runWatchLoop——同一策略只此一份实现）。
// 公式与护栏单源 concurrency/retry（#204 收编）：backoffFor 薄委托
// retry.SaturatedBackoff，本仓不再手抄移位实现；backoffMaxAttempts 钳位
// 常量随之删除（SaturatedBackoff 内部含溢出饱和护栏，见 backoffFor），
// 本地只保留两个委托参数。
const (
	backoffBase = time.Second // 首轮退避
	backoffMax  = 30 * time.Second
)

// backoffFor 由连续失败次数派生单轮指数退避：1s 起、30s 封顶。
//
// #204 收编：函数体薄委托 retry.SaturatedBackoff(backoffBase, backoffMax,
// attempts+1)——公式（initial * 2^(attempt-1)，受 maxDelay 封顶）与溢出
// 饱和护栏（#283：翻倍即将溢出时取封顶值，不回绕）单源 concurrency/retry
// （#128 立约「预计算与实跑同源」、#362 收敛监督重启侧手抄版），本仓的
// 移位手抄版不再各自漂移。参数映射：attempts 是 0 基准连续失败数、
// SaturatedBackoff 的 attempt 是 1 基准（第 attempt 次失败）⇒ 传
// attempts+1；attempts 为负时 SaturatedBackoff 按 attempt<1 视为 1 处理
// （等价旧实现的负值按 0 起步）；attempts 极大时内部溢出饱和到封顶值
// （原 backoffMaxAttempts 本地钳位职责由其接管）。封顶语义不变：attempts
// ≥ 5（1s<<5 = 32s > 30s）后恒 30s。成功重建/重注册后调用方将 attempts
// 归零 ⇒ 退避随之归零（#85）。
func backoffFor(attempts int) time.Duration {
	return retry.SaturatedBackoff(backoffBase, backoffMax, attempts+1)
}
