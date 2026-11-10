package distlock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = logx.Delegating("etcdx.distlock")

// sentinel 是本包三个错误哨兵的统一类型（#86）：可比较的值类型，同包内
// errors.Is 按值身份匹配；同时实现 Is 方法做**同文案等价**判定——redisx/
// distlock 历史上与本包各自 errors.New 出同名同文案的哨兵，是两个不同的值，
// 跨包 errors.Is 恒 false，面向 Locker 接口的适配层换后端后错误分类随之
// 漂移（429 变 500）。本类型让 etcdx 侧哨兵对文案相同的同义哨兵（redisx
// 侧即普通 errors.New 同文案值）判定为 true：
// errors.Is(etcdx.ErrLockTimeout, redisx.ErrLockTimeout) == true，
// 适配层绑哪一侧的哨兵都能正确分类；与 context.DeadlineExceeded 等无关
// 错误仍为 false。代价：任何文案完全相同的外部错误也会被判 true——本包
// 哨兵文案均带锁定语义、无前缀冲突，误判面可忽略。
type sentinel struct{ msg string }

func (e sentinel) Error() string { return e.msg }

// Is 支持 errors.Is 的 target 判定：同类型按值比较（== 已覆盖），外部
// 同文案错误（含 redisx/distlock 的同名哨兵）视为同一语义错误（#86）。
func (e sentinel) Is(target error) bool {
	if target == nil {
		return false
	}
	return target.Error() == e.msg
}

// ErrLockNotAcquired 表示锁被其他持有者占用，获取失败。
var ErrLockNotAcquired = sentinel{msg: "lock not acquired"}

// ErrLockTimeout 表示在指定超时时间内未能获取锁。
var ErrLockTimeout = sentinel{msg: "lock acquire timeout"}

// ErrNotOwner 表示 Unlock 的 ownerID 与本进程持有记录不符（或该 key 未由本进程持有）。
var ErrNotOwner = sentinel{msg: "lock not owned by this owner"}

// Locker 是分布式锁接口，与 redisx/distlock.Locker 方法集同形，面向接口编程。
//
// 两后端的统一口径（跨后端移植按此理解；「方法集同形」之外的行为差异逐条
// 列明，不再宣称无条件的「换实现不改变行为」，#86）：
//
//   - 错误哨兵跨后端 errors.Is 兼容（#86）：本包哨兵对同文案错误等价判定，
//     errors.Is(etcdx.ErrLockTimeout, redisx.ErrLockTimeout) 为 true——适配层
//     绑 redisx 的哨兵跑在 etcdx 上（或反向）错误分类不漂移。ErrNotOwner
//     与 redisx 侧的「非本人锁」哨兵文案不同，不参与跨包等价。
//   - Unlock 非持有者**显式报错**：本包返回 ErrNotOwner；redisx 自其对齐
//     修复（其 #115）起同样对非持有者显式报错而非静默 no-op——静默会让
//     「锁没放掉」不可观测，调用方误以为已释放。
//   - ttl 必须为正：ttl≤0 直接报错（与 redisx 的 "ttl must be positive"
//     对齐，#62）——此前 etcdx 把零值/负值静默抬成 1s，同一份代码换后端
//     一个立刻报错、一个悄悄持 1s 锁。etcd 租约秒级精度，(0, 1s) 的亚秒
//     TTL 抬升至 1s 并记 Warn（redisx 毫秒精确，此处是机制差异）。
//   - Lock 的 timeout≤0 表示**单次非阻塞尝试**（与 redisx 对齐，#66）：
//     成功或 ErrLockTimeout——redisx 的 Lock 先试一次再看 deadline，故
//     timeout=0 仍有一次尝试；此前 etcdx 用 WithTimeout(ctx, 0) 造出立即
//     过期的 ctx，一次都不试就 ErrLockTimeout。
//   - WithGracefulDegradation 的放行面（#63）：redisx 仅对「构造缺失
//     （client 为 nil）」放行；etcdx 构造期即拒绝 nil client，不存在该
//     场景，对应放行面为「运行期 etcd 传输层不可达」。除该面外的一切
//     错误（权限/认证/参数/压缩/未知业务错）两后端都显式报错，绝不
//     fail-open。
type Locker interface {
	// TryLock 非阻塞尝试获取锁。
	// 返回 (ownerID, nil) 表示成功，ownerID 用于后续 Unlock。
	// 返回 ("", ErrLockNotAcquired) 表示锁被占用。
	// ttl≤0 直接报错（见接口注释统一口径）。
	TryLock(ctx context.Context, key string, ttl time.Duration) (string, error)

	// Lock 阻塞获取锁，带超时。
	// 返回 (ownerID, nil) 表示成功。
	// 返回 ("", ErrLockTimeout) 表示超时未获取到。
	// timeout≤0 为单次非阻塞尝试（见接口注释统一口径）：成功或 ErrLockTimeout。
	Lock(ctx context.Context, key string, ttl time.Duration, timeout time.Duration) (string, error)

	// Unlock 释放锁，ownerID 必须匹配。
	Unlock(ctx context.Context, key string, ownerID string) error
}

const defaultPrefix = "/oekit/distlock"

// connProbeWindow 是加锁链路每一步网络交互的有界窗口（连接预检、租约 Grant
// 各一个窗口，与调用方 ctx 取较小者）：预检与 Grant 之间 etcd 进入黑洞时，
// caller 的 deadline 是唯一出口（#111）。
const connProbeWindow = 3 * time.Second

// EtcdLocker 是基于 etcd Session + concurrency.Mutex 的分布式锁实现。
type EtcdLocker struct {
	cli                 *clientv3.Client
	prefix              string
	gracefulDegradation bool
	onLock              func(key string, acquired bool, err error)
	// maxHold 是单次持有的最大持锁时长安全网上限（负值 = 不限制）。
	maxHold time.Duration

	mu sync.Mutex
	// held 记录本进程持有的锁：业务 key → ownerID → 持有状态。
	//
	// **按 ownerID 分层**（而不是 key → 条目）：同一个 key 可能在本进程里出现
	// 多个持有者（降级放行与真实持锁并存、或上层对同一 key 重复加锁）。单层
	// 映射会让后写者**静默覆盖**前一个 ⇒ 前一个的 Session 永远不被关闭
	// （租约继续续期 ⇒ 锁不释放），且前一个 owner 的 Unlock 因 ownerID 不匹配
	// 报 ErrNotOwner ⇒ 它的锁也放不掉。
	held map[string]map[string]*heldLock
}

type heldLock struct {
	ownerID string
	session *concurrency.Session // 降级放行时为 nil
	mutex   *concurrency.Mutex
	// stopSafety 停掉本次持有的「最大持锁时长」安全网 goroutine
	// （Unlock / Close 时调用；安全网自行触发后由它自己结束）。
	stopSafety func()
}

var _ Locker = (*EtcdLocker)(nil)

// EtcdLockerOption 配置 EtcdLocker 的可选参数。
type EtcdLockerOption struct {
	Prefix              string
	GracefulDegradation bool
	OnLock              func(key string, acquired bool, err error)
	// MaxHold 是单次持有的**最大持锁时长**（安全网，0 = 用默认 10×ttl，
	// 负值 = 不限制）。
	MaxHold time.Duration
}

// EtcdLockerOptionFunc 配置函数类型。
type EtcdLockerOptionFunc func(*EtcdLockerOption)

// WithPrefix 设置锁键命名空间：键形如 "/<prefix>/<key>"（默认 "/oekit/distlock"）。
func WithPrefix(prefix string) EtcdLockerOptionFunc {
	return func(o *EtcdLockerOption) { o.Prefix = prefix }
}

// WithGracefulDegradation 开启「etcd 传输层不可达」时的降级放行：TryLock/Lock
// 直接返回成功（ownerID 带 "degraded:" 前缀）。
//
// 与 redisx/distlock 的同名选项统一口径（#63）：那边只对「构造缺失
// （client 为 nil）」放行；本包构造期即拒绝 nil client（ErrNilClient），
// 不存在该场景，对应放行面就是「运行期 etcd 传输层不可达」（连接拒绝/
// 断开、gRPC Unavailable、拨号/请求超时）。**其余错误一律显式报错**：
// 权限/认证/参数/压缩等业务层故障 fail-open 会让多实例在故障窗口内同时
// 「拿到锁」并发进临界区，互斥静默失效。
//
// 即便对「不可达」放行也仅适用于单实例部署等互斥不关键的场景——多实例
// 下 etcd 重启窗口内各实例会同时降级放行，出现并发执行。
func WithGracefulDegradation() EtcdLockerOptionFunc {
	return func(o *EtcdLockerOption) { o.GracefulDegradation = true }
}

// WithOnLock 设置加锁结果回调（指标/日志钩子），在 TryLock/Lock 返回前同步调用。
func WithOnLock(fn func(key string, acquired bool, err error)) EtcdLockerOptionFunc {
	return func(o *EtcdLockerOption) { o.OnLock = fn }
}

// WithMaxHold 设置单次持有的**最大持锁时长**（安全网，#36）。
//
// 与 redisx/distlock 的 `WithMaxAutoRenew` **对称**：忘记 Unlock 时，
// 到期后安全网会**停止续租**，锁在 TTL 后自然过期 ⇒ 不会形成
// 「永久持锁 + keepalive goroutine 泄漏」。
//
//   - 0（默认）：10×ttl；
//   - 负值：不限制（兼容旧行为，需调用方保证 Unlock 一定被调用）；
//   - 正值：显式上限。
//
// 实现用的是 etcd `Session.Orphan()`：它**停止 keepalive 但不撤销租约**
// ⇒ 租约在 TTL 后自然过期、键随之删除 ⇒ 语义与 redisx 侧
// 「续期到期后锁在 TTL 后自然过期」一致（不是立刻释放）。
func WithMaxHold(d time.Duration) EtcdLockerOptionFunc {
	return func(o *EtcdLockerOption) { o.MaxHold = d }
}

// NewEtcdLocker 创建 etcd 分布式锁。
//
// client 为 nil 时返回 ErrNilClient（构造期 fail-fast，避免首次加锁时在
// cli.Get 上空指针崩溃）。
func NewEtcdLocker(cli *clientv3.Client, opts ...EtcdLockerOptionFunc) (*EtcdLocker, error) {
	if cli == nil {
		return nil, ErrNilClient
	}
	o := &EtcdLockerOption{Prefix: defaultPrefix}
	for _, fn := range opts {
		if fn != nil {
			fn(o)
		}
	}
	prefix := "/" + strings.Trim(o.Prefix, "/")
	if prefix == "/" {
		prefix = defaultPrefix
	}
	return &EtcdLocker{
		cli:                 cli,
		prefix:              prefix,
		gracefulDegradation: o.GracefulDegradation,
		onLock:              o.OnLock,
		maxHold:             o.MaxHold,
		held:                make(map[string]map[string]*heldLock),
	}, nil
}

// TryLock 非阻塞尝试获取锁：新建 Session（租约 ttl + 自动续约）后 Mutex.TryLock。
func (l *EtcdLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (string, error) {
	return l.acquire(ctx, key, ttl, func(ctx context.Context, m *concurrency.Mutex) error {
		if err := m.TryLock(ctx); err != nil {
			if errors.Is(err, concurrency.ErrLocked) {
				return ErrLockNotAcquired
			}
			return err
		}
		return nil
	})
}

// Lock 阻塞获取锁（revision 公平排队），timeout 内未获得返回 ErrLockTimeout。
// timeout≤0 为单次非阻塞尝试（与 redisx 对齐，#66）：忙时返回 ErrLockTimeout。
func (l *EtcdLocker) Lock(ctx context.Context, key string, ttl, timeout time.Duration) (string, error) {
	return l.acquire(ctx, key, ttl, func(ctx context.Context, m *concurrency.Mutex) error {
		// timeout≤0：单次非阻塞尝试。redisx 的 Lock 先 TryLock 一次再看
		// deadline ⇒ timeout=0 仍有一次尝试；此前这里 WithTimeout(ctx, 0)
		// 造出立即过期的 ctx ⇒ m.Lock 一次都不试就 DeadlineExceeded ⇒
		// 同接口换后端，redisx 拿得到锁、etcdx 永远拿不到。
		if timeout <= 0 {
			if err := m.TryLock(ctx); err != nil {
				if errors.Is(err, concurrency.ErrLocked) {
					return ErrLockTimeout // 单次尝试未得 ⇒ 与 redisx 超时口径一致
				}
				return err
			}
			return nil
		}
		tctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if err := m.Lock(tctx); err != nil {
			if ctx.Err() == nil && errors.Is(tctx.Err(), context.DeadlineExceeded) {
				return ErrLockTimeout
			}
			return err
		}
		return nil
	})
}

// acquire 统一加锁流程：连接预检 → 建 Session → 建 Mutex → 执行具体加锁
// 策略 → 记录持有。加锁失败一律关闭 Session 回收租约。
func (l *EtcdLocker) acquire(ctx context.Context, key string, ttl time.Duration,
	do func(ctx context.Context, m *concurrency.Mutex) error) (string, error) {
	if key == "" {
		return "", fmt.Errorf("etcdx distlock: key is empty")
	}
	// ttl≤0 与 redisx 对齐直接报错（#62）：同接口下 redisx 的 TryLock 对
	// ttl≤0 返回 "distlock: ttl must be positive"；此前这里静默抬成 1s——
	// 同一份调用代码换后端，一个是立刻报错、一个是悄悄持 1s 锁。零值/负值
	// 是参数错误，在任何网络交互前 fail-fast。
	if ttl <= 0 {
		return "", fmt.Errorf("etcdx distlock: ttl must be positive, got %v", ttl)
	}
	// 连接预检：轻量读在有界窗口内探活，失败直接降级/报错，不做后续网络
	// 交互（#54：caller ctx 自身取消/超时 ≠ etcd 不可达，显式报错不降级）
	pctx, pcancel := context.WithTimeout(ctx, connProbeWindow)
	_, perr := l.cli.Get(pctx, l.prefix)
	pcancel()
	if perr != nil {
		// #54：调用方 ctx 自身取消/超时 ≠ etcd 不可达——降级面只对
		// 「etcd 传输层不可用」放行，caller ctx 问题显式报错。此前把 caller
		// deadline 误标为 etcd unreachable 后降级放行：多实例同时「拿到锁」
		// 并发进临界区，互斥直接失效。
		if ctx.Err() != nil {
			l.notify(key, false, ctx.Err())
			return "", fmt.Errorf("etcdx distlock: caller context done while probing etcd: %w", ctx.Err())
		}
		return l.degradeOrFail(key, fmt.Errorf("etcdx distlock: etcd unreachable: %w", perr))
	}
	// 租约精度：etcd 的租约以**秒**为单位 ⇒ (0, 1s) 的亚秒 TTL 抬升至 1s。
	// （redisx 侧走 SetNX 是精确毫秒 ⇒ 同一个 Locker 接口，200ms 的锁在
	// redisx 是 200ms、在 etcd 是 1s。这是租约机制的精度限制，不是 bug，
	// 但调用方按毫秒理解会误判 ⇒ 抬升时至少打 Warn 明示（#62），不再静默。）
	if ttl < time.Second {
		log.Warn("sub-second ttl raised to 1s (etcd lease granularity is seconds)",
			"key", key, "ttl", ttl)
	}
	ttlSec := max(1, int(ttl/time.Second))
	// 租约 Grant 显式化（#111）：此前 NewSession 内部的 Grant 走 Session
	// 自身的后台 ctx（无任何 deadline），防护只覆盖「探测时已不可达」——
	// 预检通过后、Grant 前 etcd 进入黑洞时 Lock 永久挂死，caller 的
	// timeout/取消与 WithGracefulDegradation 全部失效（根本没有错误返回）。
	// 现在 caller ctx 贯穿 Grant（WithRequireLeader 元数据让「无 leader」
	// 快速失败而非干等），拿到租约后以 WithLease 组装 Session：NewSession
	// 跳过内部 Grant，keepalive 仍由 Session 自身 ctx 管理，生命周期语义
	// 不变，创建全程有界，失败走与预检相同的 degradeOrFail 分类。WithTTL
	// 保留供 Session.Close 的 Revoke 窗口使用（租约实际 TTL 以服务端返回
	// 为准，与 keepalive 无关）。
	gctx, gcancel := context.WithTimeout(clientv3.WithRequireLeader(ctx), connProbeWindow)
	grant, gerr := l.cli.Grant(gctx, int64(ttlSec))
	gcancel()
	if gerr != nil {
		// 与预检同口径（#54）：caller ctx 自身取消/超时 ≠ etcd 不可达
		if ctx.Err() != nil {
			l.notify(key, false, ctx.Err())
			return "", fmt.Errorf("etcdx distlock: caller context done while granting lease: %w", ctx.Err())
		}
		return l.degradeOrFail(key, fmt.Errorf("etcdx distlock: grant lease: %w", gerr))
	}
	session, err := concurrency.NewSession(l.cli, concurrency.WithLease(grant.ID), concurrency.WithTTL(ttlSec))
	if err != nil {
		if ctx.Err() != nil { // #54：caller ctx 问题显式报错
			return "", fmt.Errorf("etcdx distlock: new session: %w", err)
		}
		return l.degradeOrFail(key, fmt.Errorf("etcdx distlock: new session: %w", err))
	}
	m := concurrency.NewMutex(session, l.lockKey(key))
	if err := do(ctx, m); err != nil {
		_ = session.Close()
		if errors.Is(err, ErrLockNotAcquired) || errors.Is(err, ErrLockTimeout) || ctx.Err() != nil {
			l.notify(key, false, err)
			return "", err
		}
		// #54 同口径：caller ctx 已取消/超时的错误不进降级面
		if ctx.Err() != nil {
			return "", fmt.Errorf("etcdx distlock: lock %s: %w", key, err)
		}
		return l.degradeOrFail(key, fmt.Errorf("etcdx distlock: lock %s: %w", key, err))
	}
	ownerID := newOwnerID()
	h := &heldLock{ownerID: ownerID, session: session, mutex: m}
	h.stopSafety = l.startSafetyNet(key, ownerID, session, ttl)
	l.mu.Lock()
	l.setHeld(key, ownerID, h)
	l.mu.Unlock()
	l.notify(key, true, nil)
	return ownerID, nil
}

// startSafetyNet 启动「最大持锁时长」安全网：到期后**停止续租**
// （session.Orphan）⇒ 租约在 TTL 后自然过期、键随之删除 ⇒
// 锁不会因为调用方漏调 Unlock 而被永久持有。
//
// 与 redisx/distlock 的 WithMaxAutoRenew 对称（#36）：那边是「续期 goroutine
// 到期后退出，锁在 TTL 后自然过期」，这边是「keepalive 到期后停止，租约在
// TTL 后自然过期」——语义一致（都不是立刻释放）。
//
// 安全网自身触发后不请求关机：持锁进程可能仍在跑临界区，锁被自然过期后
// 另一个实例可以拿到锁——这正是「安全网兜底」想要的语义（对比 redisx 侧：
// 锁自然过期后同样会被别的实例拿到）。
//
// stop 返回的函数用于在 Unlock / Close 时提前终止安全网（此时锁是正常释放的，
// 不该等到期）。
func (l *EtcdLocker) startSafetyNet(key, ownerID string, session *concurrency.Session, ttl time.Duration) func() {
	maxHold := l.maxHold
	switch {
	case maxHold < 0:
		return nil // 不限制：完全依赖调用方 Unlock
	case maxHold == 0:
		maxHold = 10 * ttl
		if maxHold <= 0 {
			maxHold = 10 * time.Second
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	async.GoSafe(func() {
		timer := time.NewTimer(maxHold)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return // 正常 Unlock / Close 提前终止
		case <-timer.C:
		}
		// 到期：停止续租（不撤销租约）⇒ 租约在 TTL 后自然过期。
		log.Warn("etcdx distlock max hold exceeded, stopping lease renewal",
			"key", key, "owner", ownerID, "max_hold", maxHold)
		session.Orphan()
	})
	return cancel
}

// setHeld 记录一次持有（同 key 多 ownerID 各占一条，互不覆盖）。
// 调用方需持有 l.mu。
func (l *EtcdLocker) setHeld(key, ownerID string, h *heldLock) {
	if l.held == nil {
		l.held = make(map[string]map[string]*heldLock)
	}
	if l.held[key] == nil {
		l.held[key] = make(map[string]*heldLock)
	}
	l.held[key][ownerID] = h
}

// Unlock 释放锁：ownerID 须与本进程持有记录一致；释放后 Session 关闭回收租约。
func (l *EtcdLocker) Unlock(ctx context.Context, key, ownerID string) error {
	l.mu.Lock()
	h := l.held[key][ownerID]
	if h == nil {
		l.mu.Unlock()
		return ErrNotOwner
	}
	delete(l.held[key], ownerID)
	if len(l.held[key]) == 0 {
		delete(l.held, key)
	}
	l.mu.Unlock()
	return l.release(ctx, key, h)
}

// Close 释放本进程持有的全部锁（幂等）。
func (l *EtcdLocker) Close(ctx context.Context) error {
	l.mu.Lock()
	held := l.held
	l.held = make(map[string]map[string]*heldLock)
	l.mu.Unlock()
	var errs []error
	for key, byOwner := range held {
		for _, h := range byOwner {
			if err := l.release(ctx, key, h); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (l *EtcdLocker) release(ctx context.Context, key string, h *heldLock) error {
	if h.stopSafety != nil {
		h.stopSafety() // 正常释放路径：停掉安全网，不让它随后再动租约
	}
	if h.session == nil { // 降级放行的锁无需释放
		return nil
	}
	var err error
	if h.mutex != nil {
		if uerr := h.mutex.Unlock(ctx); uerr != nil {
			err = fmt.Errorf("etcdx distlock: unlock %s: %w", key, uerr)
		}
	}
	// 关闭 Session 撤销租约：即便 Unlock 失败，键也随租约撤销删除
	if cerr := h.session.Close(); cerr != nil && err == nil {
		err = fmt.Errorf("etcdx distlock: close session %s: %w", key, cerr)
	}
	return err
}

// degradeOrFail 处理加锁链路上的失败：开启 WithGracefulDegradation 且错误
// 属「etcd 传输层不可达」类（isUnreachable）时降级放行，否则显式报错。
//
// #63 收窄：此前这里对**任意**失败（预检失败/NewSession 失败/加锁期任意
// 非 busy 错误）都放行——权限被拒、认证失效、压缩冲突等业务层故障也会
// fail-open，多实例在故障窗口内同时「拿到锁」，互斥静默失效。与 redisx
// （仅构造缺失放行）的语义差被同一选项名掩盖，跨后端移植必踩。现在只有
// 明确的连接不可达才降级，口径写进 Locker 接口注释。
func (l *EtcdLocker) degradeOrFail(key string, err error) (string, error) {
	if l.gracefulDegradation && isUnreachable(err) {
		log.Warn("etcd unreachable, lock degraded to pass-through", "key", key, "err", err)
		ownerID := "degraded:" + newOwnerID()
		l.mu.Lock()
		l.setHeld(key, ownerID, &heldLock{ownerID: ownerID})
		l.mu.Unlock()
		l.notify(key, true, nil)
		return ownerID, nil
	}
	l.notify(key, false, err)
	return "", err
}

// isUnreachable 判断 err 是否为「etcd 传输层不可达」类故障：连接拒绝/断开、
// gRPC Unavailable、拨号/请求超时。只有这类故障进入优雅降级面（见
// degradeOrFail 的 #63 说明）；权限/认证/参数/压缩等业务层错误不属于——
// 对它们 fail-open 会静默破坏互斥。
func isUnreachable(err error) bool {
	if err == nil {
		return false
	}
	// 探活窗口（pctx 3s）超时 / grpc 把拨号超时映射为 DeadlineExceeded
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// TCP 层连接拒绝/断开（grpc 错误链里包裹的 net 错误）
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// gRPC 状态码：Unavailable = 连接不可用（断连/无法建连/keepalive 断开）。
	// 其余码（PermissionDenied/InvalidArgument/OutOfRange(压缩)/…）是业务层
	// 错误，不降级。status.FromError 内部走 errors.As，支持 %w 包裹链。
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Unavailable
	}
	return false
}

func (l *EtcdLocker) notify(key string, acquired bool, err error) {
	if l.onLock != nil {
		l.onLock(key, acquired, err)
	}
}

func (l *EtcdLocker) lockKey(key string) string {
	return l.prefix + "/" + key
}

func newOwnerID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败极罕见；退化为时间戳保证非空且基本唯一
		return fmt.Sprintf("t%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
