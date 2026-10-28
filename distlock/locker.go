package distlock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

var log = logx.Delegating("etcdx.distlock")

// ErrLockNotAcquired 表示锁被其他持有者占用，获取失败。
var ErrLockNotAcquired = errors.New("lock not acquired")

// ErrLockTimeout 表示在指定超时时间内未能获取锁。
var ErrLockTimeout = errors.New("lock acquire timeout")

// ErrNotOwner 表示 Unlock 的 ownerID 与本进程持有记录不符（或该 key 未由本进程持有）。
var ErrNotOwner = errors.New("lock not owned by this owner")

// Locker 是分布式锁接口，与 redisx/distlock.Locker 同形，面向接口编程。
type Locker interface {
	// TryLock 非阻塞尝试获取锁。
	// 返回 (ownerID, nil) 表示成功，ownerID 用于后续 Unlock。
	// 返回 ("", ErrLockNotAcquired) 表示锁被占用。
	TryLock(ctx context.Context, key string, ttl time.Duration) (string, error)

	// Lock 阻塞获取锁，带超时。
	// 返回 (ownerID, nil) 表示成功。
	// 返回 ("", ErrLockTimeout) 表示超时未获取到。
	Lock(ctx context.Context, key string, ttl time.Duration, timeout time.Duration) (string, error)

	// Unlock 释放锁，ownerID 必须匹配。
	Unlock(ctx context.Context, key string, ownerID string) error
}

const defaultPrefix = "/oekit/distlock"

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

// WithGracefulDegradation 开启无 etcd 时降级放行：etcd 不可达时 TryLock/Lock
// 直接返回成功（ownerID 带 "degraded:" 前缀）。仅适用于单实例部署等
// 互斥不关键的场景——多实例下会出现并发执行。
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
func (l *EtcdLocker) Lock(ctx context.Context, key string, ttl, timeout time.Duration) (string, error) {
	return l.acquire(ctx, key, ttl, func(ctx context.Context, m *concurrency.Mutex) error {
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
	// 连接预检：NewSession 的 Grant 走 Session 自身的后台 ctx，对不可达
	// etcd 会无限等待；先用轻量读在有界窗口内探活，失败直接降级/报错，
	// 不创建注定挂死的 Session
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	_, perr := l.cli.Get(pctx, l.prefix)
	pcancel()
	if perr != nil {
		return l.degradeOrFail(key, fmt.Errorf("etcdx distlock: etcd unreachable: %w", perr))
	}
	// 租约精度：etcd 的租约以**秒**为单位 ⇒ 亚秒 TTL 静默抬成 1s。
	// （redisx 侧走 SetNX 是精确毫秒 ⇒ 同一个 Locker 接口，200ms 的锁在
	// redisx 是 200ms、在 etcd 是 1s。这是租约机制的精度限制，不是 bug，
	// 但调用方按毫秒理解会误判。）
	ttlSec := int(ttl / time.Second)
	if ttlSec < 1 {
		ttlSec = 1
	}
	session, err := concurrency.NewSession(l.cli, concurrency.WithTTL(ttlSec))
	if err != nil {
		return l.degradeOrFail(key, fmt.Errorf("etcdx distlock: new session: %w", err))
	}
	m := concurrency.NewMutex(session, l.lockKey(key))
	if err := do(ctx, m); err != nil {
		_ = session.Close()
		if errors.Is(err, ErrLockNotAcquired) || errors.Is(err, ErrLockTimeout) || ctx.Err() != nil {
			l.notify(key, false, err)
			return "", err
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

func (l *EtcdLocker) degradeOrFail(key string, err error) (string, error) {
	if l.gracefulDegradation {
		log.Warn("etcd unavailable, lock degraded to pass-through", "key", key, "err", err)
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
