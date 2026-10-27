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

	mu   sync.Mutex
	held map[string]*heldLock // 业务 key → 本进程持有状态
}

type heldLock struct {
	ownerID string
	session *concurrency.Session // 降级放行时为 nil
	mutex   *concurrency.Mutex
}

var _ Locker = (*EtcdLocker)(nil)

// EtcdLockerOption 配置 EtcdLocker 的可选参数。
type EtcdLockerOption struct {
	Prefix              string
	GracefulDegradation bool
	OnLock              func(key string, acquired bool, err error)
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
		held:                make(map[string]*heldLock),
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
	l.mu.Lock()
	l.held[key] = &heldLock{ownerID: ownerID, session: session, mutex: m}
	l.mu.Unlock()
	l.notify(key, true, nil)
	return ownerID, nil
}

// Unlock 释放锁：ownerID 须与本进程持有记录一致；释放后 Session 关闭回收租约。
func (l *EtcdLocker) Unlock(ctx context.Context, key, ownerID string) error {
	l.mu.Lock()
	h := l.held[key]
	if h == nil || h.ownerID != ownerID {
		l.mu.Unlock()
		return ErrNotOwner
	}
	delete(l.held, key)
	l.mu.Unlock()
	return l.release(ctx, key, h)
}

// Close 释放本进程持有的全部锁（幂等）。
func (l *EtcdLocker) Close(ctx context.Context) error {
	l.mu.Lock()
	held := l.held
	l.held = make(map[string]*heldLock)
	l.mu.Unlock()
	var errs []error
	for key, h := range held {
		if err := l.release(ctx, key, h); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (l *EtcdLocker) release(ctx context.Context, key string, h *heldLock) error {
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
		l.held[key] = &heldLock{ownerID: ownerID}
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
