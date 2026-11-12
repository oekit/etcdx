package distlock

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pkgdistlock "github.com/oekit/pkg/distlock"
)

// WithLock 回归（#123 契约单源）：本包 WithLock 薄委托 pkg/distlock.WithLock，
// 释放纪律从历史 WithTimeout(Background, 3s) 统一为 context.WithoutCancel
// 口径；获取失败错误统一带 "lock <key>:" 包装、哨兵穿透。用行为可编程的
// fake Locker（不引真后端）钉住释放语义。

// fakeLocker 是行为可编程的内存 Locker：只验证 WithLock 的获取/释放路径
// 与 ctx 纪律，不依赖 etcd。
type fakeLocker struct {
	mu        sync.Mutex
	lockErr   error           // Lock 的返回错误
	unlockErr error           // Unlock 的返回错误
	unlockKey string          // Unlock 收到的 key
	unlockOwn string          // Unlock 收到的 ownerID
	unlockCtx context.Context // Unlock 收到的 ctx（验证 WithoutCancel 纪律）
	// #428 口径（pkg v0.47.0）：解耦判定点在 Unlock 调用时——ReleaseUncancelable
	// 的 defer cancel 会让事后 Err() 恒 Canceled；释放 ctx 必须带 deadline。
	unlockCtxErrAtCall  error
	unlockCtxDeadlineOK bool
	unlocked            chan struct{}
	lockCalls           int
	fnRan               bool
}

func newFakeLocker() *fakeLocker {
	return &fakeLocker{unlocked: make(chan struct{}, 1)}
}

func (f *fakeLocker) TryLock(_ context.Context, _ string, _ time.Duration) (string, error) {
	if f.lockErr != nil {
		return "", f.lockErr
	}
	return "owner-1", nil
}

func (f *fakeLocker) Lock(_ context.Context, _ string, _, _ time.Duration) (string, error) {
	f.mu.Lock()
	f.lockCalls++
	f.mu.Unlock()
	if f.lockErr != nil {
		return "", f.lockErr
	}
	return "owner-1", nil
}

func (f *fakeLocker) Unlock(ctx context.Context, key, ownerID string) error {
	f.mu.Lock()
	f.unlockCtx = ctx
	f.unlockCtxErrAtCall = ctx.Err()
	_, f.unlockCtxDeadlineOK = ctx.Deadline()
	f.unlockKey = key
	f.unlockOwn = ownerID
	f.mu.Unlock()
	if f.unlocked != nil {
		select {
		case f.unlocked <- struct{}{}:
		default:
		}
	}
	return f.unlockErr
}

// 编译期契约：fake 实现满足 Locker（= pkg/distlock.Locker，接口形状钉住）。
var _ Locker = (*fakeLocker)(nil)

// TestWithLock_AcquireRunRelease 成功路径：Lock → fn → Unlock(key, owner)。
func TestWithLock_AcquireRunRelease(t *testing.T) {
	fl := newFakeLocker()
	err := WithLock(context.Background(), fl, "job:nightly", 30*time.Second, 5*time.Second, func() error {
		fl.fnRan = true
		return nil
	})
	if err != nil {
		t.Fatalf("WithLock 应成功, got %v", err)
	}
	if fl.lockCalls != 1 {
		t.Fatalf("Lock 应恰被调一次, got %d", fl.lockCalls)
	}
	if !fl.fnRan {
		t.Fatal("fn 应被执行")
	}
	<-fl.unlocked
	if fl.unlockKey != "job:nightly" || fl.unlockOwn != "owner-1" {
		t.Fatalf("Unlock 应收到同 key 同 owner, got key=%q owner=%q", fl.unlockKey, fl.unlockOwn)
	}
}

// TestWithLock_AcquireFailure_WrapsKey_SentinelPreserved 获取失败：fn 不执行、
// 不释放；错误带 "lock <key>:" 包装（#123 单源后与 redisx 口径一致——历史
// 实现无包装），哨兵经 %w 穿透、errors.Is 可判定。
func TestWithLock_AcquireFailure_WrapsKey_SentinelPreserved(t *testing.T) {
	fl := newFakeLocker()
	fl.lockErr = ErrLockTimeout
	err := WithLock(context.Background(), fl, "k", time.Second, 10*time.Millisecond, func() error {
		t.Fatal("获取失败 fn 不得执行")
		return nil
	})
	if err == nil {
		t.Fatal("获取失败应报错")
	}
	if !strings.Contains(err.Error(), "lock k:") {
		t.Fatalf("获取失败错误应带 \"lock k:\" 包装（#123 单源口径）, got %v", err)
	}
	if !errors.Is(err, ErrLockTimeout) || !errors.Is(err, pkgdistlock.ErrLockTimeout) {
		t.Fatalf("错误链应保留 ErrLockTimeout（哨兵穿透，本包与 pkg 契约名任一写法命中）, got %v", err)
	}
	if fl.fnRan {
		t.Fatal("fn 不应被执行")
	}
	fl.mu.Lock()
	uk := fl.unlockKey
	fl.mu.Unlock()
	if uk != "" {
		t.Fatal("未获取到锁不得释放")
	}
}

// TestWithLock_FnErrorPropagates fn 的错误原样上抛（不被包装、不被释放失败
// 顶掉），且 fn 出错后锁仍必须释放。
func TestWithLock_FnErrorPropagates(t *testing.T) {
	boom := errors.New("job failed")
	fl := newFakeLocker()
	err := WithLock(context.Background(), fl, "k", time.Second, 10*time.Millisecond, func() error {
		return boom
	})
	if !errors.Is(err, boom) || err.Error() != "job failed" {
		t.Fatalf("fn 的错误应原样上抛, got %v", err)
	}
	<-fl.unlocked
	if fl.unlockKey != "k" {
		t.Fatal("fn 出错后锁仍必须释放")
	}
}

// TestWithLock_UnlockFailure_DoesNotOverrideFnError 释放失败只记日志不覆盖
// fn 的返回值（租约/TTL 到期是后端安全网）。
func TestWithLock_UnlockFailure_DoesNotOverrideFnError(t *testing.T) {
	boom := errors.New("job failed")
	fl := newFakeLocker()
	fl.unlockErr = ErrLockNotHeld
	err := WithLock(context.Background(), fl, "k", time.Second, 10*time.Millisecond, func() error {
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("fn 错误应为主结果, got %v", err)
	}
}

// TestWithLock_ReleasedWithUncancelableContext 核心释放语义回归（#123 统一
// 口径）：fn 执行期间 caller ctx 取消，释放动作仍必须拿到**未取消**的 ctx
// （WithoutCancel）——取消的 ctx 会让后端发解锁命令前就返回 context.Canceled，
// 锁变孤儿阻塞到 TTL 过期。与历史实现的差异：WithTimeout(Background, 3s) 不
// 携带 caller ctx 取值，WithoutCancel 保留取值（只剥离取消信号）。
func TestWithLock_ReleasedWithUncancelableContext(t *testing.T) {
	type ctxKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "auth"))
	fl := newFakeLocker()
	err := WithLock(ctx, fl, "k", time.Second, 10*time.Millisecond, func() error {
		cancel() // 模拟 caller ctx 在持锁期到期
		return nil
	})
	if err != nil {
		t.Fatalf("WithLock 应成功, got %v", err)
	}
	<-fl.unlocked
	if fl.unlockCtx == nil {
		t.Fatal("Unlock 未被调用")
	}
	if fl.unlockCtxErrAtCall != nil {
		t.Fatalf("释放用的 ctx 在调用点必须与 caller 取消状态解耦, got %v", fl.unlockCtxErrAtCall)
	}
	if !fl.unlockCtxDeadlineOK {
		t.Fatal("释放 ctx 必须带 deadline（pkg#428：释放动作有上界）")
	}
	// ctx 携带的取值保留（WithoutCancel 只剥离取消信号，不换 context 链；
	// 历史 WithTimeout(Background, 3s) 口径会丢掉取值——行为差异点）。
	if gotVal, _ := fl.unlockCtx.Value(ctxKey{}).(string); gotVal != "auth" {
		t.Fatalf("释放 ctx 应保留 ctx 取值, got %q", gotVal)
	}
}

// TestWithLock_ReleasedWithUncancelableContext_NoCallerCancel caller ctx 未
// 取消时释放照常工作（WithoutCancel 的常规路径）。
func TestWithLock_ReleasedWithUncancelableContext_NoCallerCancel(t *testing.T) {
	fl := newFakeLocker()
	if err := WithLock(context.Background(), fl, "k", time.Second, 10*time.Millisecond, func() error {
		return nil
	}); err != nil {
		t.Fatalf("WithLock 应成功, got %v", err)
	}
	<-fl.unlocked
	if fl.unlockCtxErrAtCall != nil {
		t.Fatalf("未取消场景释放 ctx 调用点不应带错误, got %v", fl.unlockCtxErrAtCall)
	}
	if !fl.unlockCtxDeadlineOK {
		t.Fatal("释放 ctx 必须带 deadline（pkg#428：释放动作有上界）")
	}
}
