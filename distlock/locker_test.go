package distlock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oekit/etcdx/internal/testutil"
)

func TestTryLockAndUnlock(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	l1, err := NewEtcdLocker(cli, WithPrefix("t"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	l2, err := NewEtcdLocker(cli, WithPrefix("t"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}

	owner, err := l1.TryLock(ctx, "job", 5*time.Second)
	if err != nil || owner == "" {
		t.Fatalf("TryLock = %q, %v", owner, err)
	}
	if _, err := l2.TryLock(ctx, "job", 5*time.Second); !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("second TryLock err = %v; want ErrLockNotAcquired", err)
	}
	if err := l1.Unlock(ctx, "job", "wrong-owner"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("Unlock wrong owner err = %v; want ErrNotOwner", err)
	}
	if err := l2.Unlock(ctx, "job", owner); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("Unlock from non-holder err = %v; want ErrNotOwner", err)
	}
	if err := l1.Unlock(ctx, "job", owner); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if _, err := l2.TryLock(ctx, "job", 5*time.Second); err != nil {
		t.Fatalf("TryLock after release: %v", err)
	}
	if err := l2.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close 后锁已释放
	if _, err := l1.TryLock(ctx, "job", 5*time.Second); err != nil {
		t.Fatalf("TryLock after Close: %v", err)
	}
}

func TestLockTimeoutAndHandoff(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	l1, err := NewEtcdLocker(cli)
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	l2, err := NewEtcdLocker(cli)
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}

	owner, err := l1.Lock(ctx, "res", 5*time.Second, time.Second)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	start := time.Now()
	if _, err := l2.Lock(ctx, "res", 5*time.Second, 300*time.Millisecond); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Lock while held err = %v; want ErrLockTimeout", err)
	}
	if time.Since(start) < 250*time.Millisecond {
		t.Fatal("Lock returned before timeout elapsed")
	}

	// 持有者稍后释放，等待者应在 timeout 内接棒
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = l1.Unlock(ctx, "res", owner)
	}()
	owner2, err := l2.Lock(ctx, "res", 5*time.Second, 3*time.Second)
	if err != nil {
		t.Fatalf("Lock handoff: %v", err)
	}
	_ = l2.Unlock(ctx, "res", owner2)
}

func TestLockRespectsCallerContext(t *testing.T) {
	cli := testutil.NewClient(t)
	l1, err := NewEtcdLocker(cli)
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	l2, err := NewEtcdLocker(cli)
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	owner, _ := l1.TryLock(context.Background(), "ctx", 5*time.Second)
	defer l1.Unlock(context.Background(), "ctx", owner) //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_, err = l2.Lock(ctx, "ctx", 5*time.Second, 10*time.Second)
	if err == nil || errors.Is(err, ErrLockTimeout) {
		t.Fatalf("canceled ctx err = %v; want context error, not ErrLockTimeout", err)
	}
}

func TestWithLock(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	locker, err := NewEtcdLocker(cli)
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	var calls int
	err = WithLock(ctx, locker, "wl", 5*time.Second, time.Second, func() error {
		calls++
		// 持锁期间他人拿不到
		other, err := NewEtcdLocker(cli)
		if err != nil {
			t.Errorf("NewEtcdLocker: %v", err)
			return errors.New("biz")
		}
		if _, err := other.TryLock(ctx, "wl", 5*time.Second); !errors.Is(err, ErrLockNotAcquired) {
			t.Errorf("TryLock inside WithLock err = %v; want ErrLockNotAcquired", err)
		}
		return errors.New("biz")
	})
	if err == nil || err.Error() != "biz" || calls != 1 {
		t.Fatalf("WithLock = %v, calls=%d; want biz error once", err, calls)
	}
	// 释放后可再次获取
	if _, err := locker.TryLock(ctx, "wl", 5*time.Second); err != nil {
		t.Fatalf("TryLock after WithLock: %v", err)
	}
}

func TestGracefulDegradation(t *testing.T) {
	// 不可达 etcd：严格模式报错，降级模式放行
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}, DialTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	defer func() { _ = cli.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	strict, err := NewEtcdLocker(cli)
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	if _, err := strict.TryLock(ctx, "k", time.Second); err == nil || errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("strict TryLock err = %v; want transport error", err)
	}

	var notified bool
	lax, err := NewEtcdLocker(cli, WithGracefulDegradation(), WithOnLock(func(key string, acquired bool, err error) {
		notified = acquired && err == nil
	}))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	// caller ctx 需 > probe 3s 窗口：probe 对不可达 etcd 要等满窗口才失败，
	// caller deadline 更短会被 #54 的新判定显式报错（见下一条测试）
	ctx2, cancel2 := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel2()
	owner, err := lax.TryLock(ctx2, "k", time.Second)
	if err != nil || owner == "" || !notified {
		t.Fatalf("degraded TryLock = %q, %v, notified=%v", owner, err, notified)
	}
	if err := lax.Unlock(ctx2, "k", owner); err != nil {
		t.Fatalf("degraded Unlock: %v", err)
	}
}

// TestNonPositiveTTLRejected 钉住 #62：ttl≤0 与 redisx 对齐直接报错
// （"ttl must be positive"），不再静默抬成 1s——同一 Locker 接口下，零值/
// 负值在一个后端立刻报错、在另一个后端悄悄持 1s 锁，跨后端移植必踩。
func TestNonPositiveTTLRejected(t *testing.T) {
	cli := testutil.NewClient(t)
	l, err := NewEtcdLocker(cli, WithPrefix("t"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	ctx := context.Background()
	for _, ttl := range []time.Duration{0, -time.Second} {
		if _, err := l.TryLock(ctx, "ttl", ttl); err == nil || !strings.Contains(err.Error(), "ttl must be positive") {
			t.Fatalf("TryLock ttl=%v err = %v; want ttl must be positive", ttl, err)
		}
		if _, err := l.Lock(ctx, "ttl", ttl, time.Second); err == nil || !strings.Contains(err.Error(), "ttl must be positive") {
			t.Fatalf("Lock ttl=%v err = %v; want ttl must be positive", ttl, err)
		}
	}
}

// TestSubSecondTTLRaisedToOneSecond 钉住 #62 的亚秒面：etcd 租约秒级精度，
// (0, 1s) 的 TTL 抬升至 1s（打 Warn）但加解锁语义正常、锁真实生效。
func TestSubSecondTTLRaisedToOneSecond(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	l1, err := NewEtcdLocker(cli, WithPrefix("t"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	l2, err := NewEtcdLocker(cli, WithPrefix("t"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	owner, err := l1.TryLock(ctx, "subms", 200*time.Millisecond)
	if err != nil || owner == "" {
		t.Fatalf("TryLock 200ms = %q, %v; want acquired (raised to 1s)", owner, err)
	}
	if _, err := l2.TryLock(ctx, "subms", 200*time.Millisecond); !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("second TryLock err = %v; want ErrLockNotAcquired", err)
	}
	if err := l1.Unlock(ctx, "subms", owner); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

// TestLockZeroTimeoutSingleAttempt 钉住 #66：timeout≤0 = 单次非阻塞尝试
// （与 redisx 对齐——那边 Lock 先 TryLock 一次再看 deadline）：空闲锁能拿到、
// 被占时立刻 ErrLockTimeout。旧实现 WithTimeout(ctx, 0) 造出立即过期的
// ctx，一次都不试就超时：同接口换后端，redisx timeout=0 拿得到锁、
// etcdx 永远拿不到。
func TestLockZeroTimeoutSingleAttempt(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	l1, err := NewEtcdLocker(cli)
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	l2, err := NewEtcdLocker(cli)
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}

	// 空闲锁：单次尝试成功
	owner, err := l1.Lock(ctx, "zt", 5*time.Second, 0)
	if err != nil || owner == "" {
		t.Fatalf("Lock timeout=0 on free lock = %q, %v; want acquired", owner, err)
	}
	// 被占：单次尝试未得 ⇒ 立刻 ErrLockTimeout（不进入排队等待）
	start := time.Now()
	if _, err := l2.Lock(ctx, "zt", 5*time.Second, 0); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Lock timeout=0 while held err = %v; want ErrLockTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Lock timeout=0 took %v; want immediate single attempt", elapsed)
	}
	// 负 timeout 同口径：单次尝试
	if _, err := l2.Lock(ctx, "zt", 5*time.Second, -time.Second); !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Lock timeout<0 while held err = %v; want ErrLockTimeout", err)
	}
	_ = l1.Unlock(ctx, "zt", owner)
	owner2, err := l2.Lock(ctx, "zt", 5*time.Second, 0)
	if err != nil {
		t.Fatalf("Lock timeout=0 after release: %v", err)
	}
	_ = l2.Unlock(ctx, "zt", owner2)
}

// TestGracefulDegradation_NonTransportErrorExplicit 钉住 #63：降级面只对
// 「etcd 传输层不可达」放行。业务层错误（此处用认证缺失：内嵌 etcd 开启
// auth 后，未带凭据的请求立即被拒，gRPC InvalidArgument 而非 Unavailable）
// 即便开启 WithGracefulDegradation 也必须显式报错——运行期任意故障
// fail-open 会让多实例在故障窗口内同时「拿到锁」，互斥静默失效。
func TestGracefulDegradation_NonTransportErrorExplicit(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	// 开启认证后，未带凭据的客户端一切 KV 请求报 "user name is empty"
	if _, err := cli.UserAdd(ctx, "root", "pw"); err != nil {
		t.Fatalf("UserAdd: %v", err)
	}
	if _, err := cli.UserGrantRole(ctx, "root", "root"); err != nil {
		t.Fatalf("UserGrantRole: %v", err)
	}
	if _, err := cli.AuthEnable(ctx); err != nil {
		t.Fatalf("AuthEnable: %v", err)
	}

	lax, err := NewEtcdLocker(cli, WithGracefulDegradation())
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	owner, err := lax.TryLock(ctx, "k", time.Second)
	if err == nil || owner != "" {
		t.Fatalf("业务层错误（认证缺失）不得降级放行, got owner=%q err=%v", owner, err)
	}
	if strings.HasPrefix(owner, "degraded:") {
		t.Fatalf("不得返回 degraded owner, got %q", owner)
	}
	if !strings.Contains(err.Error(), "user name is empty") {
		t.Fatalf("err 应携带认证错误（业务层故障显式透出）, got %v", err)
	}
}

// TestIsUnreachableClassification 单元钉死降级判定口径（#63）：只有传输层
// 不可达类错误进入优雅降级面，业务层错误一律显式报错。
func TestIsUnreachableClassification(t *testing.T) {
	unreachable := []error{
		context.DeadlineExceeded,
		fmt.Errorf("probe: %w", context.DeadlineExceeded),
		status.Error(codes.Unavailable, "error reading preface"),
		fmt.Errorf("etcd unreachable: %w", status.Error(codes.Unavailable, "connection closed")),
		fmt.Errorf("dial tcp 127.0.0.1:1: %w", syscall.ECONNREFUSED),
		fmt.Errorf("conn reset: %w", syscall.ECONNRESET),
	}
	for _, err := range unreachable {
		if !isUnreachable(err) {
			t.Errorf("isUnreachable(%v) = false; want true", err)
		}
	}
	business := []error{
		errors.New("boom"),
		context.Canceled, // 调用方取消不属降级面（#54：显式报错）
		status.Error(codes.PermissionDenied, "etcdserver: permission denied"),
		status.Error(codes.InvalidArgument, "etcdserver: user name is empty"),
		rpctypes.ErrCompacted, // 压缩冲突：OutOfRange
		fmt.Errorf("wrapped: %w", rpctypes.ErrCompacted),
	}
	for _, err := range business {
		if isUnreachable(err) {
			t.Errorf("isUnreachable(%v) = true; want false", err)
		}
	}
}

// TestGracefulDegradation_CallerCtxDone_ExplicitError 钉住 #54：调用方 ctx
// 自身取消/超时 ≠ etcd 不可达——即便开启 WithGracefulDegradation 也必须
// 显式报错，不得降级放行（多实例同时「拿到锁」并发进临界区 = 互斥失效）。
func TestGracefulDegradation_CallerCtxDone_ExplicitError(t *testing.T) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{"127.0.0.1:1"}, DialTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	defer func() { _ = cli.Close() }()
	lax, err := NewEtcdLocker(cli, WithGracefulDegradation())
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	owner, err := lax.TryLock(ctx, "k", time.Second)
	if err == nil {
		t.Fatalf("caller ctx 超时不得降级放行, got owner=%q", owner)
	}
	if strings.HasPrefix(owner, "degraded:") {
		t.Fatalf("不得返回 degraded owner, got %q", owner)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err 应携带 caller ctx 错误, got %v", err)
	}
}
