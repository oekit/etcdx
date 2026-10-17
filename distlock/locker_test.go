package distlock

import (
	"context"
	"errors"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oekit/etcdx/internal/testutil"
)

func TestTryLockAndUnlock(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	l1 := NewEtcdLocker(cli, WithPrefix("t"))
	l2 := NewEtcdLocker(cli, WithPrefix("t"))

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
	l1 := NewEtcdLocker(cli)
	l2 := NewEtcdLocker(cli)

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
	l1 := NewEtcdLocker(cli)
	l2 := NewEtcdLocker(cli)
	owner, _ := l1.TryLock(context.Background(), "ctx", 5*time.Second)
	defer l1.Unlock(context.Background(), "ctx", owner) //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_, err := l2.Lock(ctx, "ctx", 5*time.Second, 10*time.Second)
	if err == nil || errors.Is(err, ErrLockTimeout) {
		t.Fatalf("canceled ctx err = %v; want context error, not ErrLockTimeout", err)
	}
}

func TestWithLock(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	locker := NewEtcdLocker(cli)
	var calls int
	err := WithLock(ctx, locker, "wl", 5*time.Second, time.Second, func() error {
		calls++
		// 持锁期间他人拿不到
		if _, err := NewEtcdLocker(cli).TryLock(ctx, "wl", 5*time.Second); !errors.Is(err, ErrLockNotAcquired) {
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

	strict := NewEtcdLocker(cli)
	if _, err := strict.TryLock(ctx, "k", time.Second); err == nil || errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("strict TryLock err = %v; want transport error", err)
	}

	var notified bool
	lax := NewEtcdLocker(cli, WithGracefulDegradation(), WithOnLock(func(key string, acquired bool, err error) {
		notified = acquired && err == nil
	}))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	owner, err := lax.TryLock(ctx2, "k", time.Second)
	if err != nil || owner == "" || !notified {
		t.Fatalf("degraded TryLock = %q, %v, notified=%v", owner, err, notified)
	}
	if err := lax.Unlock(ctx2, "k", owner); err != nil {
		t.Fatalf("degraded Unlock: %v", err)
	}
}
