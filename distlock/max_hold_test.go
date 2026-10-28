package distlock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oekit/etcdx/internal/testutil"
)

// TestMaxHold_SafetyNetReleasesLock 回归测试（#36）：**漏调 Unlock** 时，
// 最大持锁时长到期后安全网停止续租 ⇒ 租约在 TTL 后自然过期 ⇒ 锁被释放，
// 另一个实例可以拿到。
//
// 缺陷成因：etcd Session 的 keepalive **无限续租**（ctx 只在 Session.Close/
// Orphan 时取消）⇒ 只要调用方漏掉 Unlock（且进程不退出），该锁的租约就永远
// 不过期 ⇒ 锁被永久持有 ⇒ 集群里其它实例每次都 ErrLockTimeout，
// 该业务功能**彻底不可用且不可自愈**。
//
// 这正是 redisx/distlock 侧用 WithMaxAutoRenew 显式防掉的那类事故；
// etcdx 的 Locker 接口自述「与 redisx/distlock.Locker 同形」，却缺同等安全网。
func TestMaxHold_SafetyNetReleasesLock(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()

	const maxHold = 300 * time.Millisecond
	const ttl = 2 * time.Second

	l1, err := NewEtcdLocker(cli, WithPrefix("t"), WithMaxHold(maxHold))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	l2, err := NewEtcdLocker(cli, WithPrefix("t"), WithMaxHold(maxHold))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	t.Cleanup(func() { _ = l2.Close(ctx) })

	owner, err := l1.TryLock(ctx, "job", ttl)
	if err != nil || owner == "" {
		t.Fatalf("TryLock = %q, %v", owner, err)
	}
	// **故意不 Unlock**——模拟调用方漏调。

	// 立刻：锁确实被持有（安全网还没到期）。
	if _, err := l2.TryLock(ctx, "job", ttl); !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("刚加锁时另一个实例应拿不到：err = %v", err)
	}

	// 等安全网到期（maxHold）→ 停止续租 → 租约在 TTL 后自然过期 → 键删除。
	deadline := time.Now().Add(maxHold + 3*ttl)
	for time.Now().Before(deadline) {
		if _, err := l2.TryLock(ctx, "job", ttl); err == nil {
			return // 拿到了 ⇒ 安全网生效，锁没有永久持有
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("安全网到期后锁仍被永久持有：另一个实例始终拿不到 ⇒ 与 redisx 侧 WithMaxAutoRenew 的语义不对称")
}

// TestMaxHold_UnlockStopsSafetyNet 回归护栏：正常 Unlock 后安全网必须**停掉**
// （否则它会在锁已正常释放之后再动一次租约）。
func TestMaxHold_UnlockStopsSafetyNet(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()

	l1, err := NewEtcdLocker(cli, WithPrefix("t"), WithMaxHold(200*time.Millisecond))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	l2, err := NewEtcdLocker(cli, WithPrefix("t"), WithMaxHold(200*time.Millisecond))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	t.Cleanup(func() { _ = l2.Close(ctx) })

	owner, err := l1.TryLock(ctx, "job", 2*time.Second)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if err := l1.Unlock(ctx, "job", owner); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	// 正常释放后，另一个实例立刻能拿到（不必等安全网到期）。
	if _, err := l2.TryLock(ctx, "job", 2*time.Second); err != nil {
		t.Fatalf("Unlock 后另一个实例应能拿到锁：%v", err)
	}
	// 等过安全网的时长，确认它没有把已经正常释放的锁再折腾出异常。
	time.Sleep(400 * time.Millisecond)
}

// TestMaxHold_NegativeDisablesSafetyNet 回归护栏：负值 = 不限制（兼容旧行为，
// 完全依赖调用方 Unlock）。
func TestMaxHold_NegativeDisablesSafetyNet(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()

	l, err := NewEtcdLocker(cli, WithPrefix("t"), WithMaxHold(-1))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	owner, err := l.TryLock(ctx, "job", 2*time.Second)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	// 不限制时，本次持有不启动安全网 ⇒ 没有 stopSafety 可停。
	l.mu.Lock()
	h := l.held["job"][owner]
	l.mu.Unlock()
	if h == nil {
		t.Fatal("持有记录缺失")
	}
	if h.stopSafety != nil {
		t.Error("WithMaxHold(-1) 不应启动安全网")
	}
	if err := l.Unlock(ctx, "job", owner); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}
