package distlock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oekit/etcdx/internal/testutil"
)

// #137 回归：租约失效（TTL 过期 / etcd 侧回收 / MaxHold 安全网停续租）后的
// 陈旧 Unlock 必须返回契约哨兵 ErrLockNotHeld——契约原文（pkg/distlock
// Locker.Unlock）：「锁不存在或已易主（陈旧持有者解锁）返回 ErrLockNotHeld
// （调用方不得继续假装持有）；其它 error 为后端通信故障」。此前该场景返回
// etcd 原始错误（requested lease not found），恰好顶替了契约点名的
// 「锁已易主」哨兵，调用方无法用 errors.Is 判定「我已不再持有」。
//
// 复现手法：直接撤销持有者的租约（等价于 TTL 过期后的租约回收；比等
// WithMaxHold 安全网快且无时序窗口），等 session 的 keepalive goroutine
// 感知到租约失效，再由第二个 locker 拿到同一把锁（证明锁确实已易主），
// 最后让原持有者 Unlock。
func TestUnlockAfterLeaseLossReturnsContractSentinel(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()

	l1, err := NewEtcdLocker(cli, WithPrefix("t137"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	owner, err := l1.TryLock(ctx, "job", 2*time.Second)
	if err != nil || owner == "" {
		t.Fatalf("TryLock = %q, %v", owner, err)
	}

	// 取出 l1 的持有记录：拿租约 ID 直接撤销（in-package 测试可访问 held）
	l1.mu.Lock()
	h := l1.held["job"][owner]
	l1.mu.Unlock()
	if h == nil || h.session == nil {
		t.Fatalf("held record missing for (job, %s)", owner)
	}
	if _, err := cli.Revoke(ctx, h.session.Lease()); err != nil {
		t.Fatalf("revoke lease: %v", err)
	}
	select {
	case <-h.session.Done(): // keepalive goroutine 已感知租约失效
	case <-time.After(5 * time.Second):
		t.Fatal("session keepalive goroutine did not observe lease revocation")
	}

	// 第二个 locker 拿到同一把锁：证明锁确实已易主（前提成立）
	l2, err := NewEtcdLocker(cli, WithPrefix("t137"))
	if err != nil {
		t.Fatalf("NewEtcdLocker(l2): %v", err)
	}
	owner2, err := l2.TryLock(ctx, "job", 2*time.Second)
	if err != nil {
		t.Fatalf("l2 TryLock after lease loss: %v（锁未易主，前提不成立）", err)
	}
	t.Cleanup(func() { _ = l2.Close(context.Background()) })

	// 陈旧持有者 Unlock：必须返回契约哨兵 ErrLockNotHeld（ErrNotOwner 与
	// 其同值），且不得漏出 etcd 原始错误文案
	err = l1.Unlock(ctx, "job", owner)
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("stale Unlock err = %v; want ErrLockNotHeld（契约哨兵，#137）", err)
	}
	if strings.Contains(err.Error(), "requested lease not found") {
		t.Fatalf("stale Unlock 漏出 etcd 原始错误文案: %v（应翻译为契约哨兵，#137）", err)
	}

	// 契约语义「本次 Unlock 没有删除任何锁」：新持有者的锁不受陈旧 Unlock
	// 影响——第三个 locker 仍抢不到
	l3, err := NewEtcdLocker(cli, WithPrefix("t137"))
	if err != nil {
		t.Fatalf("NewEtcdLocker(l3): %v", err)
	}
	if _, err := l3.TryLock(ctx, "job", 2*time.Second); !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("l3 TryLock err = %v; want ErrLockNotAcquired（陈旧 Unlock 不得动到新持有者的锁）", err)
	}
	if err := l3.Close(ctx); err != nil {
		t.Fatalf("l3 Close: %v", err)
	}
	_ = owner2
}
