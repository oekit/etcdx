package distlock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oekit/etcdx/internal/testutil"
)

// 本文件锚定 data#299（etcdx 半边）的释放重试语义：
//   - Unlock/Close 先 release、成功（或判定锁已不存在）后才删除 held 记录；
//   - 通信类失败保留记录并返回可重试错误，同一 (key, ownerID) 可再次 Unlock；
//   - Close 失败条目留在账本、错误汇总返回，可重试；
//   - 释放进行中的条目不被并发 Unlock/Close 重复发起。

func newBookkeepingLocker() *EtcdLocker {
	return &EtcdLocker{held: make(map[string]map[string]*heldLock)}
}

// TestUnlock_CommunicationFailureKeepsHeldRecord：release 返回通信错误时
// held 记录不得删除（旧行为先删后释放 ⇒ 重试变成 ErrNotOwner 终态）；
// 复位后可重试并成功清理。
func TestUnlock_CommunicationFailureKeepsHeldRecord(t *testing.T) {
	l := newBookkeepingLocker()
	h := &heldLock{ownerID: "owner-1"}
	l.setHeld("job", "owner-1", h)

	boom := errors.New("network reset while deleting lock key")
	var calls int
	l.releaseFunc = func(context.Context, string, *heldLock) error {
		calls++
		return boom
	}

	err := l.Unlock(context.Background(), "job", "owner-1")
	if !errors.Is(err, boom) {
		t.Fatalf("Unlock err = %v; want 通信错误原样可判", err)
	}
	if got := l.held["job"]["owner-1"]; got != h {
		t.Fatalf("通信失败后 held 记录被删（重试将变 ErrNotOwner 终态）: %v", got)
	}
	if h.releasing {
		t.Fatal("失败后 releasing 必须复位，允许重试")
	}

	// 重试：release 成功（nil session = 无需与 etcd 交互）⇒ 记录删除。
	l.releaseFunc = nil
	if err = l.Unlock(context.Background(), "job", "owner-1"); err != nil {
		t.Fatalf("重试 Unlock 应成功: %v", err)
	}
	if _, ok := l.held["job"]; ok {
		t.Fatalf("成功释放后 held 记录应删除: %v", l.held)
	}
	if calls != 1 {
		t.Fatalf("release 调用 = %d, want 1（重试走真实 release）", calls)
	}
}

// TestUnlock_TerminalNotHeldDropsRecord：ErrLockNotHeld 是终态（锁不存在/
// 已易主），记录必须删除，且不提供重试语义。
func TestUnlock_TerminalNotHeldDropsRecord(t *testing.T) {
	l := newBookkeepingLocker()
	h := &heldLock{ownerID: "owner-1"}
	l.setHeld("job", "owner-1", h)
	l.releaseFunc = func(context.Context, string, *heldLock) error { return ErrLockNotHeld }

	err := l.Unlock(context.Background(), "job", "owner-1")
	if !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("Unlock err = %v; want ErrLockNotHeld", err)
	}
	if _, ok := l.held["job"]; ok {
		t.Fatalf("终态 ErrLockNotHeld 应删除持有记录: %v", l.held)
	}
}

// TestClose_FailedEntriesRetainedAndRetried：Close 不再先整体取走账本；
// 失败条目留在 held，全部错误经 errors.Join 汇总；重试成功后清空。
func TestClose_FailedEntriesRetainedAndRetried(t *testing.T) {
	l := newBookkeepingLocker()
	l.setHeld("k1", "owner-a", &heldLock{ownerID: "owner-a"})
	l.setHeld("k2", "owner-b", &heldLock{ownerID: "owner-b"})

	boomA := errors.New("k1 unavailable")
	boomB := errors.New("k2 unavailable")
	fail := map[string]error{"k1": boomA, "k2": boomB}
	l.releaseFunc = func(_ context.Context, key string, _ *heldLock) error { return fail[key] }

	err := l.Close(context.Background())
	if !errors.Is(err, boomA) || !errors.Is(err, boomB) {
		t.Fatalf("Close 应汇总全部失败条目, got %v", err)
	}
	if len(l.held) != 2 || l.held["k1"]["owner-a"] == nil || l.held["k2"]["owner-b"] == nil {
		t.Fatalf("失败条目必须留在账本供重试, got %v", l.held)
	}
	for _, byOwner := range l.held {
		for _, h := range byOwner {
			if h.releasing {
				t.Fatal("失败后 releasing 必须复位")
			}
		}
	}

	// 第二次 Close：全部成功 ⇒ 账本清空。
	l.releaseFunc = nil
	if err = l.Close(context.Background()); err != nil {
		t.Fatalf("重试 Close 应成功: %v", err)
	}
	if len(l.held) != 0 {
		t.Fatalf("重试 Close 后账本应为空, got %v", l.held)
	}
}

// TestUnlock_ConcurrentDuplicateRejected：释放进行中的条目被并发 Unlock
// 视为非持有者（与「记录已删除」的单次释放语义一致），不会重复 Delete。
func TestUnlock_ConcurrentDuplicateRejected(t *testing.T) {
	l := newBookkeepingLocker()
	l.setHeld("job", "owner-1", &heldLock{ownerID: "owner-1"})

	entered := make(chan struct{})
	unblock := make(chan struct{})
	l.releaseFunc = func(context.Context, string, *heldLock) error {
		close(entered)
		<-unblock
		return nil
	}

	go func() { _ = l.Unlock(context.Background(), "job", "owner-1") }()
	<-entered
	if err := l.Unlock(context.Background(), "job", "owner-1"); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("并发重复 Unlock err = %v; want ErrNotOwner", err)
	}
	close(unblock)

	// 等首个 Unlock 完成并清账（轮询避免依赖调度顺序）。
	deadline := time.Now().Add(2 * time.Second)
	for {
		l.mu.Lock()
		_, still := l.held["job"]
		l.mu.Unlock()
		if !still {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("首个 Unlock 未清理账本")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestUnlock_CanceledCtxStillReleasesViaSessionRevoke：caller ctx 已取消时
// Mutex.Unlock 会失败，但 Session.Close（撤销租约）成功即达成契约结果
// （会话全部键删除）——此时必须返回 nil 并删记录，而不是留下可重试错误
// 让调用方再撞 ErrLockReleased/ErrLeaseNotFound。
func TestUnlock_CanceledCtxStillReleasesViaSessionRevoke(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	l, err := NewEtcdLocker(cli, WithPrefix("t299"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	owner, err := l.TryLock(ctx, "job", 5*time.Second)
	if err != nil || owner == "" {
		t.Fatalf("TryLock = %q, %v", owner, err)
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel() // 释放路径的 Delete 必然失败
	if err = l.Unlock(cctx, "job", owner); err != nil {
		t.Fatalf("撤销租约成功时 Unlock 应返回 nil（锁已释放）: %v", err)
	}
	l.mu.Lock()
	_, still := l.held["job"]
	l.mu.Unlock()
	if still {
		t.Fatal("释放成功（会话已撤销）后 held 记录必须删除")
	}

	// 锁确实可被另一个 locker 重新获取（互斥已解除）。
	l2, err := NewEtcdLocker(cli, WithPrefix("t299"))
	if err != nil {
		t.Fatalf("NewEtcdLocker(l2): %v", err)
	}
	owner2, err := l2.TryLock(ctx, "job", 5*time.Second)
	if err != nil {
		t.Fatalf("释放后 l2 TryLock: %v", err)
	}
	if err = l2.Unlock(ctx, "job", owner2); err != nil {
		t.Fatalf("l2 Unlock: %v", err)
	}
}
