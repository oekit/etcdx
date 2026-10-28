package distlock

import (
	"context"
	"testing"
)

// TestLocker_HeldMap_NoSilentOverwrite 回归测试：同一个 key 的**两个持有者**
// 不得互相覆盖。
//
// 缺陷成因：held 是「key → 条目」的单层映射，写入时直接 `l.held[key] = ...`
// ⇒ 后写者**静默覆盖**前一个 ⇒
//   - 前一个的 Session 永远不被 Close（租约继续续期 ⇒ 锁不释放）；
//   - 前一个 owner 的 Unlock 因 ownerID 不匹配报 ErrNotOwner ⇒ 它的锁也放不掉。
//
// 典型触发路径：进程已真实持有 key 的锁，随后 etcd 不可用、另一次加锁走降级
// 放行路径（degradeOrFail 也会写 held）⇒ 真实持有记录被降级记录顶掉。
//
// 修法：held 改为「key → ownerID → 条目」，各持有者各占一条。
func TestLocker_HeldMap_NoSilentOverwrite(t *testing.T) {
	l := &EtcdLocker{held: make(map[string]map[string]*heldLock)}

	// 模拟两个持有者：真实持锁（A，带 session）与降级放行（B，无 session）。
	l.setHeld("k", "owner-A", &heldLock{ownerID: "owner-A"})
	l.setHeld("k", "owner-B", &heldLock{ownerID: "owner-B"})

	if got := len(l.held["k"]); got != 2 {
		t.Fatalf("同 key 的持有者数 = %d，期望 2（不得互相覆盖）", got)
	}
	if l.held["k"]["owner-A"] == nil || l.held["k"]["owner-B"] == nil {
		t.Fatalf("持有记录被覆盖：%v", l.held["k"])
	}
}

// TestLocker_Unlock_ExactOwner 回归测试：Unlock 按 ownerID 精确释放，别的
// owner 的记录不受影响。
func TestLocker_Unlock_ExactOwner(t *testing.T) {
	l := &EtcdLocker{held: make(map[string]map[string]*heldLock)}
	l.setHeld("k", "owner-A", &heldLock{ownerID: "owner-A"})
	l.setHeld("k", "owner-B", &heldLock{ownerID: "owner-B"})

	// 不存在的 owner ⇒ ErrNotOwner，且不影响他人记录。
	if err := l.Unlock(context.Background(), "k", "owner-X"); err != ErrNotOwner {
		t.Fatalf("未知 owner 的 Unlock 应返回 ErrNotOwner，实得 %v", err)
	}
	if len(l.held["k"]) != 2 {
		t.Fatalf("失败的 Unlock 影响了他人记录：%v", l.held["k"])
	}

	// 精确释放 A（无 session ⇒ release 是 no-op）。
	if err := l.Unlock(context.Background(), "k", "owner-A"); err != nil {
		t.Fatalf("Unlock(owner-A): %v", err)
	}
	if l.held["k"]["owner-A"] != nil {
		t.Fatal("owner-A 的记录未被释放")
	}
	if l.held["k"]["owner-B"] == nil {
		t.Fatal("owner-B 的记录被误删")
	}
}

// TestLocker_Close_ReleasesAllOwners 回归护栏：Close 释放**全部**持有者
// （同 key 多个 ownerID 都要释放）。
func TestLocker_Close_ReleasesAllOwners(t *testing.T) {
	l := &EtcdLocker{held: make(map[string]map[string]*heldLock)}
	l.setHeld("k1", "owner-A", &heldLock{ownerID: "owner-A"})
	l.setHeld("k1", "owner-B", &heldLock{ownerID: "owner-B"})
	l.setHeld("k2", "owner-C", &heldLock{ownerID: "owner-C"})

	if err := l.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if len(l.held) != 0 {
		t.Fatalf("Close 之后仍有持有记录：%v", l.held)
	}
}
