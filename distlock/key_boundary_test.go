package distlock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oekit/etcdx/internal/testutil"
)

// #138 回归：锁键不得含 "/"（键层级边界，规则单源 internal/keyseg，与
// registry 侧 #110 的同类约定对齐）。缺陷形态：concurrency.Mutex 的互斥
// 判定用前缀范围查询（WithFirstCreate），锁键 "a/b" 的 owner 键
// <pfx>/a/b/<lease> 落在锁 "a" 的互斥前缀 <pfx>/a/ 之内——持有 "a/b" 时
// "a" 永远拿不到锁（返回 lock not acquired 的单向过度互斥），且全程无
// 报错提示键名有问题。修复后含 "/" 的锁键在任何网络交互前 fail-fast。
func TestLockKeyHierarchyBoundary(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	l, err := NewEtcdLocker(cli, WithPrefix("t138"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}

	// TryLock 与 Lock 两条入口都 fail-fast
	if _, err := l.TryLock(ctx, "a/b", 2*time.Second); err == nil {
		t.Fatal("TryLock(a/b) 成功；want 键层级边界 fail-fast（#138）")
	} else if errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("TryLock(a/b) 返回 ErrLockNotAcquired——这是修复前的错误行为"+
			"（a/b 与 a 单向过度互斥），want 参数错误: %v", err)
	} else if !strings.Contains(err.Error(), "key hierarchy boundary") {
		t.Fatalf("TryLock(a/b) err = %v; want 键层级边界错误（规则单源 internal/keyseg）", err)
	}
	if _, err := l.Lock(ctx, "a/b", 2*time.Second, time.Second); err == nil {
		t.Fatal("Lock(a/b) 成功；want 键层级边界 fail-fast（#138）")
	}

	// 干净键不受影响：含 "/" 的键被拒绝后，"a" 与 "a/b" 不再可能互相干扰，
	// "a" 的加锁/释放语义与修复前完全一致
	owner, err := l.TryLock(ctx, "a", 2*time.Second)
	if err != nil {
		t.Fatalf("TryLock(a): %v", err)
	}
	if err := l.Unlock(ctx, "a", owner); err != nil {
		t.Fatalf("Unlock(a): %v", err)
	}

	if err := l.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
