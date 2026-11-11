package distlock

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	pkgdistlock "github.com/oekit/pkg/distlock"

	"github.com/oekit/etcdx/internal/testutil"
)

// #123 契约单源回归：本包哨兵是 pkg/distlock 契约哨兵的同值别名（不是
// 文案等价补丁），ErrNotOwner 映射到 ErrLockNotHeld；sentinel.Is / IsOneOf
// 双向文案匹配机制已删除，跨包分类只靠同值身份 + 裸 errors.Is。

// TestSentinelsArePkgContractValues 钉住哨兵映射（#123）：本包哨兵与
// pkg/distlock 契约哨兵是**同一个值**——同进程只此一套，errors.Is 双向
// 天然成立，适配层绑哪侧哨兵分类都不漂移。变异性负对照：把任一哨兵改回
// errors.New 自有值，本测试即转红（不同值恒 Is false），#86/#112 的补丁
// 机制不得复活。
func TestSentinelsArePkgContractValues(t *testing.T) {
	if ErrLockNotAcquired != pkgdistlock.ErrLockNotAcquired {
		t.Fatal("ErrLockNotAcquired 应是 pkg/distlock 契约哨兵的同值别名（#123）")
	}
	if ErrLockTimeout != pkgdistlock.ErrLockTimeout {
		t.Fatal("ErrLockTimeout 应是 pkg/distlock 契约哨兵的同值别名（#123）")
	}
	if ErrNotOwner != pkgdistlock.ErrLockNotHeld {
		t.Fatal("ErrNotOwner 应映射到 pkg/distlock.ErrLockNotHeld（#123 哨兵映射表）")
	}
	if ErrLockNotHeld != pkgdistlock.ErrLockNotHeld {
		t.Fatal("ErrLockNotHeld 应是 pkg/distlock 契约哨兵的同值别名（#123）")
	}

	// errors.Is 双向（同值别名，两侧任一写法都命中——#112 的反方向缺口
	// 由同值身份结构性关闭，不再需要 IsOneOf 补丁）
	if !errors.Is(ErrLockTimeout, pkgdistlock.ErrLockTimeout) {
		t.Fatal("errors.Is(etcdx.ErrLockTimeout, pkg.ErrLockTimeout) = false; want true")
	}
	if !errors.Is(pkgdistlock.ErrLockTimeout, ErrLockTimeout) { //nolint:staticcheck // SA1032 误报：两侧是同一值，此处刻意断言反方向（#112 缺口由同值身份关闭）
		t.Fatal("errors.Is(pkg.ErrLockTimeout, etcdx.ErrLockTimeout) = false; want true（同值双向）")
	}
	if !errors.Is(pkgdistlock.ErrLockNotHeld, ErrNotOwner) { //nolint:staticcheck // SA1032 误报：两侧是同一值，此处刻意断言反方向（#123 映射双向）
		t.Fatal("errors.Is(pkg.ErrLockNotHeld, etcdx.ErrNotOwner) = false; want true（映射同值）")
	}

	// 哨兵间互不误判、无关错误不误入
	if errors.Is(ErrLockNotAcquired, ErrLockTimeout) {
		t.Fatal("ErrLockNotAcquired 与 ErrLockTimeout 不应等价")
	}
	if errors.Is(ErrLockTimeout, context.DeadlineExceeded) {
		t.Fatal("ErrLockTimeout 对 DeadlineExceeded 不应等价")
	}
	if errors.Is(ErrLockNotAcquired, ErrLockNotHeld) {
		t.Fatal("ErrLockNotAcquired 与 ErrLockNotHeld 不应等价")
	}

	// wrap 链上的哨兵判定不丢
	if !errors.Is(fmt.Errorf("deploy lock: %w", ErrLockNotAcquired), pkgdistlock.ErrLockNotAcquired) {
		t.Fatal("wrap 链上的 ErrLockNotAcquired 对 pkg 哨兵判定 false; want true")
	}
}

// TestSentinelTextsContractPinned 钉住对外文案（#123 哨兵映射）：NotAcquired/
// Timeout 文案与历史逐字一致；ErrNotOwner 文案由 "lock not owned by this
// owner" 统一为 pkg 契约文案 "lock not held by this owner"（对外文案变化，
// changelog BREAKING 注记）——上游若按旧文案做聚合/匹配必须改走 errors.Is。
func TestSentinelTextsContractPinned(t *testing.T) {
	if got := ErrLockNotAcquired.Error(); got != "lock not acquired" {
		t.Fatalf("ErrLockNotAcquired 文案 = %q; want %q", got, "lock not acquired")
	}
	if got := ErrLockTimeout.Error(); got != "lock acquire timeout" {
		t.Fatalf("ErrLockTimeout 文案 = %q; want %q", got, "lock acquire timeout")
	}
	if got := ErrNotOwner.Error(); got != "lock not held by this owner" {
		t.Fatalf("ErrNotOwner 文案 = %q; want %q（#123 映射后的 pkg 契约文案）",
			got, "lock not held by this owner")
	}
	if got := ErrLockNotHeld.Error(); got != "lock not held by this owner" {
		t.Fatalf("ErrLockNotHeld 文案 = %q; want %q", got, "lock not held by this owner")
	}
}

// TestLockerErrorSurfaceClassifiesViaPkgSentinels 行为级回归（#123）：
// 适配层绑 **pkg 契约哨兵** 做错误分类，接 etcdx 后端——「锁被他持有」
// 归类 busy、Unlock 非持有者得到显式 ErrLockNotHeld——同一适配层代码换
// redisx 后端（迁移后同契约）分类不变，429 不漂移成 500。裸 errors.Is
// 即正确分类，无需 IsOneOf。
func TestLockerErrorSurfaceClassifiesViaPkgSentinels(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	locker, err := NewEtcdLocker(cli, WithPrefix("t123"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}

	// 适配层的错误分类：绑 pkg 契约哨兵（面向中立契约的推荐形态）
	classify := func(err error) string {
		switch {
		case err == nil:
			return "ok"
		case errors.Is(err, pkgdistlock.ErrLockNotAcquired):
			return "busy" // 429 / 不重试
		case errors.Is(err, pkgdistlock.ErrLockNotHeld):
			return "not-held"
		case errors.Is(err, pkgdistlock.ErrLockTimeout):
			return "timeout" // 429 / 不重试
		default:
			return "internal" // 500
		}
	}

	ownerA, err := locker.TryLock(ctx, "order", 5*time.Second)
	if err != nil || ownerA == "" {
		t.Fatalf("TryLock A = %q, %v", ownerA, err)
	}

	// 场景 1：锁被他持有 → 分类 busy（换 redisx 后同分类）
	_, err = locker.TryLock(ctx, "order", 5*time.Second)
	if got := classify(err); got != "busy" {
		t.Fatalf("锁被持有时的错误分类 = %s (err=%v); want busy", got, err)
	}

	// 场景 2：Unlock 非持有者 → 显式错误（不是静默 nil），分类 not-held
	err = locker.Unlock(ctx, "order", "stale-owner")
	if err == nil {
		t.Fatal("Unlock 非持有者返回 nil（静默 no-op）; want 显式 ErrLockNotHeld")
	}
	if got := classify(err); got != "not-held" {
		t.Fatalf("Unlock 非持有者的错误分类 = %s (err=%v); want not-held", got, err)
	}
	// 历史名与契约名命中同一个值
	if !errors.Is(err, ErrNotOwner) || !errors.Is(err, ErrLockNotHeld) {
		t.Fatal("Unlock 非持有者错误应同时命中 ErrNotOwner 与 ErrLockNotHeld（同值）")
	}

	// 场景 3：Lock 超时 → 分类 timeout
	if _, err := locker.Lock(ctx, "order", 5*time.Second, 100*time.Millisecond); classify(err) != "timeout" {
		t.Fatalf("Lock 超时错误分类 err=%v; want timeout", err)
	}

	// 持有者正常释放不受影响
	if err := locker.Unlock(ctx, "order", ownerA); err != nil {
		t.Fatalf("Unlock 持有者: %v", err)
	}
	_ = locker.Close(ctx)
}
