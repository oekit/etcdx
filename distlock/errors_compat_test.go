package distlock

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/oekit/etcdx/internal/testutil"
)

// redisxLikeSentinel 复刻 redisx/distlock 哨兵的身份语义做对照（#86）：
// 那边的 ErrLockNotAcquired/ErrLockTimeout 是各自 errors.New 的同文案值
// （redisx/distlock/locker.go:20-23，文案与本包逐字相同）。为测试不引入
// redisx 依赖，用同文案 errors.New 代表它们——跨包 errors.Is 判定走的
// 正是「文案相同的普通 error」这条路径。
func redisxLikeSentinel(msg string) error { return errors.New(msg) }

// TestSentinelCrossPackageCompat 钉住 #86 分歧 1：etcdx 侧哨兵必须对
// redisx/distlock 的同名同文案哨兵 errors.Is 判 true——否则面向 Locker
// 接口的适配层绑 redisx 哨兵跑在 etcdx 上，正常的「锁被占用」被当成内部
// 错误上报（429 变 500）。变异性负对照：把哨兵改回各自 errors.New（文案
// 不变）本测试即转红——errors.Is 比的是值身份，两个 errors.New 恒不等。
func TestSentinelCrossPackageCompat(t *testing.T) {
	// 正对照：与 redisx 同名同文案哨兵（普通 errors.New 值）跨包等价
	if !errors.Is(ErrLockNotAcquired, redisxLikeSentinel("lock not acquired")) {
		t.Error("errors.Is(ErrLockNotAcquired, redisx同文案哨兵) = false; want true（#86）")
	}
	if !errors.Is(ErrLockTimeout, redisxLikeSentinel("lock acquire timeout")) {
		t.Error("errors.Is(ErrLockTimeout, redisx同文案哨兵) = false; want true（#86）")
	}
	// wrap 链上同样成立（适配层常再包一层）
	wrapped := fmt.Errorf("deploy lock: %w", ErrLockNotAcquired)
	if !errors.Is(wrapped, redisxLikeSentinel("lock not acquired")) {
		t.Error("wrap 链上的 ErrLockNotAcquired 对 redisx 同文案哨兵判定 false; want true")
	}

	// 负对照：不是「随便返回 true」——无关错误不得误判
	if errors.Is(ErrLockTimeout, context.DeadlineExceeded) {
		t.Error("errors.Is(ErrLockTimeout, DeadlineExceeded) = true; want false")
	}
	if errors.Is(ErrLockNotAcquired, ErrLockTimeout) {
		t.Error("errors.Is(ErrLockNotAcquired, ErrLockTimeout) = true; want false（文案不同）")
	}
	if errors.Is(ErrLockNotAcquired, nil) {
		t.Error("errors.Is(x, nil) = true; want false")
	}
	if errors.Is(ErrLockNotAcquired, redisxLikeSentinel("lock acquire timeout")) {
		t.Error("文案不同的同族哨兵不得互判")
	}

	// 同包值身份不受影响（既有 6 处 errors.Is 用例依赖）
	if !errors.Is(fmt.Errorf("op: %w", ErrLockTimeout), ErrLockTimeout) {
		t.Error("同包 wrap 链对 ErrLockTimeout 判定 false; want true")
	}
	if ErrLockNotAcquired.Error() != "lock not acquired" ||
		ErrLockTimeout.Error() != "lock acquire timeout" ||
		ErrNotOwner.Error() != "lock not owned by this owner" {
		t.Error("哨兵文案变化会破坏跨包等价判定，必须保持逐字一致")
	}
}

// TestLockerErrorSurfaceConsistentClassification 钉住 #86 的行为级用例：
// 同一适配层（绑「redisx 同文案哨兵」做错误分类）接 etcdx 后端——「锁被他
// 持有」归类为 busy、Unlock 非持有者得到显式错误（非 nil，redisx 侧自其对
// 齐修复起同语义）——换后端错误分类一致，429 不漂移成 500。
func TestLockerErrorSurfaceConsistentClassification(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	locker, err := NewEtcdLocker(cli, WithPrefix("t86"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}

	// 适配层的错误分类：绑 redisx 同文案哨兵（文档推荐的面向接口形态）
	busy := redisxLikeSentinel("lock not acquired")
	classify := func(err error) string {
		switch {
		case err == nil:
			return "ok"
		case errors.Is(err, busy):
			return "busy" // 429 / 不重试
		case errors.Is(err, ErrNotOwner):
			return "not-owner"
		default:
			return "internal" // 500
		}
	}

	ownerA, err := locker.TryLock(ctx, "order", 5*time.Second)
	if err != nil || ownerA == "" {
		t.Fatalf("TryLock A = %q, %v", ownerA, err)
	}

	// 场景 1：锁被他持有 → 适配层分类 busy（换 redisx 后同分类）
	_, err = locker.TryLock(ctx, "order", 5*time.Second)
	if got := classify(err); got != "busy" {
		t.Fatalf("锁被持有时的错误分类 = %s (err=%v); want busy", got, err)
	}

	// 场景 2：Unlock 非持有者 → 显式错误（不是静默 nil），分类 not-owner
	if err := locker.Unlock(ctx, "order", "stale-owner"); err == nil {
		t.Fatal("Unlock 非持有者返回 nil（静默 no-op）; want 显式错误（与 redisx 对齐口径）")
	} else if got := classify(err); got != "not-owner" {
		t.Fatalf("Unlock 非持有者的错误分类 = %s (err=%v); want not-owner", got, err)
	}

	// 持有者正常释放不受影响
	if err := locker.Unlock(ctx, "order", ownerA); err != nil {
		t.Fatalf("Unlock 持有者: %v", err)
	}
	_ = locker.Close(ctx)
}

// TestSentinelCompatBidirectionalViaIsOneOf 钉住 #112：跨包等价的双向入口
// 是 IsOneOf——裸 errors.Is 的等价挂在链头错误的 Is 方法上，redisx 侧哨兵
// 是无 Is 方法的普通 errors.New 值，errors.Is(redisx 哨兵, etcdx 哨兵) 走
// 值比较恒 false（本仓无法单侧修复，治本须 redisx 侧实现同款 Is 或双方
// 共用同一值）；适配层绑**本包**哨兵、后端跑 redisx 时（err 链上是 redisx
// 哨兵），必须用 IsOneOf 分类才不漂移——否则 ErrLockTimeout 被当成 5xx/
// 未知错误，「429 变 500」换个方向复活。变异性负对照：把 IsOneOf 的链上
// 同文案判定删掉，本测试即转红。
func TestSentinelCompatBidirectionalViaIsOneOf(t *testing.T) {
	// 事实钉住：裸 errors.Is 反方向恒 false——注释与适配层不得按「裸 errors.Is
	// 双向成立」来写（#112 的缺陷本体）
	if errors.Is(redisxLikeSentinel("lock acquire timeout"), ErrLockTimeout) {
		t.Error("errors.Is(redisx哨兵, etcdx.ErrLockTimeout) = true；裸 errors.Is 反方向仍是值比较，钉住事实防止注释再度失真")
	}

	// IsOneOf：反方向（redisx 哨兵在链上、etcdx 哨兵做 target）等价成立
	if !IsOneOf(redisxLikeSentinel("lock acquire timeout"), ErrLockTimeout) {
		t.Error("IsOneOf(redisx同文案哨兵, ErrLockTimeout) = false; want true（#112）")
	}
	if !IsOneOf(redisxLikeSentinel("lock not acquired"), ErrLockNotAcquired) {
		t.Error("IsOneOf(redisx同文案哨兵, ErrLockNotAcquired) = false; want true（#112）")
	}
	// wrap 链上的 redisx 哨兵同样命中（适配层常再包一层）
	wrapped := fmt.Errorf("deploy lock: %w", redisxLikeSentinel("lock acquire timeout"))
	if !IsOneOf(wrapped, ErrLockTimeout) {
		t.Error("wrap 链上的 redisx 同文案哨兵对 IsOneOf 判定 false; want true")
	}
	// 正方向（etcdx 哨兵在链上、redisx 同文案哨兵做 target）不回退（#86）
	if !IsOneOf(ErrLockNotAcquired, redisxLikeSentinel("lock not acquired")) {
		t.Error("IsOneOf(ErrLockNotAcquired, redisx同文案哨兵) = false; want true（#86 正方向不得回退）")
	}

	// 负对照：不是「随便返回 true」
	if IsOneOf(redisxLikeSentinel("lock acquire timeout"), ErrLockNotAcquired) {
		t.Error("文案不同的同族哨兵不得互判")
	}
	if IsOneOf(context.DeadlineExceeded, ErrLockTimeout) {
		t.Error("IsOneOf(DeadlineExceeded, ErrLockTimeout) = true; want false")
	}
	if IsOneOf(redisxLikeSentinel("lock acquire timeout"), nil) {
		t.Error("nil target 不得参与判定")
	}
	if IsOneOf(nil, ErrLockTimeout) {
		t.Error("IsOneOf(nil, ...) = true; want false")
	}
	if IsOneOf(redisxLikeSentinel("lock acquire timeout")) {
		t.Error("targets 为空不得判 true")
	}
}

// TestAdapterClassificationIndependentOfBoundSide 钉住 #112 的行为级用例
// （#86 行为级用例的反方向）：同一适配层**绑 etcdx 哨兵**做错误分类，后端
// 给来的是 redisx 形态的错误（无 Is 方法的同文案哨兵、可再包一层）——
// 用 IsOneOf 后与「绑 redisx 哨兵跑 etcdx」的分类完全一致：429/500 不随
// 「绑哪一侧」漂移。
func TestAdapterClassificationIndependentOfBoundSide(t *testing.T) {
	classify := func(err error) string {
		switch {
		case err == nil:
			return "ok"
		case IsOneOf(err, ErrLockNotAcquired):
			return "busy" // 429 / 不重试
		case IsOneOf(err, ErrLockTimeout):
			return "timeout" // 429 / 不重试
		default:
			return "internal" // 500
		}
	}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"redisx 后端裸哨兵", redisxLikeSentinel("lock not acquired"), "busy"},
		{"redisx 后端 wrap 链", fmt.Errorf("deploy: %w", redisxLikeSentinel("lock acquire timeout")), "timeout"},
		{"etcdx 后端链（正方向不回退）", fmt.Errorf("deploy: %w", ErrLockNotAcquired), "busy"},
	}
	for _, tc := range cases {
		if got := classify(tc.err); got != tc.want {
			t.Errorf("%s: 分类 = %s (err=%v); want %s", tc.name, got, tc.err, tc.want)
		}
	}
}
