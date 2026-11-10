package distlock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
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

// TestSentinelCrossPackageCompatOneWayDirection 钉住 #112 修正后的契约：
// 跨包等价是**单向**的。stdlib errors.Is 只在错误链一侧调用 Is 方法——反方向
// errors.Is(redisx 哨兵, etcdx 哨兵) 中 redisx 侧的 errors.New 值（本文件
// redisxLikeSentinel 同身份语义）无 Is 方法，与本包 sentinel 值比较也恒不等，
// etcdx 侧任何写法都无法让反方向变 true（真正双向需 redisx 侧实现同款 Is
// 或双方共用哨兵类型，跨仓另行处理）。#86 注释曾按「双向」承诺（「绑哪一侧
// 的哨兵都能正确分类」「或反向」），本用例按实际语义钉住方向，防止下游按
// 双向写适配层。
func TestSentinelCrossPackageCompatOneWayDirection(t *testing.T) {
	// 反方向：恒 false（把方向写死，误用即红）
	if errors.Is(redisxLikeSentinel("lock acquire timeout"), ErrLockTimeout) {
		t.Error("errors.Is(redisx同文案哨兵, ErrLockTimeout) = true; want false（单向等价，#112）")
	}
	if errors.Is(redisxLikeSentinel("lock not acquired"), ErrLockNotAcquired) {
		t.Error("errors.Is(redisx同文案哨兵, ErrLockNotAcquired) = true; want false（单向等价，#112）")
	}

	// 适配层绑定矩阵（#112）：errors.Is 只查错误链一侧的 Is ⇒
	//   - target 绑 redisx 侧哨兵：etcdx 后端错误经 sentinel.Is 同文案 ✓、
	//     redisx 后端错误按值身份 ✓ ——两后端通用（推荐绑定形态）
	//   - target 绑 etcdx 侧哨兵：etcdx 后端 ✓（值身份）、redisx 后端 ✗（上
	//     方反方向恒 false）——跑在 redisx 后端把 ErrLockTimeout 当未知错误
	redisxSentinel := redisxLikeSentinel("lock acquire timeout")
	etcdxChain := fmt.Errorf("deploy lock: %w", ErrLockTimeout)
	if !errors.Is(etcdxChain, redisxSentinel) {
		t.Error("绑 redisx 侧哨兵：etcdx 后端错误分类失败; want true")
	}
	if !errors.Is(redisxSentinel, redisxSentinel) {
		t.Error("绑 redisx 侧哨兵：redisx 后端错误分类失败; want true")
	}
}

// TestSentinelContractCommentOneWay 钉住 #112 的契约文本面：本缺陷的主体是
// 注释承诺与实测语义不符（承诺「双向」、实测单向），文档不得再出现已被证伪
// 的双向承诺——否则下游按「绑哪一侧都能分类」写适配层，在 redisx 后端上把
// ErrLockTimeout 误分类成 5xx。等价方向的语义由
// TestSentinelCrossPackageCompatOneWayDirection 钉住。
func TestSentinelContractCommentOneWay(t *testing.T) {
	overPromise := []string{"绑哪一侧的哨兵都能正确分类", "或反向）错误分类不漂移", "（或反向）"}
	for _, file := range []string{"locker.go", "doc.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		for _, phrase := range overPromise {
			if strings.Contains(string(src), phrase) {
				t.Errorf("%s 仍承诺双向跨包等价（%q）; want 删除双向承诺并写明单向（#112）", file, phrase)
			}
		}
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
