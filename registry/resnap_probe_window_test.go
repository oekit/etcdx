package registry

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oekit/etcdx/internal/probe"
)

// #196 回归：重建循环的快照重取（resnap）必须被探活窗口罩住——h.ctx 由
// WithCancel(Background) 派生、无 deadline，etcd 半开/黑洞时 Get 无限挂起，
// 退避 attempts 不再推进、订阅/配置监听静默停更。「黑洞挂起」无法用内嵌
// etcd 服务端确定性构造（重启空窗/认证空窗都让 Get 立即失败而非挂起），
// 手法对齐 distlock/session_grant_test.go 的 #111 桩件：不走真实 etcd，
// 直接以桩 resnap 驱动 runWatchLoop——首轮 resnap 记录收到的 ctx 并挂起
// 直到该 ctx Done（黑洞只跟随调用方 ctx 返回）；修复后该 ctx 带
// probe.Window 的 deadline，到期以 DeadlineExceeded 失败返回 ⇒ 走既有
// attempts++ 退避路径，第二轮 resnap 成功 ⇒ 重建补投递照常发生（行为闭环
// 不变）。修复前 resnap 收到的是 h.ctx 原样（无 deadline，挂到测试超时）。
func TestRunWatchLoopResnapBoundedByProbeWindow(t *testing.T) {
	h := newWatchHandle()

	// startWatch 每轮返回一条「已关闭」通道：循环立即走「通道关闭，重建」
	// 路径进入 resnap（事件投递不是本用例的被测面，onEvent 恒空转）。
	startWatch := func(context.Context, int64) clientv3.WatchChan {
		ch := make(chan clientv3.WatchResponse)
		close(ch)
		return ch
	}

	var resnapCalls atomic.Int32
	var delivered atomic.Bool
	var deadlineSeen atomic.Bool
	var deadlineWithinWindow atomic.Bool
	var firedByWindowNotParent atomic.Bool
	resnap := func(ctx context.Context) (int64, func(), error) {
		if resnapCalls.Add(1) == 1 {
			dl, ok := ctx.Deadline()
			deadlineSeen.Store(ok)
			// deadline 必须存在且落在探活窗口内（+调度余量）
			rem := time.Until(dl)
			deadlineWithinWindow.Store(rem > 0 && rem <= probe.Window+200*time.Millisecond)
			<-ctx.Done() // 模拟黑洞 Get：既不成功也不失败，只跟随调用方 ctx 返回
			// 唤醒时父 ctx 仍须存活 ⇒ 到期来自探活窗口，而非 h.ctx 被取消
			firedByWindowNotParent.Store(h.ctx.Err() == nil && ctx.Err() == context.DeadlineExceeded)
			return 0, nil, ctx.Err()
		}
		// 第二轮：成功 ⇒ 重建补投递照常发生（窗口失败走退避后的行为闭环）
		return 7, func() { delivered.Store(true) }, nil
	}

	done := make(chan struct{})
	async.GoSafe(func() {
		defer close(done)
		runWatchLoop(h, "t196", 1, startWatch,
			func(clientv3.WatchResponse) {},
			resnap,
		)
	})
	t.Cleanup(func() {
		h.cancel() // 循环任何挂起点（退避 select / resnap 窗口 / 通道 range）都随父 ctx 退出
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("runWatchLoop 未在 h 取消后退出（goroutine 泄漏）")
		}
	})

	// 时间预算：首轮退避 1s + 探活窗口 3s + 第二轮退避 2s（attempts=1）+ 余量；
	// 修复前 resnap 挂在无 deadline 的 h.ctx 上，delivered 永不置位、此处超时
	waitFor(t, 10*time.Second, func() bool { return delivered.Load() })

	if !deadlineSeen.Load() {
		t.Fatal("resnap 收到的 ctx 无 deadline（#196 未套探活窗口，黑洞 Get 将无限挂起、退避不推进）")
	}
	if !deadlineWithinWindow.Load() {
		t.Fatalf("resnap ctx 的 deadline 不在探活窗口 %v 内（窗口口径与 distlock 单源失效）", probe.Window)
	}
	if !firedByWindowNotParent.Load() {
		t.Fatal("挂起的 resnap 不是被探活窗口到期唤醒的（父 ctx 提前取消，或未以 DeadlineExceeded 失败）")
	}
}
