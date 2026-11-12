package registry

import (
	"context"
	"time"

	"github.com/oekit/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// runWatchLoop 是 Subscriber.Subscribe 与 ConfigStore.ListenConfig 共用的
// watch 观察循环骨架（#122 单源）：#55 的「Canceled/通道关闭 → 指数退避
// 重建」、#85 的「退避由连续重建失败次数派生、成功即归零」、#84 的「投递
// 前检查 superseded，被取代后的过期投递一律丢弃」全部只在这里实现一份——
// 此前循环本体在订阅/配置两侧各复制一份，#55/#84/#85 级别的修复每条都要
// 改两处。
//
// 差异点由参数注入：
//   - startWatch：构造 watch 通道（订阅=服务前缀、配置=单键，各自选项）；
//   - onEvent：一个 WatchResponse 到达时的投递形态（订阅=按事件批全量
//     重拉实例、配置=逐事件回调新值/删除空串）；onEvent 返回后若 h.ctx
//     已取消则整体退出（取数/回调窗口内被退订不再重建）；
//   - resnap：重建退避后的快照重取——返回新 revision 与「重建成功补投递」
//     闭包（订阅=全量快照回调、配置=当前值/删除空串回调）；失败返回
//     err，循环按 attempts++ 继续退避重试。
//
// target 仅用于日志定位（服务名 / 配置 dataID），统一记 "target" 字段。
func runWatchLoop(
	h *watchHandle,
	target string,
	rev int64,
	startWatch func(ctx context.Context, rev int64) clientv3.WatchChan,
	onEvent func(wresp clientv3.WatchResponse),
	resnap func(ctx context.Context) (nextRev int64, deliver func(), err error),
) {
	attempts := 0
	for h.ctx.Err() == nil {
		wch := startWatch(h.ctx, rev)
		canceled := false
		for wresp := range wch {
			if wresp.Canceled {
				// Compacted 时 WithRev(rev) 会被服务端取消——clientv3 不
				// 自动重建，必须自己重建（#55）
				log.Warn("watch canceled, rebuilding with exponential backoff",
					"target", target, logx.ErrKey, wresp.Err())
				canceled = true
				break
			}
			if h.superseded.Load() {
				return // 已被新订阅/退订取代：过期投递一律丢弃（#84）
			}
			onEvent(wresp)
			if h.ctx.Err() != nil {
				return // 事件处理窗口内被退订/取代：正常退出，不重建
			}
		}
		if h.ctx.Err() != nil {
			return // Unsubscribe/StopListenConfig：正常退出，不重建
		}
		if !canceled {
			// 通道关闭且未见 Canceled（client 断连等）：同样走重建
			log.Warn("watch channel closed, rebuilding", "target", target)
		}
		// 退避等待（可被取消打断），重取快照拿新 revision，避免重建即 Compacted
		select {
		case <-h.ctx.Done():
			return
		case <-time.After(watchBackoff(attempts)):
		}
		nextRev, deliver, err := resnap(h.ctx)
		if err != nil {
			attempts++ // 每轮快照重取失败 attempts++（#85）
			log.Warn("snapshot refresh for watch rebuild failed, retrying",
				"target", target, logx.ErrKey, err, "attempts", attempts)
			continue // 快照失败：下轮再试（rev 不变，可能仍 Compacted，会再退避）
		}
		rev = nextRev
		attempts = 0 // 重建成功：退避归零（#85），单次瞬时中断不吃历史累计窗口
		if h.superseded.Load() {
			return
		}
		deliver() // 重建成功先补投递一次当前视图（形态由 resnap 闭包决定）
	}
}
