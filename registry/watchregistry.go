package registry

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/oekit/concurrency/async"
)

// watchRegistry 是 Subscriber 与 ConfigStore 共用的 per-key 句柄登记表
// （#185 单源）：活跃句柄表、retired 待退出槽位（#149）与「登记/替换/
// supersede 交接/摘除/关闭」只在此实现一份——此前这些生命周期逻辑在订阅、
// 配置两侧各复制一份（~180 行同构），#84/#149 级别的修复每条都要改两处。
// 差异点（快照构造与事件闭包）由 Subscribe/ListenConfig 注入（观察循环骨架
// 另见 runWatchLoop，#122）。
//
// retired 槽位语义（#149）：承载已 remove/closeAll 摘除、但观察 goroutine
// 尚未退出的句柄。#84 的交接（新句柄等旧 goroutine 退出后才投递）不能依赖
// 「旧句柄还在活跃表里」——先删表再 supersede 的旧实现让退订后重建的订阅/
// 监听读到 old==nil，交接整段被绕过，新旧回调并发。同一 key 至多存在于活跃
// 表/retired 之一（install/remove/closeAll 的转移原子完成）；槽位由观察
// goroutine 退出时按身份自清（dropRetired），无重建时也不残留。
type watchRegistry[K comparable] struct {
	mu      sync.Mutex
	active  map[K]*watchHandle // key → 活跃句柄
	retired map[K]*watchHandle // key → 已摘除、观察 goroutine 退出中的句柄（#149）
}

func newWatchRegistry[K comparable]() *watchRegistry[K] {
	return &watchRegistry[K]{
		active:  make(map[K]*watchHandle),
		retired: make(map[K]*watchHandle),
	}
}

// install 登记 h（替换语义）：同 key 已有活跃句柄、或该 key 仍占着 retired
// 待退出槽位（#149——退订/关停后的重建），一律 supersede 完成交接，返回被
// 取代的旧句柄（无则 nil）——新观察 goroutine 必须等 old.done 之后才允许
// 投递（#84：仅 cancel 不交接）。supersede 在锁内执行：查旧、取代、换位是
// 原子转移，同一 key 至多存在于活跃表/retired 之一。
func (r *watchRegistry[K]) install(key K, h *watchHandle) *watchHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.active[key]
	if old == nil {
		// 退订/关停后的重建（#149）：旧句柄已从活跃表摘除但可能仍在退出中，
		// 交接等待改读 retired 槽位——「先删表」不得让 #84 的等旧 goroutine
		// 退出整段失效
		old = r.retired[key]
		delete(r.retired, key)
	}
	if old != nil {
		old.supersede() // 标记取代 + cancel：旧 goroutine 不再投递（仅 cancel 不交接，#84）
	}
	r.active[key] = h
	return old
}

// remove 摘除 key 的活跃句柄并转入 retired 待退出槽位（#149）：其观察
// goroutine 可能仍在执行消费方回调，后续同 key install 靠该槽位拿到
// old.done 完成 #84 交接——先删表再 supersede 会让交接整段失效（摘除后
// 重建与旧回调并发）。未登记为 no-op（返回 nil）。本方法不等待旧回调返回
// （不阻塞摘除方）；supersede 在锁外执行（cancel 可能唤醒观察 goroutine，
// 不持登记锁），槽位由观察 goroutine 退出时自清，不会随摘除次数累积。
func (r *watchRegistry[K]) remove(key K) *watchHandle {
	r.mu.Lock()
	h := r.active[key]
	if h != nil {
		delete(r.active, key)
		r.retired[key] = h // #149：摘除后仍可被重建订阅读到（同名至多一个）
	}
	r.mu.Unlock()
	if h != nil {
		h.supersede()
	}
	return h
}

// closeAll 摘除全部活跃句柄并整体转入 retired 槽位（#149，与 remove 同
// 口径）：Close 之后立即重建仍须等旧 goroutine 退出，回调不并发。幂等：
// 活跃表已空时为 no-op（retired 槽位仍由观察 goroutine 退出时自清）。
// 各句柄 supersede 在锁外逐个执行，与 remove 同一锁口径。
func (r *watchRegistry[K]) closeAll() {
	r.mu.Lock()
	stale := r.active
	r.active = make(map[K]*watchHandle)
	for key, h := range stale {
		r.retired[key] = h // 活跃表/retired 同名不相交（install/remove 保证）
	}
	r.mu.Unlock()
	for _, h := range stale {
		h.supersede()
	}
}

// dropRetired 观察 goroutine 退出时把自身从 retired 槽位摘除（身份校验：
// 非本句柄则不动——key 可能已被后续订阅/退订重新占用）。
func (r *watchRegistry[K]) dropRetired(key K, h *watchHandle) {
	r.mu.Lock()
	if r.retired[key] == h {
		delete(r.retired, key)
	}
	r.mu.Unlock()
}

// run 启动 h 的观察 goroutine，交接块单源（#84/#149，此前订阅/配置两侧
// 逐字同构）：先等被替换的旧 goroutine 退出（回调不可能并发）——cancel 只
// 会打断阻塞中的 Watch/Get，不会等待正在执行的回调返回，等 old.done 才建立
// 「旧 goroutine 不会再回调」的事实；等待期间本句柄又被取代/退订则不投递
// 直接退出；通过交接后仍复查 superseded，被取代/摘除后的过期投递一律丢弃。
// goroutine 任何出口都经 defer 先 close(done) 交接、再从 retired 槽位自清
// （#149：先 close 再摘除，保持「槽位还在 ⇒ goroutine 可能仍在回调」的
// 不变式）。body 为交接完成后的投递与观察循环本体——快照构造与事件闭包由
// Subscribe/ListenConfig 注入，循环骨架见 runWatchLoop（#122）。
func (r *watchRegistry[K]) run(key K, h *watchHandle, old *watchHandle, body func()) {
	async.GoSafe(func() {
		defer func() {
			h.close()
			// #149：观察 goroutine 退出后从 retired 槽位摘除自己（身份校验）
			r.dropRetired(key, h)
		}()
		if old != nil {
			select {
			case <-old.done: // 旧 goroutine 已退出：此后才允许本句柄投递
			case <-h.ctx.Done():
				return // 等待期间本句柄又被取代/退订：不投递
			}
		}
		if h.superseded.Load() {
			return
		}
		body()
	})
}

// watchHandle 记录一个活跃 watch 订阅（Subscriber 与 ConfigStore 共用）：
// cancel 终止 watch；done 在观察 goroutine 退出时关闭——替换方以此建立
// 「旧 goroutine 不会再回调」的事实（#84：cancel 只会打断阻塞中的 Watch/
// Get，不会等待正在执行的回调返回）；superseded 标记本订阅已被同 key 的新
// 订阅或停止操作取代，被取代的 goroutine 丢弃一切未投递的快照直接退出。
type watchHandle struct {
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	superseded atomic.Bool
}

func newWatchHandle() *watchHandle {
	ctx, cancel := context.WithCancel(context.Background())
	return &watchHandle{ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// supersede 标记本订阅被取代：丢弃后续投递并终止观察 goroutine。
func (h *watchHandle) supersede() {
	h.superseded.Store(true)
	h.cancel()
}

// close 由观察 goroutine defer 调用：退出即关闭 done，交接给新订阅。
func (h *watchHandle) close() {
	close(h.done)
}
