package registry

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	regcore "github.com/oekit/registry"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oekit/etcdx/internal/probe"
)

// #202 回归：事件批取数（onEvent → fetchInstances 的 Get）必须被探活窗口
// 罩住——h.ctx 由 WithCancel(Background) 派生、无 deadline，etcd 半开/黑洞
// 时取数 Get 无限挂起，onEvent 卡死不返回，后续事件全部积压，订阅静默停更
// （与 #196 resnap 修复同型）。手法对齐 resnap_probe_window_test.go 的
// #196 桩件与 distlock/session_grant_test.go 的 #111 桩件：不走真实 etcd，
// 桩 KV 的快照 Get 恒成功（订阅建立时还活着），事件批触发的取数 Get 黑洞
// ——既不成功也不失败，只跟随调用方 ctx 返回；修复前 onEvent 收到的是
// h.ctx 原样（无 deadline，挂到测试超时），修复后该 ctx 带 probe.Window
// 的 deadline，到期以 DeadlineExceeded 失败 ⇒ 走既有 Warn 丢弃分支。

// snapshotOKEventBlackHoleKV 首次 Get（Subscribe 建立订阅的快照拉取）恒
// 成功返回空响应；其后的事件批取数 Get 为黑洞。
type snapshotOKEventBlackHoleKV struct {
	clientv3.KV
	gets atomic.Int32

	deadlineSeen          atomic.Bool
	deadlineWithinWindow  atomic.Bool
	firedByWindowNotQuery atomic.Bool
	done                  chan struct{}
}

func (k *snapshotOKEventBlackHoleKV) Get(ctx context.Context, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if k.gets.Add(1) == 1 {
		// 订阅建立时的快照 Get：etcd 还活着，返回空快照（revision 1）
		return &clientv3.GetResponse{Header: &pb.ResponseHeader{Revision: 1}}, nil
	}
	// 事件批触发的取数 Get：黑洞——只跟随调用方 ctx 返回
	dl, ok := ctx.Deadline()
	k.deadlineSeen.Store(ok)
	// deadline 必须存在且落在探活窗口内（+调度余量）
	rem := time.Until(dl)
	k.deadlineWithinWindow.Store(rem > 0 && rem <= probe.Window+200*time.Millisecond)
	<-ctx.Done()
	// 唤醒时以 DeadlineExceeded 结束 ⇒ 到期来自探活窗口，而非任何一方 cancel
	k.firedByWindowNotQuery.Store(ctx.Err() == context.DeadlineExceeded)
	close(k.done)
	return nil, ctx.Err()
}

// stubWatcher 忽略真实 watch：返回测试控制的通道；h.ctx 取消时关闭通道，
// 让 runWatchLoop 的 range 正常退出（不泄漏观察 goroutine）。
type stubWatcher struct {
	clientv3.Watcher
	ch chan clientv3.WatchResponse
}

func (w *stubWatcher) Watch(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
	go func() {
		<-ctx.Done()
		close(w.ch)
	}()
	return w.ch
}

func TestOnEventFetchBoundedByProbeWindow(t *testing.T) {
	kv := &snapshotOKEventBlackHoleKV{done: make(chan struct{})}
	wch := make(chan clientv3.WatchResponse, 1)
	cli := clientv3.NewCtxClient(context.Background())
	cli.KV = kv
	cli.Watcher = &stubWatcher{ch: wch}

	sub, err := NewSubscriber(cli)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sub.Subscribe(ctx, "svc", func([]regcore.Instance) {}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() {
		// 观察循环任何挂起点（取数窗口 / 通道 range）都随退订退出
		_ = sub.Unsubscribe(ctx, "svc")
		select {
		case <-kv.done:
		case <-time.After(10 * time.Second):
			t.Error("事件批取数未在探活窗口内返回（#202 未收口，黑洞 Get 将无限挂起）")
		}
	})

	// 投递一个事件批：触发 onEvent → fetchInstances 的取数 Get（黑洞）
	wch <- clientv3.WatchResponse{
		Events: []*clientv3.Event{{Kv: &mvccpb.KeyValue{Key: []byte("k"), Value: []byte("v")}}},
	}

	// 时间预算：探活窗口 3s + 余量；修复前取数 Get 挂在无 deadline 的 h.ctx 上，
	// kv.done 永不关闭、此处超时
	select {
	case <-kv.done:
	case <-time.After(10 * time.Second):
		t.Fatal("事件批取数 10s 未返回：onEvent 取数 Get 仍无界（#202）")
	}

	if !kv.deadlineSeen.Load() {
		t.Fatal("取数 Get 收到的 ctx 无 deadline（#202 未套探活窗口，黑洞 Get 将无限挂起、订阅静默停更）")
	}
	if !kv.deadlineWithinWindow.Load() {
		t.Fatalf("取数 Get 的 deadline 不在探活窗口 %v 内（窗口口径与 distlock/watchloop 单源失效）", probe.Window)
	}
	if !kv.firedByWindowNotQuery.Load() {
		t.Fatal("挂起的取数 Get 不是被探活窗口到期唤醒的（被外部 cancel，或未以 DeadlineExceeded 失败）")
	}
}
