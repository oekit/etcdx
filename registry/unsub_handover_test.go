package registry

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	regcore "github.com/oekit/registry"

	"github.com/oekit/etcdx/internal/testutil"
)

// #149 回归：Unsubscribe 摘除句柄后重建订阅必须完成 #84 交接（等旧 goroutine
// 退出），不得因「先删 map 再 supersede」让新订阅读到 old==nil 而整段绕过——
// 缺陷形态：cb1 阻塞在回调内 → 退订 → 重订 → cb1/cb2 并发执行。
// 与 TestSubscribeReplaceDoesNotRunConcurrentCallbacks（#84 替换路径）同构，
// 钉住的是「退订后重建」这条交接失效路径。
func TestUnsubscribeThenSubscribeDoesNotRunConcurrentCallbacks(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	sub, err := NewSubscriber(cli, WithPrefix("/t149"))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	var inFlight atomic.Int32
	var concurrent atomic.Bool
	observe := func() {
		if inFlight.Add(1) != 1 {
			concurrent.Store(true)
		}
	}

	// cb1 首次投递（初始快照）后阻塞在里面，模拟「旧 goroutine 正执行回调」
	block1 := make(chan struct{})
	var enteredOnce sync.Once
	entered1 := make(chan struct{})
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		observe()
		defer inFlight.Add(-1)
		enteredOnce.Do(func() { close(entered1) })
		<-block1
	}); err != nil {
		t.Fatalf("Subscribe cb1: %v", err)
	}
	<-entered1

	// 退订：立即返回，cb1 仍在跑（旧实现把句柄从 map 直接摘掉）
	if err := sub.Unsubscribe(ctx, "order"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	// 重建订阅：修复实现从 retired 槽位拿到旧句柄并等它退出；缺陷实现
	// old==nil，立即投递首次快照
	var got2 atomic.Int32
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		observe()
		defer inFlight.Add(-1)
		got2.Add(1)
	}); err != nil {
		t.Fatalf("Subscribe cb2: %v", err)
	}

	// cb1 仍阻塞：交接等待期间 cb2 不得投递
	time.Sleep(200 * time.Millisecond)
	if got2.Load() != 0 {
		t.Fatal("退订后重建订阅在旧回调仍阻塞时就投递了（#149：交接被先删 map 绕过）")
	}
	if concurrent.Load() {
		t.Fatal("退订→重订路径新旧回调并发（违反不会并发调用 cb 契约）")
	}

	// 释放旧回调：旧 goroutine 退出后，新订阅的首次快照才投递到 cb2
	close(block1)
	waitFor(t, 5*time.Second, func() bool { return got2.Load() > 0 })
	if concurrent.Load() {
		t.Fatal("新旧订阅的回调并发执行了（违反不会并发调用 cb 契约）")
	}

	// 交接后的订阅仍然存活：实例变更继续推送
	k := instanceKey("/t149", "DEFAULT_GROUP", "order", "10.0.0.1:9000")
	v, err := encodeInstance(regcore.Instance{Service: "order", IP: "10.0.0.1", Port: 9000})
	if err != nil {
		t.Fatalf("encodeInstance: %v", err)
	}
	if _, err := cli.Put(ctx, k, string(v)); err != nil {
		t.Fatalf("Put instance: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return got2.Load() > 1 })

	_ = sub.Close(ctx)
}

// #149 回归（ConfigStore 侧）：StopListenConfig 摘除句柄后重建监听必须完成
// #84 交接——与 TestListenConfigReplaceDoesNotRunConcurrentCallbacks 同构，
// 钉住「停听后重建」这条交接失效路径。
func TestStopListenConfigThenListenConfigDoesNotRunConcurrentCallbacks(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	cs, err := NewConfigStore(cli, WithPrefix("/t149c"))
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	if err := cs.PutConfig(ctx, "app.json", "", "v1"); err != nil {
		t.Fatalf("PutConfig v1: %v", err)
	}

	var inFlight atomic.Int32
	var concurrent atomic.Bool
	observe := func() {
		if inFlight.Add(1) != 1 {
			concurrent.Store(true)
		}
	}

	// onChange1 在首个事件回调里阻塞，制造「旧 goroutine 正执行回调」
	block1 := make(chan struct{})
	var enteredOnce sync.Once
	entered1 := make(chan struct{})
	if err := cs.ListenConfig(ctx, "app.json", "", func(data string) {
		observe()
		defer inFlight.Add(-1)
		enteredOnce.Do(func() { close(entered1) })
		<-block1
	}); err != nil {
		t.Fatalf("ListenConfig onChange1: %v", err)
	}
	// ListenConfig 不回放当前值：写一次触发 cb1 进入
	if err := cs.PutConfig(ctx, "app.json", "", "v2"); err != nil {
		t.Fatalf("PutConfig v2: %v", err)
	}
	<-entered1

	// 停听：立即返回，onChange1 仍在跑
	if err := cs.StopListenConfig(ctx, "app.json", ""); err != nil {
		t.Fatalf("StopListenConfig: %v", err)
	}

	// 重建监听：修复实现从 retired 槽位拿到旧句柄并等它退出
	var got2 atomic.Int32
	if err := cs.ListenConfig(ctx, "app.json", "", func(data string) {
		observe()
		defer inFlight.Add(-1)
		got2.Add(1)
	}); err != nil {
		t.Fatalf("ListenConfig onChange2: %v", err)
	}

	// onChange1 仍阻塞期间写入 v3：新监听的 watch 在交接完成前不启动，
	// 不得此刻投递（缺陷实现 old==nil，立即起 watch 并投递 v3）
	if err := cs.PutConfig(ctx, "app.json", "", "v3"); err != nil {
		t.Fatalf("PutConfig v3: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got2.Load() != 0 {
		t.Fatal("停听后重建监听在旧回调仍阻塞时就投递了（#149：交接被先删 map 绕过）")
	}
	if concurrent.Load() {
		t.Fatal("停听→重建路径新旧回调并发（违反不会并发调用 onChange 契约）")
	}

	// 释放旧回调：旧 goroutine 退出后新监听接管，v3（落在交接窗口内）补投递
	close(block1)
	waitFor(t, 5*time.Second, func() bool { return got2.Load() > 0 })
	if concurrent.Load() {
		t.Fatal("新旧监听的回调并发执行了（违反不会并发调用 onChange 契约）")
	}

	// 交接后的监听仍然存活：后续变更继续推送
	if err := cs.PutConfig(ctx, "app.json", "", "v4"); err != nil {
		t.Fatalf("PutConfig v4: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return got2.Load() > 1 })

	_ = cs.Close(ctx)
}

// #149 回归：Close 摘除全部句柄后重建订阅同样必须完成 #84 交接（issue 点名
// Close 与 Unsubscribe/StopListenConfig 同构）。
func TestCloseThenSubscribeDoesNotRunConcurrentCallbacks(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	sub, err := NewSubscriber(cli, WithPrefix("/t149k"))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	var inFlight atomic.Int32
	var concurrent atomic.Bool
	observe := func() {
		if inFlight.Add(1) != 1 {
			concurrent.Store(true)
		}
	}

	block1 := make(chan struct{})
	var enteredOnce sync.Once
	entered1 := make(chan struct{})
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		observe()
		defer inFlight.Add(-1)
		enteredOnce.Do(func() { close(entered1) })
		<-block1
	}); err != nil {
		t.Fatalf("Subscribe cb1: %v", err)
	}
	<-entered1

	if err := sub.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got2 atomic.Int32
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		observe()
		defer inFlight.Add(-1)
		got2.Add(1)
	}); err != nil {
		t.Fatalf("Subscribe cb2: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	if got2.Load() != 0 {
		t.Fatal("Close 后重建订阅在旧回调仍阻塞时就投递了（#149：交接被换 map 绕过）")
	}
	if concurrent.Load() {
		t.Fatal("Close→重订路径新旧回调并发（违反不会并发调用 cb 契约）")
	}

	close(block1)
	waitFor(t, 5*time.Second, func() bool { return got2.Load() > 0 })
	if concurrent.Load() {
		t.Fatal("新旧订阅的回调并发执行了（违反不会并发调用 cb 契约）")
	}
}
