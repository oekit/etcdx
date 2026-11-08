package registry

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	regcore "github.com/oekit/registry"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oekit/etcdx/internal/testutil"
)

// forceWatchCancel 确定性地触发一次 watch Canceled(ErrCompacted)：
//
//  1. 推进全局 revision 并物理压缩到当前值（压缩点越过被测 watch 的
//     resume revision）；
//  2. 重启服务端（同一数据目录/端口，空窗 downtime）——客户端重连后
//     clientv3 从其 initReq.rev（最后收到的事件 rev+1；未收到过事件即建
//     watch 的 startRev）自动恢复 watch；该 rev 已被压缩 ⇒ 服务端对恢复的
//     watch 回 Canceled(ErrCompacted)，即被测代码的重建路径输入。
//
// downtime > 0 时重建循环的首轮快照重取落在服务端空窗内而失败（连续失败
// 计数 +1），供 #85 退避测试制造「失败后成功」序列。
//
// etcd 3.7 对 synced 组的 watcher 不做压缩取消（watch 以 cur+1 的未来 rev
// 创建时直接进 synced 组），只有走 unsynced 路径的（重）建 watch 才会被
// 压缩取消——重启强制走恢复路径，因此这一组合是确定性的。
func forceWatchCancel(t *testing.T, s *testutil.Server, marker string, downtime time.Duration) {
	t.Helper()
	cli := s.Client()
	ctx := context.Background()
	for _, v := range []string{"x", "y"} {
		if _, err := cli.Put(ctx, marker, v); err != nil {
			t.Fatalf("Put marker %s: %v", marker, err)
		}
	}
	resp, err := cli.Get(ctx, marker)
	if err != nil {
		t.Fatalf("Get marker: %v", err)
	}
	if _, err := cli.Compact(ctx, resp.Header.Revision, clientv3.WithCompactPhysical()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	s.Restart(downtime)
	// 等客户端重连成功再返回：后续断言的计时起点（#85 的退避判定）不含
	// grpc 重连耗时，且重启窗口内的请求不会悄悄挂起重试
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := cli.Get(ctx, marker); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("client not reconnected to restarted etcd in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestListenConfigRebuildDeliversEmptyStringOnDeletedKey 钉住 #83：配置
// watch 重建时键已被删除必须回调空串，与正常 watch 路径的 Delete 事件语义
// 一致（doc：Put 事件回调新内容，Delete 事件回调空字符串）。旧实现只在
// len(Kvs)>0 时回调——删除事件随被取消的 watch 一起丢失后，重建路径把
// 「键已删除」吞掉，消费方永久持有已删配置且无后续事件可纠正。
func TestListenConfigRebuildDeliversEmptyStringOnDeletedKey(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	cs, err := NewConfigStore(cli, WithPrefix("/t83"))
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	if err := cs.PutConfig(ctx, "app.json", "", "v1"); err != nil {
		t.Fatalf("PutConfig v1: %v", err)
	}
	var mu sync.Mutex
	var got []string
	if err := cs.ListenConfig(ctx, "app.json", "", func(data string) {
		mu.Lock()
		got = append(got, data)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}

	// 正常事件路径先钉住：v2 推送到达
	if err := cs.PutConfig(ctx, "app.json", "", "v2"); err != nil {
		t.Fatalf("PutConfig v2: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) > 0 && got[len(got)-1] == "v2"
	})

	// 触发重建（压缩 + 重启 ⇒ 恢复的 watch 被 ErrCompacted 取消），并在
	// 重建的 1s 起步退避窗口内删除键——删除事件随被取消的 watch 丢失，
	// 只能靠重建路径表达
	forceWatchCancel(t, s, "/t83/marker", 0)
	// 与 ListenConfig 同键：group("") 落到默认分组 DEFAULT_GROUP
	key := configKey("/t83", "DEFAULT_GROUP", "app.json")
	if _, err := cli.Delete(ctx, key); err != nil {
		t.Fatalf("Delete config key: %v", err)
	}

	// 重建后必须收到空串（旧行为：永不回调，此条件超时）
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) > 0 && got[len(got)-1] == ""
	})

	// 重建出的新 watch 仍然存活：重新写入继续推送
	if err := cs.PutConfig(ctx, "app.json", "", "v3"); err != nil {
		t.Fatalf("PutConfig v3: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got[len(got)-1] == "v3"
	})

	_ = cs.Close(ctx)
}

// TestSubscribeReplaceDoesNotRunConcurrentCallbacks 钉住 #84：同一服务重复
// Subscribe 替换旧订阅时，旧回调与新回调绝不错峰并发——契约「不会并发调用
// cb」。旧实现只 cancel 不等旧 goroutine 退出，且新订阅在 Subscribe 调用里
// 同步回调首次快照：旧 goroutine 正停在回调内时两者并发执行。
func TestSubscribeReplaceDoesNotRunConcurrentCallbacks(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	sub, err := NewSubscriber(cli, WithPrefix("/t84"))
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

	// 替换订阅：不得阻塞在旧回调上，更不得此刻并发执行 cb2
	var got2 atomic.Int32
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		observe()
		defer inFlight.Add(-1)
		got2.Add(1)
	}); err != nil {
		t.Fatalf("Subscribe cb2: %v", err)
	}

	// 释放旧回调：旧 goroutine 退出后，新订阅的首次快照才投递到 cb2
	close(block1)
	waitFor(t, 5*time.Second, func() bool { return got2.Load() > 0 })

	if concurrent.Load() {
		t.Fatal("新旧订阅的回调并发执行了（违反不会并发调用 cb 契约）")
	}

	// 替换后的订阅仍然存活：实例变更继续推送
	k := instanceKey("/t84", "DEFAULT_GROUP", "order", "10.0.0.1:9000")
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

// TestListenConfigReplaceDoesNotRunConcurrentCallbacks 钉住 #84（ConfigStore
// 侧）：同一键重复 ListenConfig 同样必须等旧 goroutine 退出后才开始投递。
func TestListenConfigReplaceDoesNotRunConcurrentCallbacks(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	cs, err := NewConfigStore(cli, WithPrefix("/t84c"))
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	if err := cs.PutConfig(ctx, "app.json", "", "v1"); err != nil {
		t.Fatalf("PutConfig v1: %v", err)
	}

	var inFlight atomic.Int32
	var concurrent atomic.Bool

	// onChange1 在首个事件回调里阻塞，制造「旧 goroutine 正执行回调」
	block1 := make(chan struct{})
	var enteredOnce sync.Once
	entered1 := make(chan struct{})
	if err := cs.ListenConfig(ctx, "app.json", "", func(data string) {
		if inFlight.Add(1) != 1 {
			concurrent.Store(true)
		}
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

	// 替换监听：cb1 仍阻塞，cb2 不得此刻并发执行
	var got2 atomic.Int32
	if err := cs.ListenConfig(ctx, "app.json", "", func(data string) {
		if inFlight.Add(1) != 1 {
			concurrent.Store(true)
		}
		defer inFlight.Add(-1)
		got2.Add(1)
	}); err != nil {
		t.Fatalf("ListenConfig onChange2: %v", err)
	}

	// cb1 仍阻塞期间写入 v3：新监听的首次投递（v3 事件）不得与 cb1 并发。
	// 留 200ms 让事件送达——修复实现此刻还在等旧 goroutine 退出，不会投递；
	// 只 cancel 不交接的旧实现会在此窗口内并发执行 cb2。
	if err := cs.PutConfig(ctx, "app.json", "", "v3"); err != nil {
		t.Fatalf("PutConfig v3: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// 释放旧回调：旧 goroutine 退出（过期投递被丢弃），新监听接管后续投递
	close(block1)
	waitFor(t, 5*time.Second, func() bool { return got2.Load() > 0 })

	if concurrent.Load() {
		t.Fatal("新旧监听的回调并发执行了（违反不会并发调用 onChange 契约）")
	}
	_ = cs.Close(ctx)
}

// authOutage 开启 etcd 认证，使无凭据客户端的一切请求**立即失败**
// （InvalidArgument，不进 gRPC 重试退避），返回恢复函数（以 root 凭据关认证）。
// 用于让被测代码重建路径的快照重取确定性失败——重启空窗做不到：watch 取消
// 只在客户端重连（服务端已恢复）后才送达，其后的重取必然成功。
func authOutage(t *testing.T, cli *clientv3.Client) (restore func()) {
	t.Helper()
	ctx := context.Background()
	const user, pass = "root", "sweep3-pw"
	if _, err := cli.UserAdd(ctx, user, pass); err != nil {
		t.Fatalf("UserAdd: %v", err)
	}
	if _, err := cli.UserGrantRole(ctx, user, user); err != nil {
		t.Fatalf("UserGrantRole: %v", err)
	}
	rootCli, err := clientv3.New(clientv3.Config{
		Endpoints:   cli.Endpoints(),
		Username:    user,
		Password:    pass,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("root client: %v", err)
	}
	if _, err := cli.AuthEnable(ctx); err != nil {
		t.Fatalf("AuthEnable: %v", err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			defer func() { _ = rootCli.Close() }()
			rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer rcancel()
			if _, err := rootCli.AuthDisable(rctx); err != nil {
				t.Errorf("AuthDisable: %v", err)
			}
		})
	}
}

// TestSubscribeBackoffResetsAfterSuccessfulRebuild 钉住 #85：退避由连续重建
// 失败次数派生、成功重建即归零。旧实现 backoff 声明在循环外只增不减：先经
// 一轮「两次重取失败后成功」（认证空窗使重建重取连续失败，连续失败计数 2），
// 下一轮重建的退避若未归零就是 ≥4s（旧实现累计到 8s）；修复后从 1s 起步。
// 以该轮投递延迟判定：>2.5s 即退避未归零。
func TestSubscribeBackoffResetsAfterSuccessfulRebuild(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	sub, err := NewSubscriber(cli, WithPrefix("/t85"))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	k := instanceKey("/t85", "DEFAULT_GROUP", "order", "10.0.0.1:9000")
	v, err := encodeInstance(regcore.Instance{Service: "order", IP: "10.0.0.1", Port: 9000})
	if err != nil {
		t.Fatalf("encodeInstance: %v", err)
	}
	if _, err := cli.Put(ctx, k, string(v)); err != nil {
		t.Fatalf("Put instance: %v", err)
	}

	var deliveries atomic.Int32
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		deliveries.Add(1)
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return deliveries.Load() >= 1 }) // 初始快照

	// 第二轮基线：一次干净重建（投递 #2）
	forceWatchCancel(t, s, "/t85/marker", 0)
	waitFor(t, 10*time.Second, func() bool { return deliveries.Load() >= 2 })

	// 失败轮：触发重建后立刻开启认证空窗——重建的首两次快照重取失败
	//（退避 1s、2s），3.6s 后关认证，第三次重取（退避 4s 到点）成功 ⇒
	// 重建成功、连续失败计数必须归零
	forceWatchCancel(t, s, "/t85/marker", 0)
	restore := authOutage(t, cli)
	time.Sleep(3600 * time.Millisecond) // 覆盖前两次重取（≈ +1.3s / +3.3s）
	restore()
	waitFor(t, 20*time.Second, func() bool { return deliveries.Load() >= 3 })

	// 判定轮：计数已归零 ⇒ 退避从 1s 起步（≈1.2s 交付）；未归零（或旧实现
	// 的只增不减）则本轮退避 ≥4s，必然超过 2.5s 判定线
	forceWatchCancel(t, s, "/t85/marker", 0)
	deadline := time.Now().Add(2500 * time.Millisecond)
	for deliveries.Load() < 4 {
		if time.Now().After(deadline) {
			t.Fatalf("失败轮之后的重建未在 %v 内恢复投递：退避未随成功重建归零（#85）",
				time.Since(deadline)+2500*time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 重建出的新 watch 仍然存活：实例变更继续推送
	if _, err := cli.Put(ctx, k, string(v)); err != nil {
		t.Fatalf("Put instance again: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { return deliveries.Load() >= 5 })

	_ = sub.Close(ctx)
}

// TestListenConfigBackoffResetsAfterSuccessfulRebuild 钉住 #85（ConfigStore
// 侧）：与订阅侧同口径——「两次重取失败后成功」的下一轮重建仍从 1s 起步。
func TestListenConfigBackoffResetsAfterSuccessfulRebuild(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	cs, err := NewConfigStore(cli, WithPrefix("/t85c"))
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	if err := cs.PutConfig(ctx, "app.json", "", "v1"); err != nil {
		t.Fatalf("PutConfig v1: %v", err)
	}

	var deliveries atomic.Int32
	if err := cs.ListenConfig(ctx, "app.json", "", func(data string) {
		deliveries.Add(1)
	}); err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}

	// 第二轮基线：一次干净重建（投递 #1）
	forceWatchCancel(t, s, "/t85c/marker", 0)
	waitFor(t, 10*time.Second, func() bool { return deliveries.Load() >= 1 })

	// 失败轮：重建后立刻开认证空窗，前两次快照重取失败，3.6s 后恢复，
	// 第三次重取成功 ⇒ 重建成功、计数必须归零
	forceWatchCancel(t, s, "/t85c/marker", 0)
	restore := authOutage(t, cli)
	time.Sleep(3600 * time.Millisecond)
	restore()
	waitFor(t, 20*time.Second, func() bool { return deliveries.Load() >= 2 })

	// 判定轮：计数已归零 ⇒ 退避 1s 起步；未归零则 ≥4s，超过 2.5s 判定线
	forceWatchCancel(t, s, "/t85c/marker", 0)
	deadline := time.Now().Add(2500 * time.Millisecond)
	for deliveries.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("失败轮之后的重建未在 %v 内恢复投递：退避未随成功重建归零（#85）",
				time.Since(deadline)+2500*time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 重建出的新 watch 仍然存活
	if err := cs.PutConfig(ctx, "app.json", "", "v2"); err != nil {
		t.Fatalf("PutConfig v2: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { return deliveries.Load() >= 4 })

	_ = cs.Close(ctx)
}
