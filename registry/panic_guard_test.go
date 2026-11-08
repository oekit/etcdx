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

// #87 回归钉：消费者回调 panic 不得终结观察循环。旧行为：cb/onChange 一次
// panic ⇒ async.GoSafe 的兜底 recover 之后观察 goroutine 整体退出——#55/#64
// 修出的重建循环、退避、快照回调连同 goroutine 一起消失，订阅静默死亡
// （实例视图永久冻结、配置推送停更）而 Unsubscribe/StopListenConfig 仍返回
// nil。修复后：safeNotify 就地 recover 转 Error 日志，本次投递跳过，后续
// 变更照常投递。三个用例分别覆盖事件投递点、首次快照投递点与 ConfigStore
// 事件投递点（重建快照投递点与事件投递点共用同一 helper，由 compact 测试
// 覆盖到达路径）。

func newPanicTestClient(t *testing.T) *Subscriber {
	t.Helper()
	cli := testutil.NewClient(t)
	sub, err := NewSubscriber(cli, WithPrefix("/t"))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	return sub
}

func putOrderInstance(t *testing.T, sub *Subscriber, ip string) {
	t.Helper()
	k := instanceKey("/t", "DEFAULT_GROUP", "order", ip+":9000")
	v, err := encodeInstance(regcore.Instance{Service: "order", IP: ip, Port: 9000})
	if err != nil {
		t.Fatalf("encodeInstance: %v", err)
	}
	if _, err := sub.cli.Put(context.Background(), k, string(v)); err != nil {
		t.Fatalf("Put %s: %v", k, err)
	}
}

func hasIPIn(insts []regcore.Instance, ip string) bool {
	for _, in := range insts {
		if in.IP == ip {
			return true
		}
	}
	return false
}

// TestSubscribeCallbackPanicKeepsLoopAlive 事件投递点 panic 一次后，观察
// 循环必须存活：后续 Put 的新实例仍能推送到回调。旧行为下第二个实例的
// panic 让 goroutine 死亡，第三个实例的回调永不出现。
func TestSubscribeCallbackPanicKeepsLoopAlive(t *testing.T) {
	sub := newPanicTestClient(t)
	ctx := context.Background()

	putOrderInstance(t, sub, "10.0.0.1")
	var mu sync.Mutex
	var last []regcore.Instance
	var panicked atomic.Bool
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		if !panicked.Load() { // 首个含中毒实例的投递：panic 一次，本次投递作废
			if hasIPIn(insts, "10.0.0.2") {
				panicked.Store(true)
				panic("consumer callback boom (#87 regression)")
			}
		}
		mu.Lock()
		last = insts
		mu.Unlock()
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	hasIP := func(ip string) func() bool {
		return func() bool {
			mu.Lock()
			defer mu.Unlock()
			return hasIPIn(last, ip)
		}
	}
	waitFor(t, 3*time.Second, hasIP("10.0.0.1"))

	// 中毒实例：事件驱动的投递里回调 panic（safeNotify 收编，本次投递跳过）
	putOrderInstance(t, sub, "10.0.0.2")
	waitFor(t, 3*time.Second, func() bool { return panicked.Load() })

	// 旧行为：观察 goroutine 已死，此 Put 的回调永不出现；修复后循环继续
	putOrderInstance(t, sub, "10.0.0.3")
	waitFor(t, 10*time.Second, hasIP("10.0.0.3"))

	if err := sub.Unsubscribe(ctx, "order"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	_ = sub.Close(ctx)
}

// TestSubscribeInitialSnapshotPanicKeepsLoopAlive 首次快照投递点 panic 后，
// 观察循环同样必须存活：后续事件照常投递（快照点与事件点同一条串行路径，
// 一次 panic 不该让订阅从建立起就死透）。
func TestSubscribeInitialSnapshotPanicKeepsLoopAlive(t *testing.T) {
	sub := newPanicTestClient(t)
	ctx := context.Background()

	putOrderInstance(t, sub, "10.0.0.1")
	var mu sync.Mutex
	var last []regcore.Instance
	var panicked atomic.Bool
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		if !panicked.Load() { // 首次快照即命中：在快照投递点 panic 一次
			if hasIPIn(insts, "10.0.0.1") {
				panicked.Store(true)
				panic("consumer snapshot callback boom (#87 regression)")
			}
		}
		mu.Lock()
		last = insts
		mu.Unlock()
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return panicked.Load() })

	// 快照点 panic 后循环仍活着：新实例的事件投递必须到达
	putOrderInstance(t, sub, "10.0.0.2")
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return hasIPIn(last, "10.0.0.2")
	})

	_ = sub.Close(ctx)
}

// TestListenConfigCallbackPanicKeepsLoopAlive ConfigStore 事件投递点 panic
// 一次后，监听循环必须存活：后续 PutConfig 的新内容仍能推送。
func TestListenConfigCallbackPanicKeepsLoopAlive(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	cs, err := NewConfigStore(cli, WithPrefix("/t"))
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	var mu sync.Mutex
	var got string
	var panicked atomic.Bool
	if err := cs.ListenConfig(ctx, "app.json", "", func(data string) {
		if !panicked.Load() && data == "v2" { // 中毒内容：panic 一次，本次投递作废
			panicked.Store(true)
			panic("consumer onChange boom (#87 regression)")
		}
		mu.Lock()
		got = data
		mu.Unlock()
	}); err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}
	lastGot := func() string {
		mu.Lock()
		defer mu.Unlock()
		return got
	}

	// ListenConfig 不回放当前值：先 warm-up 一次写入，确认 watch 建立且事件
	// 通路可达，再投毒——否则投毒写入可能落在 watch 建立前，panic 无从发生
	if err := cs.PutConfig(ctx, "app.json", "", "warm"); err != nil {
		t.Fatalf("PutConfig warm: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return lastGot() == "warm" })

	// v2 的投递里 onChange panic（safeNotify 收编，投递跳过）
	if err := cs.PutConfig(ctx, "app.json", "", "v2"); err != nil {
		t.Fatalf("PutConfig v2: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool { return panicked.Load() })

	// 旧行为：监听 goroutine 已死，v3 永不推送；修复后循环继续
	if err := cs.PutConfig(ctx, "app.json", "", "v3"); err != nil {
		t.Fatalf("PutConfig v3: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool { return lastGot() == "v3" })

	_ = cs.Close(ctx)
}
