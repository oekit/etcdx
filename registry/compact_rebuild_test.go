package registry

import (
	"context"
	"sync"
	"testing"
	"time"

	regcore "github.com/oekit/registry"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oekit/etcdx/internal/testutil"
)

// TestSubscribeWatchCompactedRebuild 钉住 #64：watch 因 MVCC 压缩被取消
// （ErrCompacted）后必须自动重取快照 + 以新 revision 重订阅。旧行为只打
// 一条 Warn 就 return：goroutine 静默死亡、订阅留在 subs map、Unsubscribe
// 「正常」返回，但服务实例列表从此永不更新，调用方完全无感知。
func TestSubscribeWatchCompactedRebuild(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	sub, err := NewSubscriber(cli, WithPrefix("/t"))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	putInst := func(ip string) {
		t.Helper()
		k := instanceKey("/t", "DEFAULT_GROUP", "order", ip+":9000")
		v, err := encodeInstance(regcore.Instance{Service: "order", IP: ip, Port: 9000})
		if err != nil {
			t.Fatalf("encodeInstance: %v", err)
		}
		if _, err := cli.Put(ctx, k, string(v)); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}

	// 先放第一个实例再订阅：初始快照回调即含 10.0.0.1
	putInst("10.0.0.1")
	var mu sync.Mutex
	var last []regcore.Instance
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
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
			for _, inst := range last {
				if inst.IP == ip {
					return true
				}
			}
			return false
		}
	}
	waitFor(t, 3*time.Second, hasIP("10.0.0.1"))

	// 确认正常事件路径：第二个实例上来了
	putInst("10.0.0.2")
	waitFor(t, 3*time.Second, hasIP("10.0.0.2"))

	// 推进全局 revision 并压缩到当前：订阅的 startRev 已低于压缩点 ⇒
	// 服务端取消 watch（ErrCompacted），重建路径必须接管
	if _, err := cli.Put(ctx, "/t/marker", "x"); err != nil {
		t.Fatalf("Put marker: %v", err)
	}
	resp, err := cli.Get(ctx, "/t/marker")
	if err != nil {
		t.Fatalf("Get marker: %v", err)
	}
	if _, err := cli.Compact(ctx, resp.Header.Revision, clientv3.WithCompactPhysical()); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 压缩后的新变更仍能收到（重建含 1s 起步的指数退避，留足窗口）——
	// 旧行为下订阅已死，这个条件永远不满足
	putInst("10.0.0.3")
	waitFor(t, 10*time.Second, hasIP("10.0.0.3"))

	_ = sub.Close(ctx)
}

// TestListenConfigWatchCompactedRebuild 钉住 #64（ConfigStore 侧）：配置
// watch 因压缩被取消后同样必须重建，否则配置推送静默停更。
func TestListenConfigWatchCompactedRebuild(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	cs, err := NewConfigStore(cli, WithPrefix("/t"))
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

	// 正常事件路径：v2 推送到达
	if err := cs.PutConfig(ctx, "app.json", "", "v2"); err != nil {
		t.Fatalf("PutConfig v2: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) > 0 && got[len(got)-1] == "v2"
	})

	// 推进 revision 并压缩到当前：监听的 startRev 低于压缩点 ⇒ watch 被取消
	if _, err := cli.Put(ctx, "/t/marker", "x"); err != nil {
		t.Fatalf("Put marker: %v", err)
	}
	resp, err := cli.Get(ctx, "/t/marker")
	if err != nil {
		t.Fatalf("Get marker: %v", err)
	}
	if _, err := cli.Compact(ctx, resp.Header.Revision, clientv3.WithCompactPhysical()); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// 压缩后的新变更仍推送（重建含 1s 起步退避 + 重建时回调一次当前值）
	if err := cs.PutConfig(ctx, "app.json", "", "v3"); err != nil {
		t.Fatalf("PutConfig v3: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) > 0 && got[len(got)-1] == "v3"
	})

	_ = cs.Close(ctx)
}
