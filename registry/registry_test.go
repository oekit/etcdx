package registry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	regcore "github.com/oekit/registry"

	"github.com/oekit/etcdx/internal/testutil"
)

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestRegisterDiscoverDeregister(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	reg, err := NewRegistrar(cli, WithPrefix("/t"), WithTTL(5*time.Second))
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	disc, err := NewDiscoverer(cli, WithPrefix("/t"))
	if err != nil {
		t.Fatalf("NewDiscoverer: %v", err)
	}

	if _, err := disc.Discover(ctx, "user"); !errors.Is(err, regcore.ErrNoInstances) {
		t.Fatalf("empty Discover err = %v; want ErrNoInstances", err)
	}
	all, err := disc.DiscoverAll(ctx, "user")
	if err != nil || len(all) != 0 {
		t.Fatalf("empty DiscoverAll = %v, %v; want [], nil", all, err)
	}

	a := regcore.Instance{Service: "user", IP: "10.0.0.1", Port: 8080, Weight: 20, Metadata: map[string]string{"v": "1"}}
	b := regcore.Instance{Service: "user", IP: "10.0.0.2", Port: 8080, Weight: 10}
	if err := reg.Register(ctx, a); err != nil {
		t.Fatalf("Register a: %v", err)
	}
	if err := reg.Register(ctx, b); err != nil {
		t.Fatalf("Register b: %v", err)
	}

	insts, err := disc.Discover(ctx, "user")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(insts) != 2 || insts[0].IP != "10.0.0.1" || insts[1].IP != "10.0.0.2" {
		t.Fatalf("Discover = %+v; want a,b sorted by addr", insts)
	}
	if insts[0].Weight != 20 || insts[0].Metadata["v"] != "1" || insts[0].Group != "DEFAULT_GROUP" {
		t.Fatalf("instance fields lost: %+v", insts[0])
	}

	// 重复 Register 视为刷新，不产生重复实例
	if err := reg.Register(ctx, a); err != nil {
		t.Fatalf("re-Register a: %v", err)
	}
	if insts, _ = disc.Discover(ctx, "user"); len(insts) != 2 {
		t.Fatalf("after refresh Discover len = %d; want 2", len(insts))
	}

	if err := reg.Deregister(ctx, a); err != nil {
		t.Fatalf("Deregister a: %v", err)
	}
	if err := reg.Deregister(ctx, a); err != nil {
		t.Fatalf("second Deregister should be no-op: %v", err)
	}
	insts, _ = disc.Discover(ctx, "user")
	if len(insts) != 1 || insts[0].IP != "10.0.0.2" {
		t.Fatalf("after Deregister Discover = %+v; want only b", insts)
	}

	if err := reg.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := disc.Discover(ctx, "user"); !errors.Is(err, regcore.ErrNoInstances) {
		t.Fatalf("after Close Discover err = %v; want ErrNoInstances", err)
	}
	if err := reg.Register(ctx, b); err == nil {
		t.Fatal("Register after Close should fail")
	}
}

func TestRegisterValidation(t *testing.T) {
	cli := testutil.NewClient(t)
	reg, err := NewRegistrar(cli)
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	ctx := context.Background()
	for _, inst := range []regcore.Instance{
		{IP: "1.1.1.1", Port: 80},
		{Service: "s", Port: 80},
		{Service: "s", IP: "1.1.1.1"},
		{Service: "s", IP: "1.1.1.1", Port: 70000},
	} {
		if err := reg.Register(ctx, inst); err == nil {
			t.Fatalf("Register(%+v) should fail", inst)
		}
	}
}

func TestSubscribe(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	reg, err := NewRegistrar(cli, WithPrefix("/t"))
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	sub, err := NewSubscriber(cli, WithPrefix("/t"))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}

	var mu sync.Mutex
	var snapshots [][]regcore.Instance
	err = sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		mu.Lock()
		snapshots = append(snapshots, insts)
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// 订阅即回调一次空快照
	mu.Lock()
	if len(snapshots) != 1 || len(snapshots[0]) != 0 {
		mu.Unlock()
		t.Fatalf("initial snapshot = %+v; want one empty", snapshots)
	}
	mu.Unlock()

	inst := regcore.Instance{Service: "order", IP: "10.0.0.9", Port: 9000}
	if err := reg.Register(ctx, inst); err != nil {
		t.Fatalf("Register: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		last := snapshots[len(snapshots)-1]
		return len(last) == 1 && last[0].IP == "10.0.0.9"
	})

	if err := reg.Deregister(ctx, inst); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(snapshots[len(snapshots)-1]) == 0
	})

	if err := sub.Unsubscribe(ctx, "order"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	mu.Lock()
	n := len(snapshots)
	mu.Unlock()
	_ = reg.Register(ctx, inst)
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	if len(snapshots) != n {
		mu.Unlock()
		t.Fatal("callback fired after Unsubscribe")
	}
	mu.Unlock()
	_ = reg.Close(ctx)
}

func TestConfigStore(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	cs, err := NewConfigStore(cli, WithPrefix("/t"))
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	v, err := cs.GetConfig(ctx, "app.json", "")
	if err != nil || v != "" {
		t.Fatalf("missing GetConfig = %q, %v; want \"\", nil", v, err)
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

	if err := cs.PutConfig(ctx, "app.json", "", `{"a":1}`); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if v, _ = cs.GetConfig(ctx, "app.json", ""); v != `{"a":1}` {
		t.Fatalf("GetConfig = %q", v)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1 && got[0] == `{"a":1}`
	})

	if _, err := cli.Delete(ctx, configKey("/t", "DEFAULT_GROUP", "app.json")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	waitFor(t, 3*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 2 && got[1] == ""
	})

	if err := cs.StopListenConfig(ctx, "app.json", ""); err != nil {
		t.Fatalf("StopListenConfig: %v", err)
	}
	_ = cs.PutConfig(ctx, "app.json", "", "after-stop")
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	if len(got) != 2 {
		mu.Unlock()
		t.Fatal("callback fired after StopListenConfig")
	}
	mu.Unlock()
}
