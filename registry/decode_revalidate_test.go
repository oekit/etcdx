package registry

import (
	"context"
	"testing"
	"time"

	regcore "github.com/oekit/registry"

	"github.com/oekit/etcdx/internal/testutil"
)

// #148 回归：解码侧语义复检——绕过 Register 写入的脏实例（IP 空、port 越界）
// 不得原样进入 Discover/Subscribe。缺陷形态：Register 入口 validateInstance
// fail-fast，decodeInstance 只做 JSON 解码零复检，「写入被拒、读出放行」两侧
// 口径不一致，":0"/越界端口参与负载均衡与路由。修复：decodeInstance 复用
// validateInstance 单源复检，调用方按既有的「坏值跳过 + 带键名 WARN」剔除。
func TestDecodeSideSkipsSemanticallyInvalidInstances(t *testing.T) {
	s := testutil.NewServer(t)
	cli := s.Client()
	ctx := context.Background()
	const prefix = "/t148"
	const svc = "order"
	dir := serviceDir(prefix, "DEFAULT_GROUP", svc)

	// 直接向 etcd 写入（模拟运维脚本/外部客户端/历史遗留值等绕过 Register
	// 的写入方）：一个合法实例 + 两个脏值
	mustPut := func(addr string, inst regcore.Instance) {
		t.Helper()
		v, err := encodeInstance(inst)
		if err != nil {
			t.Fatalf("encodeInstance: %v", err)
		}
		if _, err := cli.Put(ctx, dir+addr, string(v)); err != nil {
			t.Fatalf("Put %s: %v", addr, err)
		}
	}
	mustPut("10.0.0.1:9000", regcore.Instance{Service: svc, IP: "10.0.0.1", Port: 9000, Weight: 10})
	mustPut("10.0.0.2:70000", regcore.Instance{Service: svc, IP: "10.0.0.2", Port: 70000}) // port 越界
	mustPut("ghost:0", regcore.Instance{Service: svc, IP: "", Port: 0})                    // IP 空

	d, err := NewDiscoverer(cli, WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewDiscoverer: %v", err)
	}

	// Discover 侧：只有合法实例（对照：同一实例走 Register 会被
	// validateInstance 拒绝——两侧判据现在单源）
	insts, err := d.Discover(ctx, svc)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(insts) != 1 || insts[0].Addr() != "10.0.0.1:9000" {
		t.Fatalf("Discover = %v; want 仅 [10.0.0.1:9000]（脏值剔除，#148）", insts)
	}

	all, err := d.DiscoverAll(ctx, svc)
	if err != nil {
		t.Fatalf("DiscoverAll: %v", err)
	}
	if len(all) != 1 || all[0].Addr() != "10.0.0.1:9000" {
		t.Fatalf("DiscoverAll = %v; want 仅 [10.0.0.1:9000]（脏值剔除，#148）", all)
	}

	// Subscribe 侧同口径：首次快照同样只含合法实例
	sub, err := NewSubscriber(cli, WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	snapCh := make(chan []regcore.Instance, 1)
	if err := sub.Subscribe(ctx, svc, func(got []regcore.Instance) {
		select {
		case snapCh <- got:
		default:
		}
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case snap := <-snapCh:
		if len(snap) != 1 || snap[0].Addr() != "10.0.0.1:9000" {
			t.Fatalf("Subscribe 快照 = %v; want 仅 [10.0.0.1:9000]（脏值剔除，#148）", snap)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe 首次快照 5s 未到达")
	}

	_ = sub.Close(ctx)

	// 全脏场景：Discover 归入「无实例」而非返回脏地址
	for _, addr := range []string{"10.0.0.1:9000"} {
		if _, err := cli.Delete(ctx, dir+addr); err != nil {
			t.Fatalf("Delete %s: %v", addr, err)
		}
	}
	if _, err := d.Discover(ctx, svc); err != regcore.ErrNoInstances {
		t.Fatalf("Discover(全脏) err = %v; want ErrNoInstances（#148）", err)
	}
}
