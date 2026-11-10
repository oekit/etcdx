package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	regcore "github.com/oekit/registry"

	"github.com/oekit/etcdx/internal/testutil"
)

// TestKeySegmentsRejectSlash 钉住 #110 的入口面：group/service/dataID 是键层级
// 的一段，出现 "/" 与层级分隔符不可区分（键结构
// <prefix>/services/<group>/<service>/<addr> 纯字符串拼接）——服务 "order/v2"
// 的实例键会落进服务 "order" 的前缀扫描范围。注册/发现/订阅/配置各入口必须
// fail-fast 拒绝含 "/" 的段，把「键层级边界」变成构造期/入口期不变量，
// 不再依赖命名习惯。
func TestKeySegmentsRejectSlash(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()

	reg, err := NewRegistrar(cli)
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	if err := reg.Register(ctx, regcore.Instance{Service: "order/v2", IP: "10.0.0.1", Port: 8080}); err == nil {
		t.Fatal(`Register(service="order/v2") = nil; want error（服务名含 "/" 跨键空间层级）`)
	}
	if err := reg.Register(ctx, regcore.Instance{Service: "order", Group: "team/a", IP: "10.0.0.1", Port: 8080}); err == nil {
		t.Fatal(`Register(group="team/a") = nil; want error（分组名含 "/"）`)
	}
	if err := reg.Deregister(ctx, regcore.Instance{Service: "order/v2", IP: "10.0.0.1", Port: 8080}); err == nil {
		t.Fatal(`Deregister(service="order/v2") = nil; want error（入口同口径）`)
	}

	disc, err := NewDiscoverer(cli)
	if err != nil {
		t.Fatalf("NewDiscoverer: %v", err)
	}
	if _, err := disc.Discover(ctx, "order/v2"); err == nil {
		t.Fatal(`Discover("order/v2") = nil error; want error`)
	}
	if _, err := disc.DiscoverAll(ctx, "order/v2"); err == nil {
		t.Fatal(`DiscoverAll("order/v2") = nil error; want error`)
	}

	sub, err := NewSubscriber(cli)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if err := sub.Subscribe(ctx, "order/v2", func([]regcore.Instance) {}); err == nil {
		t.Fatal(`Subscribe("order/v2") = nil; want error`)
	}

	cs, err := NewConfigStore(cli)
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	if err := cs.PutConfig(ctx, "app/cfg.json", "", "{}"); err == nil {
		t.Fatal(`PutConfig(dataID="app/cfg.json") = nil; want error`)
	}
	if _, err := cs.GetConfig(ctx, "app/cfg.json", ""); err == nil {
		t.Fatal(`GetConfig(dataID="app/cfg.json") = nil error; want error`)
	}
	if err := cs.ListenConfig(ctx, "app/cfg.json", "", func(string) {}); err == nil {
		t.Fatal(`ListenConfig(dataID="app/cfg.json") = nil; want error`)
	}
	if err := cs.StopListenConfig(ctx, "app/cfg.json", ""); err == nil {
		t.Fatal(`StopListenConfig(dataID="app/cfg.json") = nil; want error`)
	}
	if err := cs.PutConfig(ctx, "cfg.json", "team/a", "{}"); err == nil {
		t.Fatal(`PutConfig(group="team/a") = nil; want error`)
	}

	// 构造期不变量：默认分组含 "/" 同样跨层，必须构造期拒绝
	if _, err := NewDiscoverer(cli, WithGroup("team/a")); err == nil {
		t.Fatal(`NewDiscoverer(WithGroup("team/a")) = nil error; want error（构造期 fail-fast）`)
	}
}

// TestDiscoverNoCrossServiceLeak 钉住 #110 的失败场景推演：服务 "order/v2"
// 的实例键（遗留数据/绕过校验写入）落在 /services/<group>/order/ 前缀之下，
// Discover("order") 连带返回它的实例并解码成 Service="order" ——服务发现把
// order 的流量路由到 order/v2 的实例上，全程无报错。读侧必须按「直接子键」
// 收紧：实例键 = 服务目录 + addr（ip:port 不含 "/"），余段含 "/" 的键一律
// 不属于本服务。
func TestDiscoverNoCrossServiceLeak(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()

	// 直连 etcd 写入「order/v2」的实例键，模拟修复前的遗留数据
	val, err := encodeInstance(regcore.Instance{Service: "order/v2", IP: "10.0.0.1", Port: 8080})
	if err != nil {
		t.Fatalf("encodeInstance: %v", err)
	}
	if _, err := cli.Put(ctx, "/oekit/registry/services/DEFAULT_GROUP/order/v2/10.0.0.1:8080", string(val)); err != nil {
		t.Fatalf("seed legacy key: %v", err)
	}

	disc, err := NewDiscoverer(cli)
	if err != nil {
		t.Fatalf("NewDiscoverer: %v", err)
	}
	insts, err := disc.Discover(ctx, "order")
	if err == nil {
		t.Fatalf(`Discover("order") = %+v; want ErrNoInstances（不得连带返回 order/v2 的实例）`, insts)
	}
	if !errors.Is(err, regcore.ErrNoInstances) {
		t.Fatalf(`Discover("order") err = %v; want ErrNoInstances`, err)
	}
	all, err := disc.DiscoverAll(ctx, "order")
	if err != nil {
		t.Fatalf(`DiscoverAll("order"): %v`, err)
	}
	if len(all) != 0 {
		t.Fatalf(`DiscoverAll("order") = %+v; want 空（不得混入 order/v2 的实例）`, all)
	}

	// Subscribe 的首次快照同口径
	sub, err := NewSubscriber(cli)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	snapCh := make(chan []regcore.Instance, 1)
	if err := sub.Subscribe(ctx, "order", func(insts []regcore.Instance) {
		select {
		case snapCh <- insts:
		default:
		}
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	select {
	case snap := <-snapCh:
		if len(snap) != 0 {
			t.Fatalf(`Subscribe("order") 首次快照 = %+v; want 空（不得混入 order/v2 的实例）`, snap)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Subscribe 首次快照未在 5s 内送达")
	}
}

// TestConfigNoCrossGroupLeak 钉住 #110 的 configKey 同构面：configKey 把
// group/dataID 直接拼进 <prefix>/configs/<group>/<dataID>，dataID 含 "/" 时
// (group="a", dataID="b/c") 与 (group="a/b", dataID="c") 解析到同一个键——
// 跨 group 串扰。入口必须拒 "/"；遗留歧义键不得被任何合法 (group, dataID)
// 组合读到。
func TestConfigNoCrossGroupLeak(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()

	// 遗留歧义键：group="a", dataID="b/c" 写入的 /configs/a/b/c
	if _, err := cli.Put(ctx, "/oekit/registry/configs/a/b/c", "leaked"); err != nil {
		t.Fatalf("seed legacy config key: %v", err)
	}

	cs, err := NewConfigStore(cli)
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	// (group="a/b", dataID="c") 解析到同一遗留键：入口必须拒 "/"，不得读到别的分组/ID 的配置
	if got, err := cs.GetConfig(ctx, "c", "a/b"); err == nil {
		t.Fatalf(`GetConfig(dataID="c", group="a/b") = %q, nil; want error（group 含 "/" 跨层串扰）`, got)
	}
	if got, err := cs.GetConfig(ctx, "b/c", "a"); err == nil {
		t.Fatalf(`GetConfig(dataID="b/c", group="a") = %q, nil; want error（dataID 含 "/" 跨层串扰）`, got)
	}
}

// TestInstanceIPSlashRejected 钉住 #110 的 addr 段完整性：实例键 = 服务目录 +
// <ip:port>，ip 含 "/" 会让键多出一层——既可能落进别的服务的前缀，又会被
// 「直接子键」读侧防御当成嵌套键跳过（注册成功却永远不可发现的静默故障）。
// addr 段同样不允许 "/"。
func TestInstanceIPSlashRejected(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	reg, err := NewRegistrar(cli)
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	if err := reg.Register(ctx, regcore.Instance{Service: "order", IP: "10.0.0.0/8", Port: 8080}); err == nil {
		t.Fatal(`Register(ip="10.0.0.0/8") = nil; want error（addr 段含 "/"）`)
	}
}
