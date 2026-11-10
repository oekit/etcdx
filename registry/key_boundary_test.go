package registry

import (
	"context"
	"testing"
	"time"

	regcore "github.com/oekit/registry"

	"github.com/oekit/etcdx/internal/testutil"
)

// #110 回归：键路径段（service/group/dataID/ip）含 "/" 时键层级边界失守——
// service "order/v2" 的实例键落在 "order/" 前缀之下，Discover("order") 的
// 前缀扫描连带返回 order/v2 的实例并解码成 Service="order"，流量被路由到
// 另一个服务且全程无报错；configKey 同构（group "g/a"+dataID "b" 与
// group "g"+dataID "a/b" 拼出同一个键，跨 group 串扰）。

func TestRegisterRejectsSlashInNameSegments(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	reg, err := NewRegistrar(cli, WithPrefix("/t110"), WithTTL(5*time.Second))
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	for _, inst := range []regcore.Instance{
		{Service: "order/v2", IP: "10.0.0.1", Port: 8080},            // service 含 "/"
		{Service: "order", Group: "g/a", IP: "10.0.0.1", Port: 8080}, // group 含 "/"
		{Service: "order", IP: "10.0.0.1/24", Port: 8080},            // ip 是实例键最后一段，同样不得含 "/"
	} {
		if err := reg.Register(ctx, inst); err == nil {
			t.Fatalf("Register service=%q group=%q ip=%q 成功；want 键路径段含 \"/\" 的 fail-fast 错误（#110）",
				inst.Service, inst.Group, inst.IP)
		}
	}
}

// TestDiscoverDoesNotCrossServiceBoundary 钉住路由面：即使 etcd 里残留了
// 历史版本写出的越界键（含 "/" 的 service 名，入口 fail-fast 之前落库的
// 脏数据），前缀扫描也必须过滤段数不吻合的键——Discover("order") 只返回
// order 自己的实例，不把 "order/v2" 的实例解码成 Service="order" 带走。
func TestDiscoverDoesNotCrossServiceBoundary(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	prefix := "/t110x"
	// 直接灌键：模拟历史版本写出的键空间（入口修复后不可能再产生新脏键）
	val := `{"ip":"10.0.0.1","port":8080}`
	for key, content := range map[string]string{
		prefix + "/services/DEFAULT_GROUP/order/10.0.0.1:8080":          val, // 干净键
		prefix + "/services/DEFAULT_GROUP/order/v2/10.0.0.2:8080":       val, // service "order/v2" 的越界键
		prefix + "/services/DEFAULT_GROUP/order/sub/deep/10.0.0.3:8080": val, // 多段越界键
	} {
		if _, err := cli.Put(ctx, key, content); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	disc, err := NewDiscoverer(cli, WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewDiscoverer: %v", err)
	}
	insts, err := disc.Discover(ctx, "order")
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(insts) != 1 {
		t.Fatalf("Discover(\"order\") 返回 %d 个实例；want 1（越界键不得跨服务参与路由，#110）: %+v", len(insts), insts)
	}
	if insts[0].Service != "order" || insts[0].Addr() != "10.0.0.1:8080" {
		t.Fatalf("Discover(\"order\") 返回了别的服务的实例: %+v", insts[0])
	}
	if all, err := disc.DiscoverAll(ctx, "order"); err != nil || len(all) != 1 {
		t.Fatalf("DiscoverAll = %v, %v; want 1 个实例", all, err)
	}
}

func TestSubscribeRejectsSlashServiceName(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	sub, err := NewSubscriber(cli, WithPrefix("/t110s"))
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	if err := sub.Subscribe(ctx, "order/v2", func([]regcore.Instance) {}); err == nil {
		t.Fatal("Subscribe(\"order/v2\") 成功；want fail-fast 错误（#110）")
	}
	_ = sub.Close(ctx)
}

// TestConfigKeySegmentCollision 钉住 configKey 同构串扰：group "g/a" +
// dataID "b" 与 group "g" + dataID "a/b" 历史上拼出同一个键——一边灌配置、
// 另一边读走。修复后两个入口都 fail-fast，不再产生歧义键。
func TestConfigKeySegmentCollision(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	cfg, err := NewConfigStore(cli, WithPrefix("/t110c"))
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}
	if err := cfg.PutConfig(ctx, "a/b", "g", "content"); err == nil {
		t.Fatal("PutConfig dataID=\"a/b\" 成功；want fail-fast 错误（#110）")
	}
	if err := cfg.PutConfig(ctx, "b", "g/a", "content"); err == nil {
		t.Fatal("PutConfig group=\"g/a\" 成功；want fail-fast 错误（#110）")
	}
	if err := cfg.ListenConfig(ctx, "a/b", "g", func(string) {}); err == nil {
		t.Fatal("ListenConfig dataID=\"a/b\" 成功；want fail-fast 错误（#110）")
	}
	if err := cfg.StopListenConfig(ctx, "a/b", "g"); err == nil {
		t.Fatal("StopListenConfig dataID=\"a/b\" 成功；want fail-fast 错误（#110）")
	}
	// 串扰面：修复前 PutConfig("a/b","g") 写出 /configs/g/a/b，GetConfig("b","g/a")
	// 读走同一键；修复后入口拒绝，此路不通
	if _, err := cfg.GetConfig(ctx, "b", "g/a"); err == nil {
		t.Fatal("GetConfig group=\"g/a\" dataID=\"b\" 成功；want fail-fast 错误（#110）")
	}
}

// TestCleanNamesStillWork 回归：干净名称的键布局一字不变——入口校验只拒
// "/"，既有键与消费路径完全兼容（#110 的向后兼容承诺）。
func TestCleanNamesStillWork(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	reg, err := NewRegistrar(cli, WithPrefix("/t110ok"), WithTTL(5*time.Second))
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	disc, err := NewDiscoverer(cli, WithPrefix("/t110ok"))
	if err != nil {
		t.Fatalf("NewDiscoverer: %v", err)
	}
	inst := regcore.Instance{Service: "order", Group: "DEFAULT_GROUP", IP: "10.0.0.1", Port: 8080}
	if err := reg.Register(ctx, inst); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// 键布局不变：与历史版本写出的键互相可见
	got, err := cli.Get(ctx, "/t110ok/services/DEFAULT_GROUP/order/10.0.0.1:8080")
	if err != nil || len(got.Kvs) != 1 {
		t.Fatalf("键布局变化：/t110ok/services/DEFAULT_GROUP/order/10.0.0.1:8080 = %v, %v", got.Kvs, err)
	}
	insts, err := disc.Discover(ctx, "order")
	if err != nil || len(insts) != 1 {
		t.Fatalf("Discover = %v, %v; want 1 个实例", insts, err)
	}
}
