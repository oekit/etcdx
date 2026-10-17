// Package registry 是 github.com/oekit/registry SPI 的 etcd 实现：
// Registrar（Lease + KeepAlive 注册，租约丢失自动重注册）、Discoverer
// （prefix 拉取）、Subscriber（prefix Watch 推送）、ConfigStore（KV Watch
// 配置监听）。上层 nns 等依赖 SPI 接口而非本包，可与 registry/nacos 互换。
//
// # 键布局
//
//	<prefix>/services/<group>/<service>/<ip:port>  → 实例 JSON（含 weight/metadata/cluster）
//	<prefix>/configs/<group>/<dataID>              → 配置内容字符串
//
// prefix 默认 "/oekit/registry"，group 默认 "DEFAULT_GROUP"。
//
// # 健康语义
//
// etcd 没有独立的健康状态：实例键绑定 Lease（默认 30s TTL + 自动续约），
// 进程崩溃后租约到期键自动消失，因此 Discover 与 DiscoverAll 返回同一集合。
//
// # 用法
//
//	import (
//	    regcore "github.com/oekit/registry"
//	    "github.com/oekit/etcdx/registry"
//	)
//
//	reg := registry.NewRegistrar(cli, registry.WithPrefix("/myapp"), registry.WithTTL(15*time.Second))
//	_ = reg.Register(ctx, regcore.Instance{Service: "user", IP: "10.0.0.1", Port: 8080})
//	defer reg.Close(ctx)
//
//	disc := registry.NewDiscoverer(cli, registry.WithPrefix("/myapp"))
//	insts, _ := disc.Discover(ctx, "user")
//
// 与 SPI 同名包 co-import 时请为本包起别名（如 etcdreg）。
package registry
