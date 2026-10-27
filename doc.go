// Package etcdx 提供 etcd 客户端全家——客户端封装（根包）、注册发现实现
// （registry，实现 github.com/oekit/registry SPI）、分布式锁
// （distlock，与 redisx/distlock.Locker 接口形态一致可互换）。
//
// 零可观测性副作用：不自动注入任何 tracing/logger 后端，SDK 内部统一
// logx.Delegating("etcdx") 懒解析。
//
// # 快速接入
//
//	client, err := etcdx.NewClient(&clientv3.Config{
//	    Endpoints: []string{"127.0.0.1:2379"},
//	})
//
// 注册发现（SPI 对接 nns/registry 链路）：
//
//	reg, err := registry.NewRegistrar(client.Client(), registry.WithPrefix("/myapp"))
//	if err != nil {
//	    return err
//	}
//	_ = reg.Register(ctx, regcore.Instance{Service: "user", IP: "10.0.0.1", Port: 8080})
//
// 分布式锁：
//
//	locker, err := distlock.NewEtcdLocker(client.Client(), distlock.WithPrefix("myapp"))
//	if err != nil {
//	    return err
//	}
//	err = distlock.WithLock(ctx, locker, "deploy:myapp", 30*time.Second, 10*time.Second, func() error {
//	    return doDeploy()
//	})
package etcdx
