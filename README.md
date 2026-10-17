# etcdx

> etcd 全家——客户端封装 / 注册发现实现 / 分布式锁

> 🌐 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

## 安装

```bash
go get github.com/oekit/etcdx@v0.1.0
```

## 这个仓库做什么

`etcdx` 是 **etcd 全家桶**：客户端封装（根包）、oekit 注册发现 SPI 的 etcd 实现
（`registry`）、分布式锁（`distlock`）。让业务不直接拼 `clientv3` 细节，
注册发现可直接对接 nns/registry 链路，分布式锁与 `redisx/distlock` 接口同形可互换。

零可观测性副作用：不自动注入任何 tracing/logger 后端；SDK 内部统一
`logx.Delegating("etcdx")` 懒解析。

## 组成

| 包 | import 路径 | 做什么 |
|---|---|---|
| `etcdx`（模块根包） | `github.com/oekit/etcdx` | `NewClient`（生产默认值：DialTimeout 5s、Endpoints 显式校验）、`NewClientFromConfig`（YAML 友好配置 + 构造即 Health）、`Health`（Member Status 任一端点可用即健康）、`Close(ctx)` 幂等关停 |
| `registry` | `github.com/oekit/etcdx/registry` | `github.com/oekit/registry/registry` SPI 的 etcd 实现：`Registrar`（Lease KeepAlive 注册 + 丢约指数退避自动重注册）、`Discoverer`（prefix 拉取，按地址排序）、`Subscriber`（prefix Watch 推送，订阅即回调快照）、`ConfigStore`（KV Watch 配置监听）；键布局 `<prefix>/services/<group>/<service>/<ip:port>` |
| `distlock` | `github.com/oekit/etcdx/distlock` | `EtcdLocker`：每锁独占 Session（租约 + 自动续约，进程崩溃 TTL 兜底释放）、`concurrency.Mutex` revision 公平排队；`TryLock`/`Lock`/`Unlock` + `WithLock` 便捷包装；接口与 `redisx/distlock.Locker` 同形可直接互换；可选无 etcd 降级放行 |

## 快速使用

```go
import (
    "github.com/oekit/etcdx"
    "github.com/oekit/etcdx/distlock"
    etcdreg "github.com/oekit/etcdx/registry"
    regcore "github.com/oekit/registry/registry"
)

client, _ := etcdx.NewClientFromConfig(ctx, etcdx.EtcdConfig{
    Endpoints: []string{"127.0.0.1:2379"},
})

// 服务注册（进程崩溃租约到期自动摘除）
reg := etcdreg.NewRegistrar(client.Client(), etcdreg.WithPrefix("/myapp"), etcdreg.WithTTL(15*time.Second))
_ = reg.Register(ctx, regcore.Instance{Service: "user", IP: "10.0.0.1", Port: 8080})
defer reg.Close(ctx)

// 分布式锁（与 redisx/distlock.Locker 同形）
locker := distlock.NewEtcdLocker(client.Client(), distlock.WithPrefix("myapp"))
_ = distlock.WithLock(ctx, locker, "deploy:user", 30*time.Second, 10*time.Second, func() error {
    return doDeploy()
})
```

### 与 redisx/distlock 互换

`distlock.EtcdLocker` 与 `redisx/distlock` 的 `Locker` 接口方法签名完全一致：
面向接口消费（如 `data/distlock` 门面）时，两种后端可直接替换，业务代码零改动。
哨兵错误（`ErrLockNotAcquired`/`ErrLockTimeout`）同名但分属两仓，`errors.Is`
请使用对应后端包的哨兵。

## 边界

- 不做 etcd 集群运维（备份/扩缩容）与 `etcdctl` 封装
- 不内置 tracing 注入（装饰器模式，同 redisx）
- 测试基于进程内嵌 etcd（`internal/testutil`），CI 无需 docker

## License

MIT
