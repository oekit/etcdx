# etcdx

> etcd 全家——客户端封装 / 注册发现实现 / 分布式锁

> 🌐 项目主页：**https://oekit.github.io/** · 名称由来：**O**pen **E**asy **Kit**（开源 · 易用 · 工具集合）
> 🤖 AI Agent：本仓规范与硬规则见 [AGENTS.md](AGENTS.md)；机器可读索引见 [llms.txt](https://oekit.github.io/llms.txt)（接口签名以源码为准）。

## 安装

```bash
go get github.com/oekit/etcdx@v0.1.3
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

## 矩阵总览（24 活跃模块，自动生成）

| 模块 | 内容 | 矩阵内依赖 |
|---|---|---|
| [pkg](https://github.com/oekit/pkg) | 微工具集 19 合 1（errors/bloomx/contextx/container/stringx/slicex/enumx/convert/encoding/jsonrepair/httpjson/mathx/pathx/pagination/ptrx/timex/cache/bufiox/strmap；v0.10.0 起 uidgen/validator 拆至 kitx）；cache（v0.4.0 起）含缓存接口 CacheAside + BatchGetter 批量能力（MGet）+ LocalCache/SingleFlightCache + cachetest 契约 + Checker；container（v0.9.0 起，收编原 collectionx）泛型容器 Set/Ring/Queue/SafeMap + Map（sync.Map 背书）/Slice（并发安全）；timex/stringx（v0.7.0 起）时间段边界 BeginningOf/EndOf 七对（分/时/日/周/月/季/年，周起始显式参数）+ trie 多模式替换器 Replacer（最长匹配）/ NewMask 敏感词遮罩；singleflighttest（v0.8.0 起）singleflight 共享契约测试套件（internal 与 concurrency 双侧引用） | — |
| [concurrency](https://github.com/oekit/concurrency) | 并发与流控原语（async/gopool/group/singleflight/progress/syncx/ratelimit/retry）+ window 滑动窗口 / breaker 自适应熔断（规范哨兵 ErrCircuitOpen）/ shedding CPU 自适应降载 / batch 攒批执行器（满条数/满字节/窗口去重）/ timewheel 单级时间轮；v0.5.0 起 retry 指数退避+抖动（MaxDelay 封顶/溢出饱和）、loop 周期原语（忙时丢 tick/单轮 panic 收编/FirstDelay 可打断）、async labeled RunSafe（panic 按任务标签上报）、syncx.FanInErr 逐槽错误（panic 槽显式标记）、KeyedLimiter 增强（容量护栏/RetryAfter/速率热更）并接入 pkg/singleflighttest 契约测试 | [pkg](https://github.com/oekit/pkg) |
| [logx](https://github.com/oekit/logx) | 统一日志门面（多后端可替换 + Sink + caller 归属）+ 优雅关停；桥接子包 hlog/hertzlog/kitexlog 为嵌套模块按需引入 | — |
| [core](https://github.com/oekit/core) | 应用基建件（governance 治理 schema / config 配置加载 + default/env/range/options tag 校验 / lifecycle 契约 / network IP 工具 / version） | — |
| [kitx](https://github.com/oekit/kitx) | 标识与校验——uidgen（Snowflake 全局单例+Extract 元数据、手写 RFC 4122 v4 UUID 零第三方依赖、RandomHex/RandomString 拒绝采样随机串）+ validator（go-playground/validator v10 接口化门面：struct tag 校验/自定义规则注册/ValidationError Unwrap 为 BizError 400 对接 middleware.Fail），自 pkg v0.10.0 拆出 | [pkg](https://github.com/oekit/pkg) |
| [metrics](https://github.com/oekit/metrics) | Prometheus 门面 + 弹性指标族 | [core](https://github.com/oekit/core) |
| [tracing](https://github.com/oekit/tracing) | OpenTelemetry 追踪 + trace_id 真相源 | [logx](https://github.com/oekit/logx) |
| [registry](https://github.com/oekit/registry) | 注册发现核心 + Nacos 实现 + Nacos 配置源 + sdbridge | [core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx) |
| [data](https://github.com/oekit/data) | 数据访问门面——复刻 ekit data/ 能力面：cache/rediscache/crud/distlock/gormx/redisx/timertask/cachetest 八子包 re-export（类型同一，实现住 pkg 与各子仓）+ cached 缓存读写编排（写后失效/事务提交后失效/查询缓存版本号失效/GetByIDs 批量点读 MGet 快路径/bloom 防穿透）+ twolevel 两级缓存与跨实例失效广播（v0.7.0 起本仓唯二非 re-export 实现）；all/ 聚合模块已废弃 retract | [concurrency](https://github.com/oekit/concurrency)、[gormx](https://github.com/oekit/gormx)、[logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg)、[redisx](https://github.com/oekit/redisx)、[timertask](https://github.com/oekit/timertask) |
| [gormx](https://github.com/oekit/gormx) | SQL/GORM 全家——连接池增强、框架无关 CRUD Service、仓储（原 oekit/gorm，v0.3.0 更名） | [logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg) |
| [redisx](https://github.com/oekit/redisx) | Redis 全家——客户端封装（根包）、分布式缓存 cache/、分布式锁 distlock、分布式限流 ratelimit（Lua 三态 + 故障本地兜底） | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx)、[pkg](https://github.com/oekit/pkg) |
| [etcdx](https://github.com/oekit/etcdx) | etcd 全家——客户端封装（根包：生产默认值/Health 探活/Close(ctx) 幂等）、registry 注册发现实现（SPI 对接 registry/nacos 链路：Lease KeepAlive 注册 + 丢约指数退避自动重注册、prefix Watch 订阅推送、ConfigStore KV 配置监听）、distlock 分布式锁（Session+concurrency.Mutex 公平排队，接口与 redisx/distlock.Locker 同形可互换，可选无 etcd 降级放行） | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[registry](https://github.com/oekit/registry) |
| [httpx](https://github.com/oekit/httpx) | HTTP 客户端 + 负载均衡——服务发现/熔断（规范哨兵 ErrCircuitOpen）/重试/指标/OTel 透传/一致性哈希（权重感知虚拟副本） | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [network](https://github.com/oekit/network) | 轻量网络客户端——WebSocket/NATS/MQ 抽象、连接与流工具 | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx)、[tracing](https://github.com/oekit/tracing) |
| [timertask](https://github.com/oekit/timertask) | 分片定时任务（FNV-1a、心跳续约）；v0.1.5 起可选可靠投递 DeliveryAtLeastOnce（租约 + Ack + 可见性超时；过期租约回扫重投/死信、停机 best-effort 收租，v0.1.7 接入生命周期真正生效） | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [middleware](https://github.com/oekit/middleware) | Hertz 自研标准中间件栈 + contrib 桥接 + hmw + SheddingMiddleware 自适应降载；v0.6.0 起 accesslog 凭证路径脱敏（WithAccessLogRedactPathPrefixes）、ratelimit 默认 KeyFunc 去端口（连接桶→地址桶） | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [k8s](https://github.com/oekit/k8s) | Kubernetes 客户端封装——RESTConfig/Clientset/DynamicClient 构建（显式 kubeconfig → in-cluster 自动 → KUBECONFIG/~/.kube/config 逐级回退，QPS/Burst 限流默认 50/100）+ leaderelection 子包 Lease 选主简化封装（OnStartedLeading/OnStoppedLeading/OnNewLeader 回调，主备单活，仅要求 Lease CRUD 权限） | [logx](https://github.com/oekit/logx) |
| [nns](https://github.com/oekit/nns) | NNS 服务治理客户端——provider 无关发现/配置/实例缓存 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[registry](https://github.com/oekit/registry) |
| [nns-endpoint](https://github.com/oekit/nns-endpoint) | NNS 端点工厂——per-service 熔断、换实例重试、gRPC 池 | [concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[httpx](https://github.com/oekit/httpx)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[registry](https://github.com/oekit/registry) |
| [bootstrap](https://github.com/oekit/bootstrap) | 应用生命周期引导——bootstrap.Run 骨架 + healthz | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [hertz](https://github.com/oekit/hertz) | Hertz 运行时集成——日志桥接、生命周期、SSE/health/metrics、适配器、OTel、crudh/validatorh（基于 kitx/validator）/versionh（原 handler 微库收编） | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[gormx](https://github.com/oekit/gormx)、[kitx](https://github.com/oekit/kitx)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[middleware](https://github.com/oekit/middleware)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
| [kitex](https://github.com/oekit/kitex) | Kitex 生态——日志桥接、治理热更、生命周期、BizError 桥接、kitexotel、istio（xDS 动态熔断，原 istio 微库收编） | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry) |
| [workflow](https://github.com/oekit/workflow) | 组合式流程编排——串行/并行/异步/条件分支 + Hook AOP | [concurrency](https://github.com/oekit/concurrency)、[logx](https://github.com/oekit/logx) |
| [kitex-examples](https://github.com/oekit/kitex-examples) | Kitex echo 示例——client/server + IDL + 治理配置 | [bootstrap](https://github.com/oekit/bootstrap)、[concurrency](https://github.com/oekit/concurrency)、[core](https://github.com/oekit/core)、[kitex](https://github.com/oekit/kitex)、[logx](https://github.com/oekit/logx)、[metrics](https://github.com/oekit/metrics)、[nns](https://github.com/oekit/nns)、[pkg](https://github.com/oekit/pkg)、[registry](https://github.com/oekit/registry)、[tracing](https://github.com/oekit/tracing) |
