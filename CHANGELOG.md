# Changelog — oekit/etcdx

所有 notable 变更记录如下。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

## [v0.1.0] — 2026-10-17

首个版本。

### 目录结构

```
github.com/oekit/etcdx                 # package etcdx — 客户端封装（模块根）
├── registry/                          # package registry — registry SPI 的 etcd 实现
├── distlock/                          # package distlock — 分布式锁（Locker 同形 redisx）
└── internal/testutil/                 # 内嵌 etcd 测试基建（不进公共 API）
```

### 能力面

- **etcdx（根包）**：`NewClient`（Endpoints 显式校验、DialTimeout 默认 5s）、
  `NewClientFromConfig`（YAML 友好 + 构造即 Health）、`Health`（任一端点
  Member Status 可用即健康）、`Close(ctx)` sync.Once 幂等
- **registry**：Registrar（Lease KeepAlive + 丢约指数退避自动重注册，重复
  Register 幂等刷新）、Discoverer（prefix 拉取，结果按地址排序确定；无实例
  返回 `registry.ErrNoInstances`）、Subscriber（prefix Watch 推送，订阅即
  回调一次快照，回调串行执行）、ConfigStore（GetConfig 缺席返回空串非错误、
  ListenConfig Put/Delete 均回调、PutConfig 便捷写）；SPI 接口来自
  `github.com/oekit/registry/registry` v0.3.0，可与 registry/nacos 互换
- **distlock**：Session（租约 TTL + 自动续约，崩溃兜底释放）+ concurrency.Mutex
  （revision 公平排队、Lock 阻塞等待而非轮询）；ownerID 进程内校验防误释放；
  `WithGracefulDegradation` 可选无 etcd 降级放行；`WithOnLock` 指标钩子；
  接口与 `redisx/distlock.Locker` 同形可互换
- 测试：内嵌 etcd 服务端（embed，内核随机端口），覆盖注册/发现/订阅推送/
  配置监听/锁互斥与超时/降级全链路，`-race` 全绿；CI 无需 docker
