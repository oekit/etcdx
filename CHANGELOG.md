# Changelog — oekit/etcdx

所有 notable 变更记录如下。格式参考 [Keep a Changelog](https://keepachangelog.com/)。

## [v0.3.2] — 2026-11-13

- 10 单修复批：watch 重建补投递（#147）、decodeInstance 复检（#148）、退订 retired 槽位（#149）、pkg#428 释放 ctx 口径测试适配等；地板对齐 pkg v0.47.0/core v0.15.1。

## [v0.3.1] — 2026-11-12

- Go 1.26 现代化：registry/discoverer.go sort.Slice→slices.SortFunc(cmp.Compare)；依赖地板对齐 concurrency v0.34.1。纯等价重构+地板追平，行为零变更。

## [v0.3.0] — 2026-11-12

- refactor(distlock)【BREAKING】: distlock 契约单源落地（etcdx#123，消费 github.com/oekit/pkg v0.45.0 的 distlock 后端中立契约）——Locker 改为 pkg/distlock.Locker 类型别名，EtcdLocker 直接满足契约；哨兵改 pkg 同值别名（ErrLockNotAcquired/ErrLockTimeout，新增契约名 ErrLockNotHeld）。**BREAKING**：ErrNotOwner 对外文案由 "lock not owned by this owner" 变为 "lock not held by this owner"（映射 pkg 契约哨兵，值身份不变、errors.Is 判定不受影响，历史名保留）；删除 sentinel.Is/IsOneOf 双向文案匹配机制（grep 全工作区无外部消费方，哨兵单源后裸 errors.Is 即正确分类，429/500 不再漂移）| refactor(distlock)【BREAKING】: WithLock 删本地实现改薄委托 pkg/distlock.WithLock 单源（etcdx#123）——释放纪律统一为 context.WithoutCancel 口径（原 WithTimeout(Background, 3s)：不携带 caller ctx 取值 + 3s 魔法常数；现释放 ctx 与 caller 取消状态解耦且取值原样保留）；获取失败错误统一带 "lock <key>:" 前缀包装、哨兵经 %w 穿透（原实现无包装，与 redisx 口径对齐）| refactor(registry): Subscriber.Subscribe 与 ConfigStore.ListenConfig 复制的 watch 重建循环骨架提取为 runWatchLoop 单源（etcdx#122）——#55 Canceled/断连重建、#84 superseded 丢弃、#85 退避成功归零只实现一份，差异点（watch 目标/事件投递形态/快照重取补投递）闭包注入；日志统一 target 字段（原 service/dataID）| refactor(distlock): EtcdLocker.acquire 的 caller-ctx 判别与 notify/degradeOrFail 路由提取 stageFailed 单源（etcdx#124）——#54 caller-ctx ≠ etcd 不可达口径不变，acquire 主体缩至约 50 行；建 Session 阶段 caller-ctx 失败补 notify（原实现该分支漏回调，与其他阶段对齐）| build: 依赖地板——pkg v0.42.0→v0.45.0（indirect→direct，契约消费）、logx v0.9.2→v0.10.0、registry v0.5.14→v0.5.15（发版前 floor sync）

关联问题单：etcdx#122、etcdx#123、etcdx#124。

关联问题单：etcdx#122、etcdx#123、etcdx#124。

## [v0.2.12] — 2026-11-11

- fix(registry): 键路径段（group/service/dataID/ip）含 "/" 入口 fail-fast + 前缀扫描过滤段数不吻合的既有脏键（#110）——service "order/v2" 的实例键落在 "order/" 前缀之下，Discover("order") 连带返回 order/v2 的实例并解码成 Service="order"，流量被路由到另一个服务且全程无报错；configKey 同构（group "g/a"+dataID "b" 与 group "g"+dataID "a/b" 拼出同一个键，跨 group 串扰）。注册/发现/订阅/配置入口把「键层级边界」变成构造期不变量；键布局不变，既有干净键完全兼容
- fix(distlock): 建 Session 的租约 Grant 显式化并带上界（#111）——此前 NewSession 内部的 Grant 走 Session 自身的后台 ctx（无任何 deadline），连接预检只覆盖「探测时已不可达」，预检通过后 etcd 进入黑洞（网络分区/节点宕机切换）时 Lock 永久挂死，caller timeout/取消与 WithGracefulDegradation 全部失效（根本没有错误返回）。现在 caller ctx 贯穿 Grant（WithRequireLeader 元数据让「无 leader」快速失败 + 3s 窗口与预检同窗），拿到租约后以 WithLease 组装 Session——keepalive 仍由 Session 自身 ctx 管理，生命周期语义不变，失败走与预检相同的 degradeOrFail 分类
- feat(distlock): 新增 IsOneOf——「绑哪一侧的哨兵都能正确分类」双向成立（#112）——裸 errors.Is 的跨包等价挂在链头错误的 Is 方法上，redisx 侧哨兵是无 Is 方法的普通 errors.New 值，errors.Is(redisx.ErrLockTimeout, etcdx.ErrLockTimeout) 恒 false，适配层绑 etcdx 哨兵跑 redisx 后端时正常超时被误分类成 500（#86 承诺的反方向缺口）。IsOneOf 在 errors.Is 之外追加链上同文案判定；裸 errors.Is 仍单向（治本须 redisx 侧哨兵实现同款 Is 或双方共用同一值），Locker 接口注释与 doc 同步修正

关联问题单：etcdx#110、etcdx#111、etcdx#112。

## [v0.2.11] — 2026-11-10

- - build: 依赖地板对齐（第二轮发版后 floor sync，无代码变更）

## [v0.2.10] — 2026-11-10

- - build: 依赖地板对齐（floor sync，无代码变更）

## [v0.2.9] — 2026-11-09

- fix(registry): 订阅循环回调 panic 隔离——一次 panic 不再杀死重建循环（#87）

关联问题单：etcdx#87。

## [v0.2.8] — 2026-11-09

- fix(registry): 配置 watch 重建投递已删键空串——对齐 Delete 事件承诺（#83）
- fix(registry): 重复 Subscribe 等旧 goroutine 退出——过期快照不再最后交付（#84）
- fix(registry): watch 重建退避成功即归零——不再 30s 常态延迟（#85）
- fix(distlock): 哨兵跨包 errors.Is 等价 + Unlock 非持有者口径与 redisx 对齐（#86）

关联问题单：etcdx#83、etcdx#84、etcdx#85、etcdx#86。

## [v0.2.7] — 2026-11-08

- build: 依赖地板对齐——本版把 oekit 依赖地板抬到各依赖当前最新已发布版本（matrix-tools/scripts/align_floors.py 维护轮）

## [v0.2.6] — 2026-11-07

- fix(distlock): ttl<=0 前置校验报错、亚秒 TTL 抬 1s 打 Warn——与 redisx「ttl 必须为正」统一口径（#62）
- fix(distlock): WithGracefulDegradation 收窄为「运行期传输层不可达」放行——业务层错误一律报错，与 redisx 口径交叉声明（#63）
- test(registry): MVCC 压缩后 watch 重建回归钉——Compaction 后新变更仍被推送（主体修复已随 #55/PR #60 落地）（#64）
- fix(client): NewClient 浅拷贝 Config——不再原地改写调用方配置（#65）
- fix(distlock): Lock timeout<=0 语义对齐 redisx——单次非阻塞尝试而非零次（#66）
- fix(registry): 同 key 并发 Register 串行化——后到者停掉先到者的 supervise+租约，孤儿租约消灭（#67）

关联问题单：etcdx#62、etcdx#63、etcdx#64、etcdx#65、etcdx#66、etcdx#67。

## [v0.2.5] — 2026-11-06

- fix(distlock): `WithGracefulDegradation` 的降级面收紧到「etcd 不可达」（[#54](https://github.com/oekit/etcdx/issues/54)）——此前 acquire 全链路任何错误都进降级放行：连接预检的 3s 窗口继承调用方 ctx，调用方自己的取消/超时也被当成「etcd 不可达」降级放行——etcd 完全健康、仅 caller 预算 1s 时，TryLock 返回 `degraded:` 成功，多实例同时「拿到锁」并发进临界区，互斥直接失效。修法：三个降级入口（预检/NewSession/do）均补 `ctx.Err() != nil ⇒ 显式报错`，降级只对 etcd 传输层不可用放行
- fix(registry): Subscriber/ConfigStore 的 watch 被取消后指数退避自动重建（[#55](https://github.com/oekit/etcdx/issues/55)）——此前 `wresp.Canceled` 只 Warn 一条就 return：订阅留在 map、Unsubscribe 「正常」返回，但永不再回调——长驻订阅 + auto-compaction（`WithRev(startRev)` 被服务端取消的常态）让实例列表/配置推送几天后无声停更，消费方拿过期快照继续跑。修法：对齐注册侧「丢约指数退避自动重注册」的对等韧性——重建前重读快照拿新 revision（避免重建即 Compacted）+ 立即回调一次当前值，退避上限 30s；Unsubscribe/正常退出不重建

关联问题单：etcdx#54、etcdx#55。

## [v0.2.4] — 2026-11-06

- build: 矩阵依赖地板对齐（维护轮 #233）

## [v0.2.3] — 2026-11-02

- build: 矩阵内依赖地板 `concurrency` v0.23.2 → v0.23.3（维护轮全量对齐；本版不含 API 变更）

## [v0.2.2] — 2026-10-28

- feat(distlock): EtcdLocker 补「最大持锁时长」安全网（#36）——漏调 Unlock 不再永久持锁

`EtcdLocker` 每次加锁新建一个 etcd Session，Session 的 keepalive 会**无限续租**（ctx 只在 `Session.Close/Orphan` 时取消）；而持有记录只在 `Unlock` / `Close` 时清除 ⇒ **调用方漏调 `Unlock`（且进程不退出）⇒ 该锁的租约永不过期 ⇒ 锁被永久持有** ⇒ 集群里其它实例每次都 `ErrLockTimeout`，该业务功能**彻底不可用且不可自愈**（进程重启前不会恢复），同时每次加锁泄漏一个 keepalive goroutine 和一条租约。

这正是 `redisx/distlock` 侧已用 `WithMaxAutoRenew` 显式防掉的那一类事故（那边明写：「忘记 Unlock 时，续期 goroutine 到期后自行退出，锁在 TTL 后自然过期，不会形成永久持锁 + goroutine 泄漏」），而 `etcdx` 的 `Locker` 接口自述「与 redisx/distlock.Locker 同形」却**完全没有等价的安全网**。

修法：新增 `WithMaxHold(d)`（0 = 默认 10×ttl；负值 = 不限制；正值 = 显式上限），到期时的动作用 etcd **`Session.Orphan()`**——它**停止 keepalive 但不撤销租约** ⇒ 租约在 TTL 后自然过期、键随之删除 ⇒ 语义与 redisx 侧「续期到期后锁在 TTL 后自然过期」**一致**（不是立刻释放）。`Unlock` / `Close` 走 `release()` 时停掉该安全网 goroutine（正常释放不该等到期）。

另补注释：etcd 租约以**秒**为单位，亚秒 TTL 会被静默抬成 1s（redisx 侧 `SetNX` 是精确毫秒）⇒ 同一个 `Locker` 接口，200ms 的锁在 redisx 是 200ms、在 etcd 是 1s——这是租约机制的精度限制，不是 bug，但调用方按毫秒理解会误判。

补 3 条用例（跑在仓内自带嵌入式 etcd 上）：**故意不 Unlock** ⇒ 安全网到期后另一个实例能拿到锁；正常 Unlock 后另一个实例立刻能拿到；`WithMaxHold(-1)` 不启动安全网（#36）

## [v0.2.1] — 2026-10-28

- fix(distlock): 本进程持有状态改为「key → ownerID → 条目」两层映射——此前单层映射下，同一 key 的多个持有者（降级放行与真实持锁并存、或上层重复加锁）后写者静默覆盖前一个 ⇒ 前一个的 Session 永不关闭（租约继续续期 ⇒ 锁不释放），且其 Unlock 因 ownerID 不匹配报 ErrNotOwner ⇒ 它的锁也放不掉

## [v0.2.0] — 2026-10-28

- **BREAKING**: fix(etcdx): 构造器不校验 nil client ⇒ 「构造时忘了传 client」被推迟到**第一次实际访问**才爆（`cli.Get`/`cli.Put` 上的空指针崩溃，堆栈里只有 etcd 内部帧，看不出根因，往往是生产环境第一次注册服务时）。`registry` 的四个构造器（`NewRegistrar`/`NewDiscoverer`/`NewSubscriber`/`NewConfigStore`）与 `distlock.NewEtcdLocker` 改为返回 `(*T, error)`，client 为 nil 时返回新增的 `ErrNilClient`——与同 org 的 `registry/nacosimpl` 同口径（构造期 fail-fast）。**下游怎么改**：调用点按 `reg, err := registry.NewRegistrar(cli)` 形态处理 err 即可；org 内零消费方

## [v0.1.7] — 2026-10-23

- build: 矩阵内依赖地板对齐（align_floors 不动点）：concurrency v0.17.0 → v0.20.2、logx v0.7.0 → v0.7.1、registry v0.4.1 → v0.5.0（#23）

## [v0.1.6] — 2026-10-19

- build: 矩阵内依赖地板对齐（align_floors 不动点）：concurrency v0.5.4 → v0.17.0、logx v0.6.0 → v0.7.0

## [v0.1.5] — 2026-10-18

- fix(deps): registry v0.4.1 地板对齐（零代码改动）

## [v0.1.4] — 2026-10-18

- fix(deps): logx 地板 v0.5.0 → v0.6.0、core 地板 v0.6.0 → v0.7.0（零代码改动）

## [v0.1.3] — 2026-10-17

- registry v0.4.0 导入路径升级（SPI 包提升至模块根：`github.com/oekit/registry/registry` → `github.com/oekit/registry`），代码零逻辑变化

## [v0.1.2] — 2026-10-17

- concurrency 地板 v0.5.2 → v0.5.4

## [v0.1.1] — 2026-10-17

- fix(distlock): acquire 前置 3s 连接预检防 NewSession 无界挂死；GoSafe 换已发布变体（GoSafeLabeled 未随 tag 发布）；errcheck 清零
- fix(deps): 内部依赖地板全量对齐最新——concurrency → v0.5.2、logx v0.2.0 → v0.5.0、registry v0.3.0 → v0.3.1（core v0.6.0 indirect 跟随）
- build: go 维持 1.26.0——etcd client v3 生态全家声明 go1.26，暂不做生态级回退

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
  acquire 前置有界连接预检（3s 窗口）——不可达 etcd 直接降级/报错，
  规避 `concurrency.NewSession` 的 Grant 无界等待；`WithGracefulDegradation`
  可选无 etcd 降级放行；`WithOnLock` 指标钩子；接口与 `redisx/distlock.Locker`
  同形可互换
- 测试：内嵌 etcd 服务端（embed，内核随机端口），覆盖注册/发现/订阅推送/
  配置监听/锁互斥与超时/降级全链路，`-race` 全绿；CI 无需 docker
