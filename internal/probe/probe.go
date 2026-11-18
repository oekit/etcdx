// Package probe 提供 etcdx 仓内 distlock / registry 两包共用的 etcd 网络
// 交互探活有界窗口——数值单源（distlock#111 立值、registry#196 接入），
// 两包只引用本包，不各自复制 3s 字面量；后续调值只改一处。
//
// 适用形态：调用方 ctx 无 deadline（如 WithCancel(Background) 派生的长驻
// ctx）时的单次网络交互（连接预检 / 租约 Grant / watch 重建的快照重取
// Get）——etcd 半开连接 / 网络黑洞下 gRPC 可能无限挂起且不报错，套本窗口
// 后失败可在有限时间内被观测，交由调用方既有的退避 / 降级路径接管。
//
// 根包 Client.Health 的 healthEndpointWindow（#125）是独立语义的 var
// （per-endpoint 探测预算，var 形态供测试收窄窗口），不并入本单源。
package probe

import "time"

// Window 是单次 etcd 网络交互的探活有界窗口（3s，AGENTS.md「探测类 ≤10s」
// 区间内）。
const Window = 3 * time.Second
