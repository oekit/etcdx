// Package keyseg 提供 etcdx 仓内 registry / distlock 两包共用的键路径段
// 层级边界校验——规则单源（registry#110 立约，distlock#138 补齐），两包
// 只引用本包，不各自复制规则文档。
//
// etcd 键空间按 "/" 分层（前缀扫描 / 范围查询按 "/" 边界命中），构成键
// 路径的段（service / group / dataID / ip / 锁键）不得含 "/"：
//
//   - registry 侧：路径段含 "/" 时与分隔符不可区分——service "order/v2" 的
//     实例键落在 "order/" 前缀之下，Discover("order") 的前缀扫描连带返回
//     order/v2 的实例并解码成 Service="order"（流量被路由到另一个服务，
//     全程无报错）；configKey 同构，group "g/a"+dataID "b" 与 group
//     "g"+dataID "a/b" 拼出同一个键（跨 group 串扰）。
//   - distlock 侧：锁键直接拼进 etcd 分层键空间，concurrency.Mutex 的
//     互斥判定用前缀范围查询（WithFirstCreate）——锁键 "a/b" 的 owner 键
//     <pfx>/a/b/<lease> 落在锁 "a" 的互斥前缀 <pfx>/a/ 之内，持有 "a/b"
//     时 "a" 永远拿不到锁（单向过度互斥）；反方向（子键被祖先挡）不成立。
//
// 两包入口 fail-fast（注册/发现/订阅/加锁），把键层级边界变成不变量；
// 键布局本身不变，既有干净键完全兼容。
package keyseg

import (
	"fmt"
	"strings"
)

// Check 报告 name 作为单个键路径段是否越出层级边界（含 "/"）。
// kind 仅用于错误文案定位（如 "service" / "group" / "lock key"）。
func Check(kind, name string) error {
	if strings.Contains(name, "/") {
		return fmt.Errorf("%s %q must not contain %q (key hierarchy boundary, registry#110/distlock#138)", kind, name, "/")
	}
	return nil
}
