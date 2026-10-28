package registry

import (
	"errors"
)

// ErrNilClient 表示构造时传入了 nil 的 etcd client。
//
// 构造期 fail-fast：client 为 nil 时，任何一次实际访问（Register / Discover /
// Subscribe / PutConfig……）都会在 `cli.Get`/`cli.Put` 上空指针崩溃——错误发生在
// **离构造点很远**的地方（往往是生产环境第一次注册服务时），堆栈里只有 etcd
// 内部帧，看不出「是构造时没传 client」。构造期报错把这道接线错误挡在启动
// 阶段。
//
// 与 registry/nacosimpl 的 ErrNilClient 同口径。
var ErrNilClient = errors.New("etcdx/registry: client must be non-nil")
