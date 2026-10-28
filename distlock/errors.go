package distlock

import (
	"errors"
)

// ErrNilClient 表示构造时传入了 nil 的 etcd client。
//
// 构造期 fail-fast：client 为 nil 时，首次加锁会在连接预检的 `cli.Get` 上
// 空指针崩溃——错误发生在**离构造点很远**的地方（往往是生产环境第一次抢锁
// 时），堆栈里只有 etcd 内部帧，看不出「是构造时没传 client」。构造期报错
// 把这道接线错误挡在启动阶段。
//
// 与 registry/nacosimpl 的 ErrNilClient 同口径。
var ErrNilClient = errors.New("etcdx/distlock: client must be non-nil")
