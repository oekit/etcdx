package registry

import (
	"errors"
	"testing"
)

// TestConstructors_NilClient 回归测试：构造器收到 nil client 时必须返回
// ErrNilClient，**不得**返回一个「看起来能用」的实例。
//
// 缺陷成因：五个构造器都不校验 cli ⇒ 构造成功、错误推迟到第一次实际访问
// （cli.Get / cli.Put）才爆——那时是空指针崩溃，且堆栈里只有 etcd 内部帧，
// 完全看不出「是构造时没传 client」。错误发生点离构造点很远（往往是生产
// 环境第一次注册服务时）。
//
// 与 registry/nacosimpl 的 ErrNilClient 同口径（那边早就是构造期报错）。
func TestConstructors_NilClient(t *testing.T) {
	if _, err := NewRegistrar(nil); !errors.Is(err, ErrNilClient) {
		t.Errorf("NewRegistrar(nil) = %v，期望 ErrNilClient", err)
	}
	if _, err := NewDiscoverer(nil); !errors.Is(err, ErrNilClient) {
		t.Errorf("NewDiscoverer(nil) = %v，期望 ErrNilClient", err)
	}
	if _, err := NewSubscriber(nil); !errors.Is(err, ErrNilClient) {
		t.Errorf("NewSubscriber(nil) = %v，期望 ErrNilClient", err)
	}
	if _, err := NewConfigStore(nil); !errors.Is(err, ErrNilClient) {
		t.Errorf("NewConfigStore(nil) = %v，期望 ErrNilClient", err)
	}
}
