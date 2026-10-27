package distlock

import (
	"errors"
	"testing"
)

// TestNewEtcdLocker_NilClient 回归测试：构造器收到 nil client 时必须返回
// ErrNilClient，**不得**返回一个「看起来能用」的实例。
//
// 缺陷成因：构造器不校验 cli ⇒ 构造成功、错误推迟到第一次加锁的连接预检
// （cli.Get）才爆——那时是空指针崩溃，且堆栈里只有 etcd 内部帧，看不出
// 「是构造时没传 client」。
func TestNewEtcdLocker_NilClient(t *testing.T) {
	locker, err := NewEtcdLocker(nil)
	if !errors.Is(err, ErrNilClient) {
		t.Errorf("NewEtcdLocker(nil) = (%v, %v)，期望 ErrNilClient", locker, err)
	}
	if locker != nil {
		t.Errorf("NewEtcdLocker(nil) 应返回 nil 实例，实得 %v", locker)
	}
}
