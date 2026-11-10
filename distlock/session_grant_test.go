package distlock

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// #111 回归：加锁链路建 Session 的 Grant 无界等待——连接预检通过后 etcd 才
// 进入黑洞（网络分区丢包/节点宕机切换/iptables DROP）时，Lock 永久挂死：
// caller ctx/timeout 不被感知，WithGracefulDegradation 的降级面永不触发
// （根本没有错误返回）。「探测过、Grant 前黑洞」是窄窗口竞态，真实 etcd 的
// 集成测试结构性测不到——用桩件把窗口钉死：KV 探活恒成功（探测时活着），
// Lease 的 Grant 只在有界 ctx 下返回（黑洞里真实 gRPC 调用等 TCP 重传耗尽，
// deadline 是唯一出口；ctx 无界 = 永久挂起，正是缺陷形态）。

// probeOKKV 连接预检探活恒成功：模拟「探测时 etcd 还活着」。
type probeOKKV struct{ clientv3.KV }

func (probeOKKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{}, nil
}

// blackHoleLease Grant 阶段黑洞：请求既不成功也不失败，只跟随调用方给定的
// ctx 返回。
type blackHoleLease struct{ clientv3.Lease }

func (blackHoleLease) Grant(ctx context.Context, _ int64) (*clientv3.LeaseGrantResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestLockBoundedWhenEtcdBlackholesAfterProbe(t *testing.T) {
	newBlackHoleClient := func(t *testing.T) *clientv3.Client {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel) // 退出时解除桩件里的永久挂起（pre-fix 挂死的 goroutine 收尾）
		cli := clientv3.NewCtxClient(ctx)
		cli.KV = probeOKKV{}
		cli.Lease = blackHoleLease{}
		return cli
	}

	type lockResult struct {
		owner string
		err   error
	}
	waitBounded := func(t *testing.T, done <-chan lockResult, what string) lockResult {
		t.Helper()
		select {
		case r := <-done:
			return r
		case <-time.After(10 * time.Second):
			t.Fatalf("%s 在预检通过后的黑洞场景下 10s 未返回：建 Session 的 Grant 仍无界（#111）", what)
			return lockResult{}
		}
	}

	t.Run("graceful degradation", func(t *testing.T) {
		lax, err := NewEtcdLocker(newBlackHoleClient(t), WithGracefulDegradation())
		if err != nil {
			t.Fatalf("NewEtcdLocker: %v", err)
		}
		done := make(chan lockResult, 1)
		async.GoSafe(func() {
			owner, err := lax.Lock(context.Background(), "k", time.Second, 5*time.Second)
			done <- lockResult{owner, err}
		})
		r := waitBounded(t, done, "WithGracefulDegradation 的 Lock")
		if r.err != nil {
			t.Fatalf("Lock err = %v; want 降级放行（黑洞属 etcd 传输层不可达面，#63）", r.err)
		}
		if !strings.HasPrefix(r.owner, "degraded:") {
			t.Fatalf("owner = %q; want \"degraded:\" 前缀", r.owner)
		}
		if err := lax.Unlock(context.Background(), "k", r.owner); err != nil {
			t.Fatalf("degraded Unlock: %v", err)
		}
	})

	t.Run("strict explicit error", func(t *testing.T) {
		strict, err := NewEtcdLocker(newBlackHoleClient(t))
		if err != nil {
			t.Fatalf("NewEtcdLocker: %v", err)
		}
		done := make(chan lockResult, 1)
		async.GoSafe(func() {
			owner, err := strict.Lock(context.Background(), "k", time.Second, 5*time.Second)
			done <- lockResult{owner, err}
		})
		r := waitBounded(t, done, "严格模式 Lock")
		if r.err == nil {
			t.Fatalf("strict Lock = (%q, nil); want 显式传输错误而非挂死", r.owner)
		}
		if strings.HasPrefix(r.owner, "degraded:") {
			t.Fatalf("未开启降级却返回 degraded owner %q", r.owner)
		}
	})

	t.Run("caller ctx cancel propagates into grant", func(t *testing.T) {
		strict, err := NewEtcdLocker(newBlackHoleClient(t))
		if err != nil {
			t.Fatalf("NewEtcdLocker: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan lockResult, 1)
		start := time.Now()
		async.GoSafe(func() {
			owner, err := strict.Lock(ctx, "k", time.Second, 5*time.Second)
			done <- lockResult{owner, err}
		})
		time.Sleep(300 * time.Millisecond)
		cancel()
		r := waitBounded(t, done, "caller ctx 取消后的 Lock")
		if elapsed := time.Since(start); elapsed > 3*time.Second {
			t.Fatalf("Lock 在 caller 取消后 %v 才返回；want 跟随 caller ctx 即刻返回（#111）", elapsed)
		}
		// #54 同口径：caller ctx 自身取消 ≠ etcd 不可达，显式报错不降级
		if r.err == nil || !strings.Contains(r.err.Error(), "caller context done") {
			t.Fatalf("err = %v; want caller context done while granting lease", r.err)
		}
	})
}
