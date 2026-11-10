package distlock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/oekit/etcdx/internal/testutil"
)

// leaseGrantMethod 是 etcd LeaseGrant 的 gRPC 全限定方法名。
const leaseGrantMethod = "/etcdserverpb.Lease/LeaseGrant"

// newBlackHoleClient 在健康 etcd 前构造一个「只有 LeaseGrant 黑洞」的客户端：
// unary 拦截器放行一切 RPC，唯独把 LeaseGrant 悬到 block 关闭或调用方 ctx
// 结束——精确模拟「连接预检成功之后、Grant 之前 etcd 进入黑洞」的窄窗口
// （网络分区丢包/iptables DROP，#111）。
func newBlackHoleClient(t *testing.T, endpoints []string, block <-chan struct{}) *clientv3.Client {
	t.Helper()
	intercept := func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if method == leaseGrantMethod {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-block:
				return status.Error(codes.Unavailable, "lease grant black-holed")
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithChainUnaryInterceptor(intercept)},
	})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// waitLock 在独立 goroutine 里执行加锁并要求在 budget 内返回；超时即红
// （加锁链路存在无界挂死，#111）。
func waitLock(t *testing.T, budget time.Duration, f func() (string, error)) (string, error) {
	t.Helper()
	type lockResult struct {
		owner string
		err   error
	}
	done := make(chan lockResult, 1)
	go func() {
		owner, ferr := f()
		done <- lockResult{owner: owner, err: ferr}
	}()
	select {
	case r := <-done:
		return r.owner, r.err
	case <-time.After(budget):
		t.Fatalf("加锁在 %v 内未返回——建 Session 的 Grant 无界挂死（#111）", budget)
		return "", nil
	}
}

// TestLockGrantBounded_BlackHole 钉住 #111：探测通过后 etcd 在 Grant 前
// 进入黑洞，Lock 必须在有界窗口内返回——caller 的 timeout 参数、caller ctx
// 取消、WithGracefulDegradation 的降级面全部不得失效。修法：显式带 deadline
// 的 Grant（与预检同窗 3s）+ concurrency.WithLease 组装 Session，创建全程
// 有界，失败走既有 degradeOrFail 分类。
func TestLockGrantBounded_BlackHole(t *testing.T) {
	srv := testutil.NewServer(t)
	endpoints := srv.Client().Endpoints()

	t.Run("strict 模式有界报错", func(t *testing.T) {
		block := make(chan struct{})
		cli := newBlackHoleClient(t, endpoints, block)
		t.Cleanup(func() { close(block) })
		l, err := NewEtcdLocker(cli)
		if err != nil {
			t.Fatalf("NewEtcdLocker: %v", err)
		}
		// Lock 声明 timeout=1s；Grant 黑洞时也不得无限挂死
		owner, lerr := waitLock(t, 8*time.Second, func() (string, error) {
			return l.Lock(context.Background(), "k", 5*time.Second, time.Second)
		})
		if lerr == nil || owner != "" {
			t.Fatalf("Grant 黑洞下 Lock = %q, %v; want 有界报错（strict 模式不放行）", owner, lerr)
		}
	})

	t.Run("caller ctx 取消被感知", func(t *testing.T) {
		block := make(chan struct{})
		cli := newBlackHoleClient(t, endpoints, block)
		t.Cleanup(func() { close(block) })
		l, err := NewEtcdLocker(cli)
		if err != nil {
			t.Fatalf("NewEtcdLocker: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(300 * time.Millisecond)
			cancel()
		}()
		owner, lerr := waitLock(t, 5*time.Second, func() (string, error) {
			return l.Lock(ctx, "k", 5*time.Second, 5*time.Second)
		})
		if lerr == nil || owner != "" {
			t.Fatalf("caller ctx 取消后 Lock = %q, %v; want 报错", owner, lerr)
		}
		if !errors.Is(lerr, context.Canceled) {
			t.Fatalf("err = %v; want 携带 context.Canceled（caller ctx 取消必须被感知，#111）", lerr)
		}
	})

	t.Run("优雅降级面正常触发", func(t *testing.T) {
		block := make(chan struct{})
		cli := newBlackHoleClient(t, endpoints, block)
		t.Cleanup(func() { close(block) })
		l, err := NewEtcdLocker(cli, WithGracefulDegradation())
		if err != nil {
			t.Fatalf("NewEtcdLocker: %v", err)
		}
		owner, lerr := waitLock(t, 8*time.Second, func() (string, error) {
			return l.Lock(context.Background(), "k", 5*time.Second, time.Second)
		})
		if lerr != nil || !strings.HasPrefix(owner, "degraded:") {
			t.Fatalf("Grant 黑洞下降级 Lock = %q, %v; want degraded: 前缀放行（#111：降级面不得因挂死永不触发）", owner, lerr)
		}
		if uerr := l.Unlock(context.Background(), "k", owner); uerr != nil {
			t.Fatalf("Unlock 降级锁: %v", uerr)
		}
	})
}
