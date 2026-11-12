package registry

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/oekit/etcdx/internal/testutil"
)

// #147 回归：配置 watch 重建补投递的 Get 失败不得被静默吞掉——重建窗口内的
// 变更必须最终投递（修复后该 Get 与 revision 同源：失败走 resnap 的 err →
// 指数退避重试，与订阅侧单次 Get 同口径）。
//
// 缺陷形态：resnap 拆成两次 Get（KeysOnly 取 revision + 补投递闭包再做完整
// Get），第二次失败被 `gerr == nil` 的空 else 吞掉——重建窗口内的变更无日志、
// 无投递、永久丢失。
//
// 故障注入（grpc 链式拦截器）：armed 期间第一次「非 KeysOnly 且命中配置键」
// 的 Range 失败一次——修复前打中补投递闭包的第二次 Get（被吞 ⇒ v3 永久丢），
// 修复后打中 resnap 的单次 Get（走退避重试 ⇒ v3 最终投递）。注入错误为普通
// error（非 gRPC status），etcd 重试拦截器的 isSafeRetryImmutableRPC 不重试
// （status.FromError 非 status 错误即 false），保证恰好失败一次。
func TestListenConfigRebuildGetFailureIsNotSilentlySwallowed(t *testing.T) {
	s := testutil.NewServer(t)
	ctx := context.Background()
	const prefix = "/t147"
	cfgKey := configKey(prefix, "DEFAULT_GROUP", "app.json")

	var armed atomic.Bool
	var injected atomic.Int32
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.ClientURL()},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if method == "/etcdserverpb.KV/Range" && armed.Load() {
					if r, ok := req.(*etcdserverpb.RangeRequest); ok && !r.KeysOnly && bytes.Equal(r.Key, []byte(cfgKey)) {
						if armed.CompareAndSwap(true, false) {
							injected.Add(1)
							return errors.New("injected config Get failure (#147 probe)")
						}
					}
				}
				return invoker(ctx, method, req, reply, cc, opts...)
			}),
		},
	})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	cs, err := NewConfigStore(cli, WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewConfigStore: %v", err)
	}

	var mu sync.Mutex
	var got []string
	if err := cs.ListenConfig(ctx, "app.json", "", func(data string) {
		mu.Lock()
		got = append(got, data)
		mu.Unlock()
	}); err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}
	if err := cs.PutConfig(ctx, "app.json", "", "v1"); err != nil {
		t.Fatalf("PutConfig v1: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) > 0 && got[len(got)-1] == "v1"
	})

	// 注入下一次完整 Get 失败一次，随后触发 watch 重建（压缩 + 重启）
	armed.Store(true)
	forceWatchCancel(t, s, prefix+"/marker", 0)

	// 变更落在重建窗口（旧 watch 已死、重建未完成）：其投递只能依靠重建
	// 补投递的 Get——注入失败被吞时该变更永久丢失
	if err := cs.PutConfig(ctx, "app.json", "", "v3"); err != nil {
		t.Fatalf("PutConfig v3: %v", err)
	}

	waitFor(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, v := range got {
			if v == "v3" {
				return true
			}
		}
		return false
	})
	if injected.Load() == 0 {
		t.Fatal("故障注入未命中补投递/resnap 的完整 Get（探针失效，测试无结论）")
	}

	// 重建出的新 watch 仍然存活：后续变更继续推送
	if err := cs.PutConfig(ctx, "app.json", "", "v4"); err != nil {
		t.Fatalf("PutConfig v4: %v", err)
	}
	waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) > 0 && got[len(got)-1] == "v4"
	})

	_ = cs.Close(ctx)
}
