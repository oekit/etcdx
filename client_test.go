package etcdx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oekit/etcdx/internal/testutil"
)

func TestNewClientValidation(t *testing.T) {
	if _, err := NewClient(nil); err == nil {
		t.Fatal("nil config should fail")
	}
	if _, err := NewClient(&clientv3.Config{}); err == nil {
		t.Fatal("empty endpoints should fail")
	}
}

func TestClientHealthAndClose(t *testing.T) {
	raw := testutil.NewClient(t)
	c, err := NewClient(&clientv3.Config{Endpoints: raw.Endpoints()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx := context.Background()
	if err := c.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
	}
	if _, err := c.Put(ctx, "/etcdx/test", "v"); err != nil {
		t.Fatalf("Put via embedded client: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatalf("second Close should be idempotent: %v", err)
	}
}

// TestNewClientDoesNotMutateCallerConfig 钉住 #65：NewClient 的零值默认
// （DialTimeout 0 → 5s）只能在拷贝上应用，不得原地改写调用方传入的
// clientv3.Config——调用方复用同一结构体建多个客户端/记录配置时，字段被
// 悄悄改掉是未声明的副作用。
func TestNewClientDoesNotMutateCallerConfig(t *testing.T) {
	raw := testutil.NewClient(t)
	cfg := &clientv3.Config{Endpoints: raw.Endpoints()} // DialTimeout 零值
	c, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close(context.Background()) }()
	if cfg.DialTimeout != 0 {
		t.Fatalf("caller config DialTimeout mutated to %v; want untouched 0", cfg.DialTimeout)
	}
	// 默认值确实生效：内部客户端按 5s DialTimeout 拨号成功并可正常工作
	ctx := context.Background()
	if _, err := c.Put(ctx, "/etcdx/no-mutate", "v"); err != nil {
		t.Fatalf("Put via client built from copied config: %v", err)
	}
}

func TestNewClientFromConfig(t *testing.T) {
	raw := testutil.NewClient(t)
	ctx := context.Background()
	c, err := NewClientFromConfig(ctx, EtcdConfig{Endpoints: raw.Endpoints(), DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("NewClientFromConfig: %v", err)
	}
	_ = c.Close(ctx)

	// 不可达端点：拨号超时后 Health 失败，client 被回收
	bad, err := NewClientFromConfig(ctx, EtcdConfig{Endpoints: []string{"127.0.0.1:1"}, DialTimeout: 300 * time.Millisecond})
	if err == nil {
		_ = bad.Close(ctx)
		t.Fatal("unreachable endpoint should fail health check")
	}
}

// TestHealthOrCleanup_SurfacesCloseError 回归（#184）：探活失败回收路径的
// Close 错误必须可观测。
//
// 修复前（`_ = c.Close(ctx)`）：gRPC 连接已建立而 Health 失败时，连接回收
// 失败（半开连接、fd 回收失败）被 `_ =` 整个吞掉，泄漏零信号。修复后
// errors.Join 聚合上抛：Health 错误与 Close 错误均可被 errors.Is/As 判定。
//
// 注入方式：clientv3.NewCtxClient 构造无 gRPC 连接的客户端——零 endpoint
// ⇒ Health 必失败（"no endpoints"，不依赖网络、确定性）；无连接时 clientv3
// Close 先 cancel 自身 ctx、返回 ctx.Err() ⇒ 必失败（预取消底层 ctx 只是让
// 失败来源更显式）。
func TestHealthOrCleanup_SurfacesCloseError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &Client{Client: clientv3.NewCtxClient(ctx)}

	err := healthOrCleanup(context.Background(), c)
	if err == nil {
		t.Fatal("Health 失败必须返回 error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Close 错误必须可观测（#184，修复前被 _ = 吞掉），got %v", err)
	}
	for _, want := range []string{"etcdx: ping failed", "etcdx: close failed", "no endpoints", "context canceled"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("聚合错误消息缺段 %q，got %q", want, err.Error())
		}
	}
}

// TestHealthOrCleanup_HealthErrorOnlyWhenCloseOK 回收成功时不引入噪声错误：
// 返回口径与历史一致（仅 Health 错误、%w 包装、消息不变）。
//
// 注入方式：真实 gRPC 客户端指向不可达端点（连接拒绝，快速确定性）⇒ Health
// 失败而 Close 成功（真实连接的 conn.Close() 返回 nil）。conn-less 的
// NewCtxClient 无法表达本分支——clientv3 Close 先 cancel 自身 ctx、无连接时
// 返回 ctx.Err()，恒失败。
func TestHealthOrCleanup_HealthErrorOnlyWhenCloseOK(t *testing.T) {
	raw, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:1"},
		DialTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("clientv3.New: %v", err)
	}
	c := &Client{Client: raw}

	err = healthOrCleanup(context.Background(), c)
	if err == nil {
		t.Fatal("Health 失败必须返回 error")
	}
	if want := "etcdx: ping failed"; !strings.Contains(err.Error(), want) {
		t.Fatalf("error message = %q, want contains %q", err.Error(), want)
	}
	if strings.Contains(err.Error(), "close failed") {
		t.Fatalf("Close 成功时不得引入 close 错误，got %q", err.Error())
	}
}

// TestHealthOrCleanup_OK Health 成功路径零副作用：不关 client（后续正常使用）。
func TestHealthOrCleanup_OK(t *testing.T) {
	c := &Client{Client: testutil.NewClient(t)}
	ctx := context.Background()

	if err := healthOrCleanup(ctx, c); err != nil {
		t.Fatalf("Health 成功应返回 nil, got %v", err)
	}
	// 未被回收：底层客户端仍可用（若被误关，Put 会立刻报连接关闭类错误）
	if _, err := c.Put(ctx, "/etcdx/health-or-cleanup", "v"); err != nil {
		t.Fatalf("Health 成功不得关闭 client，Put: %v", err)
	}
}
