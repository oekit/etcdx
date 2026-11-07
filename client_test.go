package etcdx

import (
	"context"
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
