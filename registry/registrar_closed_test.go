package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	regcore "github.com/oekit/registry"

	"github.com/oekit/etcdx/internal/testutil"
)

// #131 回归：Registrar 关闭态错误必须是哨兵 ErrRegistrarClosed——「此后
// Register 永远失败」是明确的终态类别，此前只能 strings.Contains 文案
// 匹配分流「停止重试」与「瞬时失败」（关闭竞态/优雅停机场景），与同包
// ErrNilClient 的哨兵范式不一致；两处重复文案今后还会漂移。
func TestRegisterAfterCloseReturnsSentinel(t *testing.T) {
	cli := testutil.NewClient(t)
	ctx := context.Background()
	reg, err := NewRegistrar(cli, WithPrefix("/t131"), WithTTL(5*time.Second))
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	inst := regcore.Instance{Service: "svc", IP: "10.0.0.1", Port: 8080}
	if err := reg.Register(ctx, inst); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := reg.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = reg.Register(ctx, inst)
	if !errors.Is(err, ErrRegistrarClosed) {
		t.Fatalf("Register after Close err = %v; want ErrRegistrarClosed（#131）", err)
	}
	// 文案保持既有形态：既有按字符串匹配的调用方不破兼容（哨兵是加法）
	if got := err.Error(); got != "etcdx registry: registrar closed" {
		t.Fatalf("err.Error() = %q; want 既有文案不变（兼容字符串匹配调用方）", got)
	}
}
