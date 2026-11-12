package etcdx

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/oekit/etcdx/internal/testutil"
)

// blackHoleListener 接受连接但永不响应：gRPC 建连后 HTTP/2 握手等不到
// 服务端 SETTINGS 帧，Status 调用一直挂起直到 ctx deadline——确定性黑洞
// endpoint，不依赖不可路由 IP 的网络环境。
func blackHoleListener(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// 连接经 conns 通道移交，持有期间不读不写即黑洞语义；超缓存兜底直接
	// 关闭（不影响黑洞语义——黑洞靠「已建连接永不响应」生效）。
	conns := make(chan net.Conn, 64)
	acceptedDone := make(chan struct{})
	async.GoSafe(func() {
		defer close(conns)
		defer close(acceptedDone)
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			select {
			case conns <- c:
			default:
				_ = c.Close()
			}
		}
	})
	// 单一 cleanup 保证顺序：先关 listener（Accept 出错 → accept goroutine
	// 退出 → conns 关闭），再收尸全部连接。
	t.Cleanup(func() {
		_ = l.Close()
		<-acceptedDone
		for c := range conns {
			_ = c.Close()
		}
	})
	return l.Addr().String()
}

// #125 回归：Health 的探测预算按 endpoint 独立。缺陷形态：全部 endpoint
// 共享一个 3s 窗口，首个黑洞 endpoint 吃光预算，其余 endpoint 在剩余≈0 的
// 预算内全部瞬时失败，健康判定退化为「第一个 endpoint 的状态」。
// 修复形态：每 endpoint 一个独立窗口 + 全序列总预算兜底。
func TestHealthPerEndpointBudget(t *testing.T) {
	oldWindow, oldTotal := healthEndpointWindow, healthTotalBudget
	healthEndpointWindow, healthTotalBudget = 300*time.Millisecond, 2*time.Second
	t.Cleanup(func() { healthEndpointWindow, healthTotalBudget = oldWindow, oldTotal })

	blackhole := blackHoleListener(t)
	healthy := testutil.NewServer(t).Client().Endpoints()[0]

	c, err := NewClient(&clientv3.Config{Endpoints: []string{blackhole, healthy}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	start := time.Now()
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health = %v; want nil——首个黑洞 endpoint 只应吃掉自己的窗口，"+
			"不得吃光其余 endpoint 的预算（#125）", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("Health 耗时 %v ≥ 总预算 2s；独立窗口下黑洞 endpoint 的消耗应有界（#125）", elapsed)
	}
}

// #125 总预算兜底：全部 endpoint 都黑洞时，Health 在总预算内有界返回错误，
// 不得按「N 个 endpoint × 各自窗口」无界拉长。
func TestHealthTotalBudgetBounds(t *testing.T) {
	oldWindow, oldTotal := healthEndpointWindow, healthTotalBudget
	healthEndpointWindow, healthTotalBudget = 400*time.Millisecond, 700*time.Millisecond
	t.Cleanup(func() { healthEndpointWindow, healthTotalBudget = oldWindow, oldTotal })

	bh1 := blackHoleListener(t)
	bh2 := blackHoleListener(t)

	c, err := NewClient(&clientv3.Config{Endpoints: []string{bh1, bh2}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	start := time.Now()
	err = c.Health(context.Background())
	if err == nil {
		t.Fatal("Health = nil; want error（全部 endpoint 黑洞时必须报错）")
	}
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("Health 耗时 %v；应被总预算 700ms 兜底而非 N×窗口无界拉长（#125）", elapsed)
	}
}
