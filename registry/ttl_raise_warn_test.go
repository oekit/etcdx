package registry

import (
	"context"
	"testing"
	"time"

	"github.com/oekit/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// #203 回归：options 归一处（applyOptions）的亚秒 TTL 钳值不得静默——对齐
// 同仓 distlock 亚秒锁 TTL 抬升的约定（#62 立约「抬升时至少打 Warn 明示，
// 不再静默」）。etcd 租约以秒为单位，(0, 1s) 的 TTL 钳到 1s 是租约精度的
// 机制性限制而非 bug，但调用方按毫秒理解会误判（同一 TTL 在 redisx 侧精确
// 生效），钳值时打 Warn 明示。applyOptions 仅在构造时各跑一次，每条钳值
// 至多一条 Warn，不重复刷屏。
func TestSubSecondTTLClampWarns(t *testing.T) {
	old := logx.Default()
	t.Cleanup(func() { logx.SetDefault(old) })

	// 正常 TTL（≥1s）：不触发钳值 Warn
	capture := newCaptureLogger()
	logx.SetDefault(capture)
	if _, err := NewRegistrar(clientv3.NewCtxClient(context.Background()), WithTTL(5*time.Second)); err != nil {
		t.Fatalf("NewRegistrar(WithTTL(5s)): %v", err)
	}
	if _, ok := capture.find(logx.LevelWarn, clampWarnMsg); ok {
		t.Fatalf("TTL ≥ 1s 不应触发钳值 Warn；捕获记录: %v", capture.snapshot())
	}

	// 亚秒 / 零值 TTL：钳到 1s 且 Warn 留痕（带原始 ttl 字段）
	for _, ttl := range []time.Duration{500 * time.Millisecond, 0} {
		capture := newCaptureLogger() // 每个用例独立捕获，Warn 记录互不串扰
		logx.SetDefault(capture)
		reg, err := NewRegistrar(clientv3.NewCtxClient(context.Background()), WithTTL(ttl))
		if err != nil {
			t.Fatalf("NewRegistrar(WithTTL(%v)): %v", ttl, err)
		}
		if reg.opt.TTL != time.Second {
			t.Fatalf("TTL %v 钳值后 = %v; want 1s", ttl, reg.opt.TTL)
		}
		rec, ok := capture.find(logx.LevelWarn, clampWarnMsg)
		if !ok {
			t.Fatalf("TTL %v 钳值未打 Warn（#203 不得静默）；捕获记录: %v", ttl, capture.snapshot())
		}
		if v, _ := rec.attr("ttl"); v != ttl {
			t.Fatalf("Warn 的 ttl 字段 = %v; want 原始值 %v", v, ttl)
		}
	}
}
