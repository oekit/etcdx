package registry

import (
	"testing"
	"time"
)

// TestBackoffForSequence 钉住 #186：注册侧 reRegister 与 watch 重建收敛为
// 同一退避派生单源——1s 起、30s 封顶，封顶语义为「乘后判 + 显式 min」。
//
// 分叉史：reRegister 旧实现乘前判（`if backoff < 30s { backoff *= 2 }`），
// 第 6 轮等待越顶到 32s；watchBackoff 旧实现乘后判，第 6 轮收敛 30s。统一
// 取后者（封顶后恒 30s 是共同意图），两侧行为序列必须一致。
func TestBackoffForSequence(t *testing.T) {
	want := []time.Duration{
		backoffBase,      // attempts=0
		2 * time.Second,  // attempts=1
		4 * time.Second,  // attempts=2
		8 * time.Second,  // attempts=3
		16 * time.Second, // attempts=4
		30 * time.Second, // attempts=5：1s<<5=32s 越顶，显式 min 收敛 30s（非乘前判的 32s）
		30 * time.Second, // attempts=6
		30 * time.Second, // attempts=100：封顶后恒 30s
	}
	for i, w := range want {
		if got := backoffFor(i); got != w {
			t.Fatalf("backoffFor(%d) = %v, want %v", i, got, w)
		}
	}
	if got := backoffFor(1_000_000); got != backoffMax {
		t.Fatalf("backoffFor(1e6) = %v, want %v（attempts 极大不得移位回绕）", got, backoffMax)
	}
	if got := backoffFor(-1); got != backoffBase {
		t.Fatalf("backoffFor(-1) = %v, want %v（负数按 0 处理，从基数起步）", got, backoffBase)
	}
}
