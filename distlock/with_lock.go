package distlock

import (
	"context"
	"time"

	pkgdistlock "github.com/oekit/pkg/distlock"
)

// WithLock 加锁 → 执行 fn → 自动释放的便捷包装（#123 契约单源：薄委托
// github.com/oekit/pkg/distlock.WithLock，本包不再自持一份实现）。
//
// 获取锁失败（含 ErrLockTimeout）直接返回错误、fn 不执行；错误统一带
// "lock <key>:" 前缀包装、哨兵经 %w 穿透（errors.Is 判定不丢）——与
// redisx 侧口径一致（此前本包无包装，跨后端错误形态漂移）。
//
// 释放纪律统一为 pkg 单源的 [pkgdistlock.ReleaseUncancelable] 口径
// （context.WithoutCancel）：**持锁期 caller ctx 取消不吞释放动作**——
// 解锁用与 caller 取消状态解耦、且保留 ctx 取值的 context。与历史实现
// 的差异（changelog BREAKING 注记）：此前是 WithTimeout(Background, 3s)
// 的独立短超时 ctx（不携带 caller ctx 取值，3s 为魔法常数）。
// 释放失败只记日志不覆盖 fn 的返回值（租约到期兜底释放）。
func WithLock(ctx context.Context, locker Locker, key string, ttl, timeout time.Duration, fn func() error) error {
	return pkgdistlock.WithLock(ctx, locker, key, ttl, timeout, fn)
}
