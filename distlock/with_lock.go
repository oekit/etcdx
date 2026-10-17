package distlock

import (
	"context"
	"time"
)

// WithLock 加锁 → 执行 fn → 自动释放的便捷包装。
// 获取锁失败（含 ErrLockTimeout）直接返回错误，fn 不执行；
// 释放失败只记日志不覆盖 fn 的返回值（租约到期兜底释放）。
func WithLock(ctx context.Context, locker Locker, key string, ttl, timeout time.Duration, fn func() error) error {
	ownerID, err := locker.Lock(ctx, key, ttl, timeout)
	if err != nil {
		return err
	}
	defer func() {
		// 释放用独立短超时 ctx：业务 ctx 可能已取消，不能因此泄漏锁到租约到期
		uctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if uerr := locker.Unlock(uctx, key, ownerID); uerr != nil {
			log.Warn("unlock failed, lease expiry will release", "key", key, "err", uerr)
		}
	}()
	return fn()
}
