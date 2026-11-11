// Package distlock 提供基于 etcd 的分布式锁，实现
// github.com/oekit/pkg/distlock.Locker 后端中立契约（TryLock/Lock/Unlock +
// ownerID 语义，#123 契约单源），与 redisx/distlock 后端（迁移后）同类型
// 可互换，业务代码面向 Locker 接口编程即可。
//
// # 跨后端错误面（#123 单源后）
//
// 错误哨兵即 pkg/distlock 的契约哨兵（同值别名）：ErrLockNotAcquired /
// ErrLockTimeout / ErrLockNotHeld（历史名 [ErrNotOwner]）——同一进程只此
// 一套值，适配层绑哪一侧哨兵做 errors.Is 分类都不漂移（429 不再变 500），
// 历史 #86/#112 的 sentinel.Is 同文案等价与 IsOneOf 双向判定补丁已随契约
// 单源删除。文案映射注意：ErrNotOwner 历史文案是 "lock not owned by this
// owner"，契约单源后统一为 pkg 哨兵文案 "lock not held by this owner"
// （对外文案变化，见 changelog）。Unlock 对非持有者显式报 ErrLockNotHeld
// （redisx 侧同样在其 #115 对齐为显式报错），两侧不再一边静默一边报错。
// 机制级差异（亚秒 TTL 精度、降级放行面）见 Locker 类型注释的统一口径。
//
// # 实现机制
//
//   - 每把锁独占一个 etcd Session（Lease + 自动续约）：进程存活期间锁不会
//     因 TTL 到期丢失；进程崩溃后租约到期自动释放（ttl 即故障释放上限）
//   - 加锁走 clientv3/concurrency.Mutex（revision 公平排队，Lock 阻塞等待
//     前序持有者释放而非轮询）
//   - ownerID 由本进程生成并在 Unlock 时校验，防止错把他人的锁释放；
//     跨进程安全性由 etcd 租约与 Mutex 自身保证
//   - 可选「etcd 传输层不可达」时降级放行（单实例部署场景，默认关闭；
//     权限/参数等业务层错误不降级，见 WithGracefulDegradation）
//
// # 快速接入
//
//	locker, err := distlock.NewEtcdLocker(cli, distlock.WithPrefix("myapp"))
//	if err != nil {
//	    return err
//	}
//	err = distlock.WithLock(ctx, locker, "deploy:myapp:dev", 30*time.Second, 10*time.Second, func() error {
//	    return doDeploy()
//	})
//
// 手动加解锁：
//
//	owner, err := locker.TryLock(ctx, "job:cleanup", 30*time.Second)
//	if errors.Is(err, distlock.ErrLockNotAcquired) { return } // 别人在跑
//	defer locker.Unlock(ctx, "job:cleanup", owner)
package distlock
