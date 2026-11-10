// Package distlock 提供基于 etcd 的分布式锁，方法集与
// github.com/oekit/redisx/distlock.Locker 完全一致（TryLock/Lock/Unlock +
// ownerID 语义），Go 结构化类型下两种后端可直接互换，业务代码面向
// Locker 接口编程即可。
//
// # 跨后端错误面（#86）
//
// 错误哨兵跨后端 errors.Is **单向**等价（#86，方向修正 #112）：本包哨兵对
// **同文案**错误等价判定，errors.Is(etcdx.ErrLockTimeout, redisx.ErrLockTimeout)
// 为 true；反向 errors.Is(redisx 哨兵, etcdx 哨兵) 恒 false（stdlib errors.Is
// 只查错误链一侧的 Is，redisx 侧 errors.New 值无 Is 方法）。适配层把 target
// 绑在 **redisx 侧哨兵**上即可两后端通用，「锁被占用」「获取超时」的错误
// 分类不漂移；绑 etcdx 哨兵跑在 redisx 后端会误分类。Unlock 对非持有者
// 显式报 ErrNotOwner（redisx 侧同样在其 #115 对齐为显式报错），两侧不再
// 一边静默一边报错。机制级差异（亚秒 TTL 精度、降级放行面）见 Locker
// 接口注释的统一口径。
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
