// Package distlock 提供基于 etcd 的分布式锁，接口形态与
// github.com/oekit/redisx/distlock.Locker 完全一致（TryLock/Lock/Unlock +
// ownerID 语义），Go 结构化类型下两种后端可直接互换，业务代码面向
// Locker 接口编程即可。
//
// # 实现机制
//
//   - 每把锁独占一个 etcd Session（Lease + 自动续约）：进程存活期间锁不会
//     因 TTL 到期丢失；进程崩溃后租约到期自动释放（ttl 即故障释放上限）
//   - 加锁走 clientv3/concurrency.Mutex（revision 公平排队，Lock 阻塞等待
//     前序持有者释放而非轮询）
//   - ownerID 由本进程生成并在 Unlock 时校验，防止错把他人的锁释放；
//     跨进程安全性由 etcd 租约与 Mutex 自身保证
//   - 可选无 etcd 时降级放行（单实例部署场景，默认关闭）
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
