package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
	regcore "github.com/oekit/registry"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var log = logx.Delegating("etcdx.registry")

// Registrar 是 regcore.Registrar 的 etcd 实现：实例键绑定 Lease 并由后台
// 监护 goroutine 持续 KeepAlive；租约丢失（网络分区、etcd 重启）后自动
// 指数退避重注册，直到 Deregister/Close 显式停止。
type Registrar struct {
	cli *clientv3.Client
	opt *Option

	mu     sync.Mutex
	regs   map[string]*registration // 实例键 → 监护状态
	closed bool
}

type registration struct {
	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	leaseID clientv3.LeaseID
}

func (r *registration) lease() clientv3.LeaseID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaseID
}

func (r *registration) setLease(id clientv3.LeaseID) {
	r.mu.Lock()
	r.leaseID = id
	r.mu.Unlock()
}

var _ regcore.Registrar = (*Registrar)(nil)

// NewRegistrar 创建 etcd 注册器。
//
// client 为 nil 时返回 ErrNilClient（构造期 fail-fast，避免首次注册时在
// cli.Put 上空指针崩溃）。
func NewRegistrar(cli *clientv3.Client, opts ...OptionFunc) (*Registrar, error) {
	if cli == nil {
		return nil, ErrNilClient
	}
	return &Registrar{cli: cli, opt: applyOptions(opts), regs: make(map[string]*registration)}, nil
}

// Register 注册实例：Grant 租约 → Put 带租约的实例键 → 启动监护 goroutine。
// 同一实例重复 Register 视为刷新（旧监护停止、旧租约撤销后重新注册）。
// 注册成功返回后，续约与重注册与调用方 ctx 无关，由 Deregister/Close 终止。
func (r *Registrar) Register(ctx context.Context, inst regcore.Instance) error {
	if err := validateInstance(inst); err != nil {
		return err
	}
	group := r.opt.group(inst.Group)
	key := instanceKey(r.opt.Prefix, group, inst.Service, inst.Addr())
	val, err := encodeInstance(inst)
	if err != nil {
		return err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return fmt.Errorf("etcdx registry: registrar closed")
	}
	old := r.regs[key]
	delete(r.regs, key)
	r.mu.Unlock()
	if old != nil {
		_ = r.stop(ctx, old) // 旧注册清理失败不阻断新注册；旧租约由 TTL 兜底
	}

	leaseID, err := r.putWithLease(ctx, key, string(val))
	if err != nil {
		return err
	}
	sctx, cancel := context.WithCancel(context.Background())
	reg := &registration{cancel: cancel, done: make(chan struct{}), leaseID: leaseID}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		cancel()
		close(reg.done)
		_, _ = r.cli.Revoke(ctx, leaseID)
		return fmt.Errorf("etcdx registry: registrar closed")
	}
	r.regs[key] = reg
	r.mu.Unlock()

	async.GoSafe(func() { r.supervise(sctx, key, string(val), reg) })
	log.Info("instance registered", "key", key, "lease", leaseID, "ttl", r.opt.TTL)
	return nil
}

// Deregister 注销实例：停止监护 → Revoke 租约（键随租约原子删除）。
// 未注册或已注销的实例为 no-op。
func (r *Registrar) Deregister(ctx context.Context, inst regcore.Instance) error {
	if err := validateInstance(inst); err != nil {
		return err
	}
	key := instanceKey(r.opt.Prefix, r.opt.group(inst.Group), inst.Service, inst.Addr())
	r.mu.Lock()
	reg := r.regs[key]
	delete(r.regs, key)
	r.mu.Unlock()
	if reg == nil {
		return nil
	}
	if err := r.stop(ctx, reg); err != nil {
		return err
	}
	log.Info("instance deregistered", "key", key)
	return nil
}

// Close 注销全部实例并拒绝后续 Register（幂等）。
func (r *Registrar) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	regs := r.regs
	r.regs = make(map[string]*registration)
	r.mu.Unlock()

	var errs []error
	for key, reg := range regs {
		if err := r.stop(ctx, reg); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

// stop 终止监护并撤销租约；租约已过期（ErrLeaseNotFound）不算错误。
func (r *Registrar) stop(ctx context.Context, reg *registration) error {
	reg.cancel()
	<-reg.done
	id := reg.lease()
	if id == clientv3.NoLease {
		return nil
	}
	if _, err := r.cli.Revoke(ctx, id); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
		return fmt.Errorf("etcdx registry: revoke lease %x: %w", id, err)
	}
	return nil
}

// putWithLease 申请租约并写入实例键；Put 失败时回收刚申请的租约。
func (r *Registrar) putWithLease(ctx context.Context, key, val string) (clientv3.LeaseID, error) {
	grant, err := r.cli.Grant(ctx, r.opt.ttlSeconds())
	if err != nil {
		return clientv3.NoLease, fmt.Errorf("etcdx registry: grant lease: %w", err)
	}
	if _, err := r.cli.Put(ctx, key, val, clientv3.WithLease(grant.ID)); err != nil {
		_, _ = r.cli.Revoke(ctx, grant.ID)
		return clientv3.NoLease, fmt.Errorf("etcdx registry: put %s: %w", key, err)
	}
	return grant.ID, nil
}

// supervise 是每个实例的监护循环：持续消费 KeepAlive 响应，通道关闭
// （租约丢失 / 连接长期不可用）即指数退避重注册，直到 ctx 取消。
func (r *Registrar) supervise(ctx context.Context, key, val string, reg *registration) {
	defer close(reg.done)
	for {
		ch, err := r.cli.KeepAlive(ctx, reg.lease())
		if err != nil {
			log.Warn("keepalive start failed", "key", key, "err", err)
		} else {
			for range ch { //nolint:revive // 仅排空响应，通道关闭即退出
			}
		}
		if ctx.Err() != nil {
			return
		}
		log.Warn("lease lost, re-registering", "key", key)
		if !r.reRegister(ctx, key, val, reg) {
			return
		}
	}
}

// reRegister 退避重试直到成功或 ctx 取消；返回是否成功。
func (r *Registrar) reRegister(ctx context.Context, key, val string, reg *registration) bool {
	backoff := time.Second
	for {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		id, err := r.putWithLease(rctx, key, val)
		cancel()
		if err == nil {
			reg.setLease(id)
			log.Info("instance re-registered", "key", key, "lease", id)
			return true
		}
		log.Warn("re-register failed, will retry", "key", key, "backoff", backoff, "err", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
