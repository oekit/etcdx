package registry

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/oekit/logx"
	regcore "github.com/oekit/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Discoverer 是 regcore.Discoverer 的 etcd 实现：按服务前缀一次 Get 拉取全部实例。
type Discoverer struct {
	cli *clientv3.Client
	opt *Option
}

var _ regcore.Discoverer = (*Discoverer)(nil)

// NewDiscoverer 创建 etcd 发现器。
//
// client 为 nil 时返回 ErrNilClient（构造期 fail-fast，避免首次发现时在
// cli.Get 上空指针崩溃）。
func NewDiscoverer(cli *clientv3.Client, opts ...OptionFunc) (*Discoverer, error) {
	if cli == nil {
		return nil, ErrNilClient
	}
	return &Discoverer{cli: cli, opt: applyOptions(opts)}, nil
}

// Discover 返回服务当前存活实例（按地址排序，结果确定）；无实例返回
// regcore.ErrNoInstances。
func (d *Discoverer) Discover(ctx context.Context, serviceName string) ([]regcore.Instance, error) {
	insts, err := fetchInstances(ctx, d.cli, d.opt.Prefix, d.opt.Group, serviceName)
	if err != nil {
		return nil, err
	}
	if len(insts) == 0 {
		return nil, regcore.ErrNoInstances
	}
	return insts, nil
}

// DiscoverAll 与 Discover 返回同一集合：etcd 无独立健康状态，死实例已随
// 租约到期自动消失；区别仅在无实例时返回空切片而非 ErrNoInstances。
func (d *Discoverer) DiscoverAll(ctx context.Context, serviceName string) ([]regcore.Instance, error) {
	return fetchInstances(ctx, d.cli, d.opt.Prefix, d.opt.Group, serviceName)
}

// fetchInstances 拉取并解码某服务下全部实例键；单个坏值跳过不致整批失败。
func fetchInstances(ctx context.Context, cli *clientv3.Client, prefix, group, service string) ([]regcore.Instance, error) {
	if service == "" {
		return nil, fmt.Errorf("etcdx registry: service name is empty")
	}
	// 键层级边界 fail-fast（#110）：名称段含 "/" 的服务/分组不被发现——
	// 拼出的前缀会越界命中其他服务（或被其他服务命中）的键空间
	if err := checkName("service", service); err != nil {
		return nil, err
	}
	if err := checkName("group", group); err != nil {
		return nil, err
	}
	dir := serviceDir(prefix, group, service)
	resp, err := cli.Get(ctx, dir, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcdx registry: get %s: %w", service, err)
	}
	out := make([]regcore.Instance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		if !validInstanceKey(kv.Key, dir) {
			log.Warn("skip out-of-boundary instance key (legacy slash name)",
				"key", string(kv.Key), "dir", dir)
			continue
		}
		inst, err := decodeInstance(service, group, kv.Value)
		if err != nil {
			log.Warn("skip malformed instance", "key", string(kv.Key), logx.ErrKey, err)
			continue
		}
		out = append(out, inst)
	}
	slices.SortFunc(out, func(a, b regcore.Instance) int { return cmp.Compare(a.Addr(), b.Addr()) })
	return out, nil
}
