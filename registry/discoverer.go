package registry

import (
	"context"
	"fmt"
	"sort"

	regcore "github.com/oekit/registry/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Discoverer 是 regcore.Discoverer 的 etcd 实现：按服务前缀一次 Get 拉取全部实例。
type Discoverer struct {
	cli *clientv3.Client
	opt *Option
}

var _ regcore.Discoverer = (*Discoverer)(nil)

// NewDiscoverer 创建 etcd 发现器。
func NewDiscoverer(cli *clientv3.Client, opts ...OptionFunc) *Discoverer {
	return &Discoverer{cli: cli, opt: applyOptions(opts)}
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
	resp, err := cli.Get(ctx, serviceDir(prefix, group, service), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("etcdx registry: get %s: %w", service, err)
	}
	out := make([]regcore.Instance, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		inst, err := decodeInstance(service, group, kv.Value)
		if err != nil {
			log.Warn("skip malformed instance", "key", string(kv.Key), "err", err)
			continue
		}
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Addr() < out[j].Addr() })
	return out, nil
}
