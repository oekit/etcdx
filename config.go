package etcdx

import (
	"context"
	stderrors "errors"
	"time"

	errors "github.com/oekit/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdConfig 是 etcd 连接配置，可直接从 YAML 反序列化。
type EtcdConfig struct {
	// Endpoints 是 etcd 集群地址列表（至少一项）。
	Endpoints []string `yaml:"endpoints"`
	// Username/Password 用于 etcd 认证（可选）。
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	// DialTimeout 拨号超时；0 时取默认 5s。
	DialTimeout time.Duration `yaml:"dialTimeout"`
}

// NewClientFromConfig 从配置创建客户端（构造后立即 Health 验证连通性）。
//
// 用法：
//
//	client, err := etcdx.NewClientFromConfig(ctx, etcdx.EtcdConfig{
//	    Endpoints: []string{"127.0.0.1:2379"},
//	})
func NewClientFromConfig(ctx context.Context, cfg EtcdConfig) (*Client, error) {
	c, err := NewClient(&clientv3.Config{
		Endpoints:   cfg.Endpoints,
		Username:    cfg.Username,
		Password:    cfg.Password,
		DialTimeout: cfg.DialTimeout,
	})
	if err != nil {
		return nil, err
	}
	if err := healthOrCleanup(ctx, c); err != nil {
		return nil, err
	}
	return c, nil
}

// healthOrCleanup 连通性探测：Health 失败时回收已创建的 client（gRPC 连接
// 已建立），并把回收失败一并上抛（#184，对齐 redisx#149 口径）。
//
// 契约（本仓错误处理硬规则：所有 error 禁止 `_` 忽略）：gRPC 连接已建立而
// Health 失败，恰是唯一一次能观测到连接回收失败（半开连接、fd 回收失败）
// 的时机，回收错误不得静默丢弃——与 Health 错误经 errors.Join 聚合上抛
// （两侧各自 %w 包装，errors.Is/As 判定口径不变）；回收成功时返回口径与
// 历史一致（仅 Health 错误）。回收走 Client.Close(ctx) 幂等单源。
func healthOrCleanup(ctx context.Context, c *Client) error {
	if err := c.Health(ctx); err != nil {
		if cerr := c.Close(ctx); cerr != nil {
			return stderrors.Join(
				errors.Wrapf(err, "etcdx: ping failed"),
				errors.Wrapf(cerr, "etcdx: close failed"),
			)
		}
		return errors.Wrapf(err, "etcdx: ping failed")
	}
	return nil
}
