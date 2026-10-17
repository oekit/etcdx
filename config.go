package etcdx

import (
	"context"
	"fmt"
	"time"

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
	if err := c.Health(ctx); err != nil {
		// 探活失败也要回收已创建的 client（gRPC 连接已建立）
		_ = c.Close(ctx)
		return nil, fmt.Errorf("etcdx: ping failed: %w", err)
	}
	return c, nil
}
