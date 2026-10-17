package etcdx

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Client 是 etcd 客户端封装：内嵌 *clientv3.Client 透传 KV/Lease/Watch 等
// 全部原生命令，另加 Close(ctx) 幂等关停与 Health 健康探针。
// 零可观测性副作用：需要 tracing 时用装饰器显式接入。
type Client struct {
	*clientv3.Client

	closeOnce sync.Once
	closeErr  error
}

// NewClient 从 clientv3.Config 创建客户端。
//
// 生产默认值（调用方显式设置的字段不覆盖）：DialTimeout 5s 收紧故障感知
// 窗口；AutoSyncInterval/MaxCallSendMsgSize 等其余字段交给 etcd 默认值。
// Endpoints 为空显式报错（静默连本地 2379 是事故源）。
func NewClient(opt *clientv3.Config) (*Client, error) {
	if opt == nil {
		return nil, fmt.Errorf("etcdx: config is nil")
	}
	if len(opt.Endpoints) == 0 {
		return nil, fmt.Errorf("etcdx: endpoints is empty")
	}
	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}
	c, err := clientv3.New(*opt)
	if err != nil {
		return nil, fmt.Errorf("etcdx: new client: %w", err)
	}
	return &Client{Client: c}, nil
}

// Health 通过 Member Status 探活：任一 endpoint 可用即健康。
func (c *Client) Health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var lastErr error
	for _, ep := range c.Endpoints() {
		_, err := c.Status(ctx, ep)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("etcdx: no endpoints")
	}
	return fmt.Errorf("etcdx: health check failed: %w", lastErr)
}

// Close 幂等关停客户端（sync.Once 保证多次调用安全）。
func (c *Client) Close(_ context.Context) error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Client.Close()
	})
	return c.closeErr
}
