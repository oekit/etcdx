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
//
// **不改写调用方传入的 Config**（#65）：零值默认在浅拷贝 `cfg := *opt` 上
// 应用——调用方复用同一结构体建多个客户端、或序列化记录配置时，不会发现
// 字段被悄悄改掉（0 → 5s）。此前直接写入调用方的结构体，属未声明的副作用。
func NewClient(opt *clientv3.Config) (*Client, error) {
	if opt == nil {
		return nil, fmt.Errorf("etcdx: config is nil")
	}
	if len(opt.Endpoints) == 0 {
		return nil, fmt.Errorf("etcdx: endpoints is empty")
	}
	cfg := *opt
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	c, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("etcdx: new client: %w", err)
	}
	return &Client{Client: c}, nil
}

// Health 探测预算（#125）：var 而非 const 仅供测试收窄窗口（测试同进程内
// 改回即还原）；生产默认值语义为：单 endpoint 独立 3s 窗口、全序列总预算
// 10s（AGENTS.md 超时区间「探测类 ≤10s」内）。
var (
	// healthEndpointWindow 是单个 endpoint 的 Member Status 探测窗口：首个
	// 黑洞 endpoint 只吃自己的预算，不影响其余 endpoint 的探测（#125）。
	healthEndpointWindow = 3 * time.Second
	// healthTotalBudget 是 Health 全序列的总预算：endpoint 各自独立窗口后，
	// 多 endpoint 串行探测的最坏总时长必须仍有界。
	healthTotalBudget = 10 * time.Second
)

// Health 通过 Member Status 探活：任一 endpoint 可用即健康。
//
// 预算两级（#125）：每个 endpoint 一个独立的 3s 窗口——此前全部 endpoint
// 共享一个 3s 窗口，首个黑洞 endpoint 吃光预算，其余 endpoint 在剩余≈0 的
// 预算内全部瞬时失败，健康判定退化为「第一个 endpoint 的状态」；总预算
// 10s 兜住多 endpoint 串行探测的最坏总时长（探测类 ≤10s 区间内）。
func (c *Client) Health(ctx context.Context) error {
	total, cancel := context.WithTimeout(ctx, healthTotalBudget)
	defer cancel()
	var lastErr error
	for _, ep := range c.Endpoints() {
		if total.Err() != nil {
			break // 总预算耗尽：不再探测剩余 endpoint（#125 总预算兜底）
		}
		// 每 endpoint 独立窗口（#125）：黑洞 endpoint 最多吃掉自己的 3s
		epCtx, epCancel := context.WithTimeout(total, healthEndpointWindow)
		_, err := c.Status(epCtx, ep)
		epCancel()
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
