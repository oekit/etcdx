package registry

import (
	"context"
	"fmt"
	"sync"

	"github.com/oekit/concurrency/async"
	regcore "github.com/oekit/registry/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ConfigStore 是 regcore.ConfigStore 的 etcd 实现：配置即一个 KV，
// 监听即 Watch 该键。
type ConfigStore struct {
	cli *clientv3.Client
	opt *Option

	mu        sync.Mutex
	listeners map[string]context.CancelFunc // 配置键 → 取消函数
}

var _ regcore.ConfigStore = (*ConfigStore)(nil)

// NewConfigStore 创建 etcd 配置源。
func NewConfigStore(cli *clientv3.Client, opts ...OptionFunc) *ConfigStore {
	return &ConfigStore{cli: cli, opt: applyOptions(opts), listeners: make(map[string]context.CancelFunc)}
}

// PutConfig 写入配置（SPI 之外的便捷方法：供运维脚本/测试灌配置）。
func (c *ConfigStore) PutConfig(ctx context.Context, dataID, group, content string) error {
	if dataID == "" {
		return fmt.Errorf("etcdx registry: dataID is empty")
	}
	if _, err := c.cli.Put(ctx, configKey(c.opt.Prefix, c.opt.group(group), dataID), content); err != nil {
		return fmt.Errorf("etcdx registry: put config %s: %w", dataID, err)
	}
	return nil
}

// GetConfig 拉取配置内容；键不存在返回空字符串（非错误）。
func (c *ConfigStore) GetConfig(ctx context.Context, dataID, group string) (string, error) {
	if dataID == "" {
		return "", fmt.Errorf("etcdx registry: dataID is empty")
	}
	resp, err := c.cli.Get(ctx, configKey(c.opt.Prefix, c.opt.group(group), dataID))
	if err != nil {
		return "", fmt.Errorf("etcdx registry: get config %s: %w", dataID, err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// ListenConfig 监听配置变更：Put 事件回调新内容，Delete 事件回调空字符串。
// 不回放当前值（需要初值请先 GetConfig）。回调在监听专属 goroutine 中串行
// 执行。同一键重复监听会替换旧回调。监听生命周期与传入 ctx 无关，由
// StopListenConfig 终止。
func (c *ConfigStore) ListenConfig(ctx context.Context, dataID, group string, onChange func(data string)) error {
	if dataID == "" {
		return fmt.Errorf("etcdx registry: dataID is empty")
	}
	if onChange == nil {
		return fmt.Errorf("etcdx registry: onChange callback is nil")
	}
	key := configKey(c.opt.Prefix, c.opt.group(group), dataID)

	// 记录当前 revision，Watch 从 revision+1 起不漏变更
	resp, err := c.cli.Get(ctx, key, clientv3.WithKeysOnly())
	if err != nil {
		return fmt.Errorf("etcdx registry: listen config %s: %w", dataID, err)
	}
	startRev := resp.Header.Revision + 1

	wctx, cancel := context.WithCancel(context.Background())
	c.mu.Lock()
	if old := c.listeners[key]; old != nil {
		old()
	}
	c.listeners[key] = cancel
	c.mu.Unlock()

	async.GoSafeLabeled("etcdx.registry.config-watch", func() {
		wch := c.cli.Watch(wctx, key, clientv3.WithRev(startRev))
		for wresp := range wch {
			if wresp.Canceled {
				log.Warn("config watch canceled", "dataID", dataID, "err", wresp.Err())
				return
			}
			for _, ev := range wresp.Events {
				if ev.Type == clientv3.EventTypeDelete {
					onChange("")
				} else {
					onChange(string(ev.Kv.Value))
				}
			}
		}
	})
	return nil
}

// StopListenConfig 停止监听（未监听为 no-op）。
func (c *ConfigStore) StopListenConfig(_ context.Context, dataID, group string) error {
	key := configKey(c.opt.Prefix, c.opt.group(group), dataID)
	c.mu.Lock()
	cancel := c.listeners[key]
	delete(c.listeners, key)
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// Close 停止全部监听。
func (c *ConfigStore) Close(_ context.Context) error {
	c.mu.Lock()
	ls := c.listeners
	c.listeners = make(map[string]context.CancelFunc)
	c.mu.Unlock()
	for _, cancel := range ls {
		cancel()
	}
	return nil
}
