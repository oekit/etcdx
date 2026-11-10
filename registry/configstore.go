package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/oekit/concurrency/async"
	regcore "github.com/oekit/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ConfigStore 是 regcore.ConfigStore 的 etcd 实现：配置即一个 KV，
// 监听即 Watch 该键。
type ConfigStore struct {
	cli *clientv3.Client
	opt *Option

	mu        sync.Mutex
	listeners map[string]*watchHandle // 配置键 → 监听句柄
}

var _ regcore.ConfigStore = (*ConfigStore)(nil)

// NewConfigStore 创建 etcd 配置源。
//
// client 为 nil 时返回 ErrNilClient（构造期 fail-fast，避免首次读写时在
// cli.Get/cli.Put 上空指针崩溃）。
func NewConfigStore(cli *clientv3.Client, opts ...OptionFunc) (*ConfigStore, error) {
	if cli == nil {
		return nil, ErrNilClient
	}
	return &ConfigStore{cli: cli, opt: applyOptions(opts), listeners: make(map[string]*watchHandle)}, nil
}

// PutConfig 写入配置（SPI 之外的便捷方法：供运维脚本/测试灌配置）。
func (c *ConfigStore) PutConfig(ctx context.Context, dataID, group, content string) error {
	if dataID == "" {
		return fmt.Errorf("etcdx registry: dataID is empty")
	}
	g := c.opt.group(group)
	if err := c.checkConfigName(dataID, g); err != nil {
		return err
	}
	if _, err := c.cli.Put(ctx, configKey(c.opt.Prefix, g, dataID), content); err != nil {
		return fmt.Errorf("etcdx registry: put config %s: %w", dataID, err)
	}
	return nil
}

// GetConfig 拉取配置内容；键不存在返回空字符串（非错误）。
func (c *ConfigStore) GetConfig(ctx context.Context, dataID, group string) (string, error) {
	if dataID == "" {
		return "", fmt.Errorf("etcdx registry: dataID is empty")
	}
	g := c.opt.group(group)
	if err := c.checkConfigName(dataID, g); err != nil {
		return "", err
	}
	resp, err := c.cli.Get(ctx, configKey(c.opt.Prefix, g, dataID))
	if err != nil {
		return "", fmt.Errorf("etcdx registry: get config %s: %w", dataID, err)
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}

// checkConfigName 校验配置键的两个路径段（#110）：dataID 含 "/" 时与
// group 分隔符不可区分——group "g/a"+dataID "b" 与 group "g"+dataID
// "a/b" 拼出同一个键，跨 group 串扰；configKey 是精确读写（非前缀扫描），
// 歧义只发生在键构造这一步，fail-fast 即封闭。
func (c *ConfigStore) checkConfigName(dataID, group string) error {
	if err := checkName("dataID", dataID); err != nil {
		return err
	}
	return checkName("group", group)
}

// ListenConfig 监听配置变更：Put 事件回调新内容，Delete 事件回调空字符串。
// 不回放当前值（需要初值请先 GetConfig）。回调在监听专属 goroutine 中串行
// 执行（不会并发调用 onChange）。onChange panic 不会终结监听循环：panic 被
// 就地 recover 转 Error 日志（含调用栈），本次投递跳过，后续变更照常推送
// （#87）。同一键重复监听会替换旧监听：旧观察
// goroutine 被标记取代并取消，其未投递的过期内容被丢弃，新监听等旧
// goroutine 退出后才开始投递，新旧回调从不错峰并发（#84）。替换不阻塞
// ListenConfig 调用方，但旧回调永久阻塞会拖住新监听的首次投递。监听生命
// 周期与传入 ctx 无关，由 StopListenConfig 终止。
func (c *ConfigStore) ListenConfig(ctx context.Context, dataID, group string, onChange func(data string)) error {
	if dataID == "" {
		return fmt.Errorf("etcdx registry: dataID is empty")
	}
	if err := c.checkConfigName(dataID, c.opt.group(group)); err != nil {
		return err
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

	h := newWatchHandle()
	c.mu.Lock()
	old := c.listeners[key]
	if old != nil {
		old.supersede() // 标记取代 + cancel：旧 goroutine 不再投递（仅 cancel 不交接，#84）
	}
	c.listeners[key] = h
	c.mu.Unlock()

	async.GoSafe(func() {
		// 观察循环（#55）：Canceled（ErrCompacted 等）/通道关闭后指数退避重建
		//（重读当前值拿新 revision + 立即回调一次最新内容）——对齐订阅侧的
		// 自动重建韧性；此前只 Warn 一条就 return，配置推送几天后无声停更。
		//
		// #84：投递前先等被替换的旧 goroutine 退出（回调不可能并发），之后
		// 所有投递前检查 superseded，被取代/停听后的过期内容一律丢弃。
		defer h.close()
		if old != nil {
			select {
			case <-old.done: // 旧 goroutine 已退出：此后才允许本监听投递
			case <-h.ctx.Done():
				return // 等待期间本监听又被取代/停听：不投递
			}
		}
		if h.superseded.Load() {
			return
		}

		// 退避由**连续重建失败次数**派生（#85，与订阅侧同口径）：每轮快照
		// 重取失败 attempts++，成功即归零。旧实现 backoff 声明在循环外只增
		// 不减，几次抖动后涨满 30s，此后任意一次瞬时中断都要吃满最长窗口
		// 才恢复推送，配置热更新最多延迟 30s。
		attempts := 0
		rev := startRev
		for h.ctx.Err() == nil {
			wch := c.cli.Watch(h.ctx, key, clientv3.WithRev(rev))
			canceled := false
			for wresp := range wch {
				if wresp.Canceled {
					log.Warn("config watch canceled, rebuilding with exponential backoff",
						"dataID", dataID, "err", wresp.Err())
					canceled = true
					break
				}
				if h.superseded.Load() {
					return // 已被新监听/停听取代：过期投递一律丢弃（#84）
				}
				for _, ev := range wresp.Events {
					if ev.Type == clientv3.EventTypeDelete {
						safeNotify(dataID, func() { onChange("") }) // onChange panic 不终结循环（#87）
					} else {
						safeNotify(dataID, func() { onChange(string(ev.Kv.Value)) })
					}
				}
			}
			if h.ctx.Err() != nil {
				return // StopListenConfig：正常退出
			}
			if !canceled {
				log.Warn("config watch channel closed, rebuilding", "dataID", dataID)
			}
			select {
			case <-h.ctx.Done():
				return
			case <-time.After(watchBackoff(attempts)):
			}
			resp, err := c.cli.Get(h.ctx, key, clientv3.WithKeysOnly())
			if err != nil {
				attempts++
				log.Warn("config snapshot refresh for rebuild failed, retrying",
					"dataID", dataID, "err", err, "attempts", attempts)
				continue
			}
			rev = resp.Header.Revision + 1
			attempts = 0 // 重建成功：退避归零（#85），单次瞬时中断不吃历史累计窗口
			if h.superseded.Load() {
				return
			}
			// 重建后立即回调一次当前值（重建窗口内的变更不丢视图）。键已被
			// 删除时回调空串，与上方 Delete 事件语义一致（#83）——旧实现
			// 跳过空结果，「键已删除」在重建路径上无从表达：删除事件随被
			// 取消的 watch 一起丢失后，消费方永久持有已删配置且无后续事件
			// 可纠正。
			if val, gerr := c.cli.Get(h.ctx, key); gerr == nil {
				if len(val.Kvs) > 0 {
					safeNotify(dataID, func() { onChange(string(val.Kvs[0].Value)) }) // #87
				} else {
					safeNotify(dataID, func() { onChange("") }) // #87
				}
			}
		}
	})
	return nil
}

// StopListenConfig 停止监听（未监听为 no-op）。
func (c *ConfigStore) StopListenConfig(_ context.Context, dataID, group string) error {
	g := c.opt.group(group)
	if err := c.checkConfigName(dataID, g); err != nil {
		return err
	}
	key := configKey(c.opt.Prefix, g, dataID)
	c.mu.Lock()
	h := c.listeners[key]
	delete(c.listeners, key)
	c.mu.Unlock()
	if h != nil {
		h.supersede()
	}
	return nil
}

// Close 停止全部监听。
func (c *ConfigStore) Close(_ context.Context) error {
	c.mu.Lock()
	ls := c.listeners
	c.listeners = make(map[string]*watchHandle)
	c.mu.Unlock()
	for _, h := range ls {
		h.supersede()
	}
	return nil
}
