package registry

import (
	"context"
	"fmt"
	"sync"

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

	// retired 承载已 StopListenConfig/Close 摘除、但观察 goroutine 尚未退出
	// 的句柄（#149，与 Subscriber.retired 同构）：#84 的交接不能依赖「旧句柄
	// 还在 listeners 里」——先删 map 再 supersede 会让停听后重建的监听拿到
	// old==nil，交接整段失效，新旧回调并发。同一名字至多存在于两者之一；
	// 槽位由观察 goroutine 退出时按身份自清（dropRetired）。
	retired map[string]*watchHandle
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
	return &ConfigStore{
		cli: cli, opt: applyOptions(opts),
		listeners: make(map[string]*watchHandle), retired: make(map[string]*watchHandle),
	}, nil
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
// 周期与传入 ctx 无关，由 StopListenConfig 终止。停听（StopListenConfig）
// 或 Close 之后立即重建监听同样遵守 #84 交接：被摘除的旧句柄保留在 retired
// 待退出槽位，新监听仍能等到其退出后才投递（#149）。
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
	if old == nil {
		// 停听/关停后的重建监听（#149）：旧句柄已从 listeners 摘除但可能
		// 仍在退出中，交接等待改读 retired 槽位
		old = c.retired[key]
		delete(c.retired, key)
	}
	if old != nil {
		old.supersede() // 标记取代 + cancel：旧 goroutine 不再投递（仅 cancel 不交接，#84）
	}
	c.listeners[key] = h
	c.mu.Unlock()

	async.GoSafe(func() {
		// #84：投递前先等被替换的旧 goroutine 退出（回调不可能并发），之后
		// 所有投递前检查 superseded，被取代/停听后的过期内容一律丢弃。
		defer func() {
			h.close()
			// #149：观察 goroutine 退出后从 retired 槽位摘除自己（身份校验）
			c.dropRetired(key, h)
		}()
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

		// 观察循环骨架单源见 runWatchLoop（#122）：Canceled（ErrCompacted 等）/
		// 通道关闭后指数退避重建（重读当前值拿新 revision + 立即回调一次最新
		// 内容）；退避由连续重建失败次数派生（#85），成功即归零。本侧差异点：
		// 事件逐条回调新值/删除空串；重建补投递回调当前值——键已被删除时
		// 回调空串，与 Delete 事件语义一致（#83）——「键已删除」在重建路径上
		// 必须可表达：删除事件随被取消的 watch 一起丢失后，消费方永久持有
		// 已删配置且无后续事件可纠正。onChange panic 由 safeNotify 就地收编
		// （#87），不炸穿重建循环。
		runWatchLoop(
			h,
			dataID,
			startRev,
			func(ctx context.Context, rev int64) clientv3.WatchChan {
				return c.cli.Watch(ctx, key, clientv3.WithRev(rev))
			},
			func(wresp clientv3.WatchResponse) {
				for _, ev := range wresp.Events {
					if ev.Type == clientv3.EventTypeDelete {
						safeNotify(dataID, func() { onChange("") }) // onChange panic 不终结循环（#87）
					} else {
						safeNotify(dataID, func() { onChange(string(ev.Kv.Value)) }) // #87
					}
				}
			},
			func(ctx context.Context) (int64, func(), error) {
				// 单次完整 Get 同时取 revision 与当前值（#147）：此前 KeysOnly
				// 取 revision、补投递闭包再做第二次完整 Get——第二次 Get 失败被
				// `gerr == nil` 的空 else 静默吞掉，重建窗口内的变更无日志、无
				// 投递、永久丢失（#83 只修了「键已删」分支）。现在取值与 revision
				// 同源：Get 失败走上方 err → attempts++ 指数退避重试，与订阅侧
				// resnap（单次 Get）同一错误口径；补投递闭包不再有自己的错误面。
				resp, err := c.cli.Get(ctx, key)
				if err != nil {
					return 0, nil, err
				}
				cur := ""
				if len(resp.Kvs) > 0 {
					cur = string(resp.Kvs[0].Value)
				}
				return resp.Header.Revision + 1, func() {
					// 重建后立即回调一次当前值（重建窗口内的变更不丢视图）；
					// 键已被删除时 cur 为空串，与 Delete 事件语义一致（#83）。
					safeNotify(dataID, func() { onChange(cur) }) // onChange panic 不终结循环（#87）
				}, nil
			},
		)
	})
	return nil
}

// StopListenConfig 停止监听（未监听为 no-op）。
//
// 摘除的句柄先进入 retired 待退出槽位而非直接丢弃（#149，与
// Subscriber.Unsubscribe 同口径）：其观察 goroutine 可能仍在执行消费方回调，
// 后续同键 ListenConfig 靠该槽位拿到 old.done 完成 #84 交接——先删 map 再
// supersede 会让交接整段失效（停听后重建监听与旧回调并发）。本方法不等待
// 旧回调返回；槽位由观察 goroutine 退出时自清。
func (c *ConfigStore) StopListenConfig(_ context.Context, dataID, group string) error {
	g := c.opt.group(group)
	if err := c.checkConfigName(dataID, g); err != nil {
		return err
	}
	key := configKey(c.opt.Prefix, g, dataID)
	c.mu.Lock()
	h := c.listeners[key]
	if h != nil {
		delete(c.listeners, key)
		c.retired[key] = h // #149：摘除后仍可被重建监听读到（同名至多一个）
	}
	c.mu.Unlock()
	if h != nil {
		h.supersede()
	}
	return nil
}

// Close 停止全部监听。
//
// 被取消的句柄同样转入 retired 待退出槽位（#149，与 StopListenConfig 同
// 口径）：Close 之后立即重建监听仍须等旧 goroutine 退出，回调不并发。
func (c *ConfigStore) Close(_ context.Context) error {
	c.mu.Lock()
	ls := c.listeners
	c.listeners = make(map[string]*watchHandle)
	for name, h := range ls {
		c.retired[name] = h // listeners/retired 同名不相交（见 retired 字段注释）
	}
	c.mu.Unlock()
	for _, h := range ls {
		h.supersede()
	}
	return nil
}

// dropRetired 观察 goroutine 退出时把自身从 retired 槽位摘除（身份校验：
// 非本句柄则不动）。逻辑与 Subscriber.dropRetired 同构（#149）。
func (c *ConfigStore) dropRetired(key string, h *watchHandle) {
	c.mu.Lock()
	if c.retired[key] == h {
		delete(c.retired, key)
	}
	c.mu.Unlock()
}
