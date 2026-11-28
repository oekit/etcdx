package registry

import (
	"strings"
	"time"
)

const (
	defaultPrefix = "/oekit/registry"
	defaultGroup  = "DEFAULT_GROUP"
	defaultTTL    = 30 * time.Second
)

// RegistryOption 是本包各组件的共享配置（裸 Option 命名违反硬规则「Option 命名 XxxOption」，下一 minor 对齐同仓 distlock 的 EtcdLockerOption 风格）。
type RegistryOption struct {
	// Prefix 是全部键的根前缀（默认 "/oekit/registry"），自动去尾部 "/"。
	Prefix string
	// Group 是 Instance.Group 为空时使用的默认分组（默认 "DEFAULT_GROUP"）。
	Group string
	// TTL 是注册租约时长（默认 30s；etcd 服务端可能上调至其最小租约 TTL）。
	TTL time.Duration
}

// clampWarnMsg 亚秒 TTL 钳值 Warn 的消息单源（#203）：文案与同仓 distlock
// 亚秒锁 TTL 抬升明示（#62）同款，测试同源断言，不复制字符串字面量。
const clampWarnMsg = "sub-second ttl raised to 1s (etcd lease granularity is seconds)"

// RegistryOptionFunc 配置函数类型。
type RegistryOptionFunc func(*RegistryOption)

// WithPrefix 设置键根前缀。
func WithPrefix(prefix string) RegistryOptionFunc {
	return func(o *RegistryOption) { o.Prefix = prefix }
}

// WithGroup 设置默认分组。
func WithGroup(group string) RegistryOptionFunc {
	return func(o *RegistryOption) { o.Group = group }
}

// WithTTL 设置注册租约时长（秒级精度，不足 1s 按 1s 计）。
func WithTTL(ttl time.Duration) RegistryOptionFunc {
	return func(o *RegistryOption) { o.TTL = ttl }
}

func applyOptions(opts []RegistryOptionFunc) *RegistryOption {
	o := &RegistryOption{Prefix: defaultPrefix, Group: defaultGroup, TTL: defaultTTL}
	for _, fn := range opts {
		if fn != nil {
			fn(o)
		}
	}
	o.Prefix = strings.TrimRight(o.Prefix, "/")
	if o.Prefix == "" {
		o.Prefix = defaultPrefix
	}
	if o.Group == "" {
		o.Group = defaultGroup
	}
	// 亚秒 TTL 钳值不再静默（#203）：对齐同仓 distlock 亚秒锁 TTL 抬升的约定
	// （#62 立约「抬升时至少打 Warn 明示，不再静默」）——etcd 租约以**秒**为
	// 单位，(0, 1s) 的 TTL 钳到 1s 是租约精度的机制性限制而非 bug，但调用方
	// 按毫秒理解会误判（同一 TTL 在 redisx 侧精确生效），钳值时打 Warn 明示。
	// applyOptions 仅在四个组件的构造函数各跑一次，Warn 不会重复刷屏。
	if o.TTL < time.Second {
		log.Warn(clampWarnMsg, "ttl", o.TTL)
		o.TTL = time.Second
	}
	return o
}

// ttlSeconds 返回租约秒数（etcd Grant 以秒为单位）。
func (o *RegistryOption) ttlSeconds() int64 {
	return int64(o.TTL / time.Second)
}

func (o *RegistryOption) group(g string) string {
	if g == "" {
		return o.Group
	}
	return g
}
