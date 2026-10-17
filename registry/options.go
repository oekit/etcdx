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

// Option 是本包各组件的共享配置。
type Option struct {
	// Prefix 是全部键的根前缀（默认 "/oekit/registry"），自动去尾部 "/"。
	Prefix string
	// Group 是 Instance.Group 为空时使用的默认分组（默认 "DEFAULT_GROUP"）。
	Group string
	// TTL 是注册租约时长（默认 30s；etcd 服务端可能上调至其最小租约 TTL）。
	TTL time.Duration
}

// OptionFunc 配置函数类型。
type OptionFunc func(*Option)

// WithPrefix 设置键根前缀。
func WithPrefix(prefix string) OptionFunc {
	return func(o *Option) { o.Prefix = prefix }
}

// WithGroup 设置默认分组。
func WithGroup(group string) OptionFunc {
	return func(o *Option) { o.Group = group }
}

// WithTTL 设置注册租约时长（秒级精度，不足 1s 按 1s 计）。
func WithTTL(ttl time.Duration) OptionFunc {
	return func(o *Option) { o.TTL = ttl }
}

func applyOptions(opts []OptionFunc) *Option {
	o := &Option{Prefix: defaultPrefix, Group: defaultGroup, TTL: defaultTTL}
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
	if o.TTL < time.Second {
		o.TTL = time.Second
	}
	return o
}

// ttlSeconds 返回租约秒数（etcd Grant 以秒为单位）。
func (o *Option) ttlSeconds() int64 {
	return int64(o.TTL / time.Second)
}

func (o *Option) group(g string) string {
	if g == "" {
		return o.Group
	}
	return g
}
