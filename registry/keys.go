package registry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	regcore "github.com/oekit/registry"
)

// serviceDir 返回某服务全部实例键的公共前缀（含尾部 "/"，供 WithPrefix 使用）。
func serviceDir(prefix, group, service string) string {
	return prefix + "/services/" + group + "/" + service + "/"
}

// instanceKey 返回单个实例的键。
func instanceKey(prefix, group, service, addr string) string {
	return serviceDir(prefix, group, service) + addr
}

// configKey 返回配置项的键。
func configKey(prefix, group, dataID string) string {
	return prefix + "/configs/" + group + "/" + dataID
}

// validateKeySegment 校验键层级的一段（group/service/dataID/addr）：必须非空
// 且不含 "/"。键结构 <prefix>/services/<group>/<service>/<addr> 与
// <prefix>/configs/<group>/<dataID> 都是纯字符串拼接，段内出现 "/" 与层级
// 分隔符不可区分（#110）：服务 "order/v2" 的实例键落进服务 "order" 的前缀
// 扫描范围，Discover("order") 连带返回 "order/v2" 的实例，流量被路由到
// 另一个服务；dataID 含 "/" 时 (group="a", dataID="b/c") 与
// (group="a/b", dataID="c") 解析到同一个键，跨 group 串扰。把「段内无 "/"」
// 做成构造期/入口期不变量，键层级边界不再依赖命名习惯。
func validateKeySegment(kind, name string) error {
	if name == "" {
		return fmt.Errorf("etcdx registry: %s is empty", kind)
	}
	if strings.Contains(name, "/") {
		return fmt.Errorf("etcdx registry: %s %q must not contain %q: reserved as key hierarchy separator", kind, name, "/")
	}
	return nil
}

// isDirectInstanceEntry 判断前缀扫描到的键是否为服务目录的直接实例键：实例
// 键 = serviceDir + addr（ip:port，不含 "/"），余段出现 "/" 说明它是名字带
// "/" 的服务留下的更深一层键（历史遗留或绕过校验写入，#110 读侧防御），
// 不得并入本服务的实例集。调用方保证 key 以 dir 为前缀（prefix 扫描结果）。
func isDirectInstanceEntry(dir string, key []byte) bool {
	return !bytes.Contains(key[len(dir):], []byte("/"))
}

// instanceValue 是实例键的存储形态（JSON）。Service/Group 由键路径承载不重复存。
type instanceValue struct {
	IP       string            `json:"ip"`
	Port     int               `json:"port"`
	Weight   float64           `json:"weight"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Cluster  string            `json:"cluster,omitempty"`
}

func encodeInstance(inst regcore.Instance) ([]byte, error) {
	b, err := json.Marshal(instanceValue{
		IP: inst.IP, Port: inst.Port, Weight: inst.Weight,
		Metadata: inst.Metadata, Cluster: inst.Cluster,
	})
	if err != nil {
		return nil, fmt.Errorf("etcdx registry: encode instance: %w", err)
	}
	return b, nil
}

func decodeInstance(service, group string, b []byte) (regcore.Instance, error) {
	var v instanceValue
	if err := json.Unmarshal(b, &v); err != nil {
		return regcore.Instance{}, fmt.Errorf("etcdx registry: decode instance: %w", err)
	}
	return regcore.Instance{
		Service: service, IP: v.IP, Port: v.Port, Weight: v.Weight,
		Metadata: v.Metadata, Group: group, Cluster: v.Cluster,
	}, nil
}

func validateInstance(inst regcore.Instance) error {
	if err := validateKeySegment("service name", inst.Service); err != nil {
		return err
	}
	// 空 group 回退默认分组（options.group），非空时必须是合法层级段（#110）
	if inst.Group != "" {
		if err := validateKeySegment("group name", inst.Group); err != nil {
			return err
		}
	}
	// addr（ip:port）是键的最后一段：ip 含 "/" 会多出一层——既可能落进别的
	// 服务的前缀，又会被「直接子键」读侧防御当嵌套键跳过（注册成功却永远
	// 不可发现的静默故障，#110）
	if err := validateKeySegment("instance ip", inst.IP); err != nil {
		return err
	}
	if inst.Port <= 0 || inst.Port > 65535 {
		return fmt.Errorf("etcdx registry: invalid port %d", inst.Port)
	}
	return nil
}
