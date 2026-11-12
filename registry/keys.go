package registry

import (
	"encoding/json"
	"fmt"
	"strings"

	regcore "github.com/oekit/registry"

	"github.com/oekit/etcdx/internal/keyseg"
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
	if inst.Service == "" {
		return fmt.Errorf("etcdx registry: service name is empty")
	}
	if err := checkName("service", inst.Service); err != nil {
		return err
	}
	if inst.Group != "" {
		if err := checkName("group", inst.Group); err != nil {
			return err
		}
	}
	if inst.IP == "" {
		return fmt.Errorf("etcdx registry: instance ip is empty")
	}
	// IP 是实例键的最后一段（serviceDir + Addr()），同样不得越出层级边界
	if err := checkName("ip", inst.IP); err != nil {
		return err
	}
	if inst.Port <= 0 || inst.Port > 65535 {
		return fmt.Errorf("etcdx registry: invalid port %d", inst.Port)
	}
	return nil
}

// checkName 校验键路径段（service/group/dataID/ip）的层级边界：规则单源在
// internal/keyseg（registry#110 立约 + distlock#138 补齐共用，两包不复制
// 规则文档），此处只保留 registry 侧的错误前缀包装。
func checkName(kind, name string) error {
	if err := keyseg.Check(kind, name); err != nil {
		return fmt.Errorf("etcdx registry: %w", err)
	}
	return nil
}

// validInstanceKey 判定 key 是否为 dir 之下的**单段**实例键（dir + addr）。
// 前缀扫描（WithPrefix）会把 serviceDir 之下的一切都带回来，历史上含 "/"
// 的 service 名写出的越界键（"order/v2" 落在 "order/" 之下）也混在其中——
// 入口 fail-fast 后不可能再产生新脏键，但既有脏键仍在 etcd 里，段数不吻合
// 的键一律跳过，不再跨服务参与路由（#110 防御纵深）。
func validInstanceKey(key []byte, dir string) bool {
	rest, ok := strings.CutPrefix(string(key), dir)
	return ok && rest != "" && !strings.Contains(rest, "/")
}
