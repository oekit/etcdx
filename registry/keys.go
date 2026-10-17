package registry

import (
	"encoding/json"
	"fmt"

	regcore "github.com/oekit/registry/registry"
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
	if inst.IP == "" {
		return fmt.Errorf("etcdx registry: instance ip is empty")
	}
	if inst.Port <= 0 || inst.Port > 65535 {
		return fmt.Errorf("etcdx registry: invalid port %d", inst.Port)
	}
	return nil
}
