// Package testutil 提供进程内嵌 etcd 服务端（embed），供各包单元测试使用，
// 不进入公共 API（internal）。
package testutil

import (
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// NewClient 启动一个内嵌 etcd 实例并返回已连接的客户端；
// 测试结束时自动关停服务端与客户端。端口由内核随机分配，
// 数据目录为 t.TempDir()（自动清理）。
func NewClient(t *testing.T) *clientv3.Client {
	t.Helper()

	clientURL, peerURL := freeURL(t), freeURL(t)

	cfg := embed.NewConfig()
	cfg.Name = "etcdx-test"
	cfg.Dir = t.TempDir()
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = cfg.Name + "=" + peerURL.String()
	cfg.ClusterState = embed.ClusterStateFlagNew
	// 测试静音：etcd 日志走 stderr 且降级为 error；关闭 gateway 避免
	// 额外占用 HTTP 端口
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{"stderr"}
	cfg.EnableGRPCGateway = false

	srv, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("start embedded etcd: %v", err)
	}
	select {
	case <-srv.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		srv.Close()
		t.Fatal("embedded etcd not ready in 15s")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		srv.Close()
		t.Fatalf("connect embedded etcd: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
		srv.Close()
	})
	return cli
}

// freeURL 申请一个内核随机空闲端口的 127.0.0.1 URL。
// 端口在 close 与 etcd bind 之间存在竞态窗口，实践中概率极低；
// 若真撞上 StartEtcd 会直接报错（fail-loud），重跑即可。
func freeURL(t *testing.T) url.URL {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("grab free port: %v", err)
	}
	defer func() { _ = l.Close() }()
	u, err := url.Parse(fmt.Sprintf("http://%s", l.Addr().String()))
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return *u
}
