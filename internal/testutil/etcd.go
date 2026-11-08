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

// Server 是一个可重启的内嵌 etcd 测试服务端：同一数据目录 + 同一端口重启
// （模拟 etcd-server 重启/抖动），MVCC 数据与压缩点跨重启保留，客户端自动
// 重连。
type Server struct {
	t   *testing.T
	dir string

	clientURL string
	peerURL   string

	srv *embed.Etcd
	cli *clientv3.Client
}

// NewServer 启动一个内嵌 etcd 实例并返回服务端句柄；测试结束时自动关停。
// 端口由内核随机分配，数据目录为 t.TempDir()（自动清理）。
func NewServer(t *testing.T) *Server {
	t.Helper()
	s := &Server{
		t:         t,
		dir:       t.TempDir(),
		clientURL: freeAddr(t),
		peerURL:   freeAddr(t),
	}
	s.start(false)
	t.Cleanup(s.stop)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{s.clientURL},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		s.stop()
		t.Fatalf("connect embedded etcd: %v", err)
	}
	s.cli = cli
	return s
}

// NewClient 启动一个内嵌 etcd 实例并返回已连接的客户端；测试结束时自动
// 关停服务端与客户端。
func NewClient(t *testing.T) *clientv3.Client {
	t.Helper()
	return NewServer(t).Client()
}

// Client 返回已连接的客户端（跨重启自动重连同一 endpoint）。
func (s *Server) Client() *clientv3.Client { return s.cli }

// start 以当前数据目录与端口启动服务端并等待就绪；首启 ClusterState 用
// New（空目录建集群），重启用 Existing（从数据目录恢复既有集群）。
func (s *Server) start(restart bool) {
	s.t.Helper()
	cfg := embed.NewConfig()
	cfg.Name = "etcdx-test"
	cfg.Dir = s.dir
	cfg.ListenClientUrls = []url.URL{mustURL(s.clientURL)}
	cfg.ListenPeerUrls = []url.URL{mustURL(s.peerURL)}
	cfg.AdvertisePeerUrls = []url.URL{mustURL(s.peerURL)}
	cfg.InitialCluster = cfg.Name + "=http://" + s.peerURL
	cfg.ClusterState = embed.ClusterStateFlagNew
	if restart {
		// 单节点带数据目录重启：从既有集群状态恢复
		cfg.ClusterState = embed.ClusterStateFlagExisting
	}
	// 测试静音：etcd 日志走 stderr 且降级为 error；关闭 gateway 避免
	// 额外占用 HTTP 端口
	cfg.Logger = "zap"
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{"stderr"}
	cfg.EnableGRPCGateway = false

	srv, err := embed.StartEtcd(cfg)
	if err != nil {
		s.t.Fatalf("start embedded etcd: %v", err)
	}
	select {
	case <-srv.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		srv.Close()
		s.t.Fatal("embedded etcd not ready in 15s")
	}
	s.srv = srv
}

// stop 关停服务端与客户端（t.Cleanup 调用）。
func (s *Server) stop() {
	if s.cli != nil {
		_ = s.cli.Close()
	}
	if s.srv != nil {
		s.srv.Close()
	}
}

// Restart 重启服务端：先 Close（触发客户端 watch 流断开），空窗 downtime
// 后以同一数据目录 + 同一端口拉起并等待就绪。客户端自动重连；重连后
// clientv3 从其最后收到的 revision 自动恢复（resume）watch——若压缩点已
// 越过该 revision，服务端会对恢复的 watch 回 Canceled(ErrCompacted)，
// 从而确定性地触发被测代码的 watch 重建路径。
func (s *Server) Restart(downtime time.Duration) {
	s.t.Helper()
	s.srv.Close()
	if downtime > 0 {
		time.Sleep(downtime)
	}
	s.start(true)
}

// freeAddr 申请一个内核随机空闲端口的 127.0.0.1 地址。
// 端口在 close 与 etcd bind 之间存在竞态窗口，实践中概率极低；
// 若真撞上 StartEtcd 会直接报错（fail-loud），重跑即可。
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("grab free port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func mustURL(addr string) url.URL {
	u, err := url.Parse(fmt.Sprintf("http://%s", addr))
	if err != nil {
		panic(fmt.Sprintf("parse url %s: %v", addr, err))
	}
	return *u
}
