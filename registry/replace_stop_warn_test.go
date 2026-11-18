package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oekit/concurrency/async"
	"github.com/oekit/logx"
	regcore "github.com/oekit/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// #196 回归：Register 替换旧注册时 stop（Revoke 旧租约）失败必须留下
// 可观测信号（Warn 日志），不得 `_ =` 静默吞错——这是唯一产生孤儿租约的
// 路径：旧注册已出 regs 表、其 supervise 已 cancel，Deregister/Close 都
// 收不到它；Revoke 失败若无痕迹，孤儿租约只能等 TTL 兜底过期且无人知晓。
//
// 手法（对齐 distlock/acquire_rollback_test.go 的桩件模式，把真实 etcd
// 测不到的「Revoke 恰好失败」钉成确定性用例）：Grant/Put 正常放行
// （两次 Register 都成功），KeepAlive 挂起直到 ctx 取消（supervise 可被
// stop 正常收尾），Revoke 恒失败（模拟替换瞬间 etcd 黑洞）⇒ 此前
// `_ = r.stop(ctx, old)` 吞掉的正是这条错误。
func TestReplaceOldRegistrationStopFailureIsObservable(t *testing.T) {
	capture := newCaptureLogger()
	old := logx.Default()
	logx.SetDefault(capture)
	t.Cleanup(func() { logx.SetDefault(old) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // 桩件 goroutine 收尾

	cli := clientv3.NewCtxClient(ctx)
	cli.KV = okPutKV{}
	cli.Lease = failRevokeLease{}

	reg, err := NewRegistrar(cli, WithPrefix("/t196"), WithTTL(5*time.Second))
	if err != nil {
		t.Fatalf("NewRegistrar: %v", err)
	}
	inst := regcore.Instance{Service: "svc", IP: "10.0.0.1", Port: 8080}
	if err := reg.Register(ctx, inst); err != nil {
		t.Fatalf("Register #1: %v", err)
	}
	// 替换旧注册：stop(old) 的 Revoke 失败 ⇒ Warn 留痕；新注册本身不受影响
	if err := reg.Register(ctx, inst); err != nil {
		t.Fatalf("Register #2（替换路径不得被旧清理失败阻断）: %v", err)
	}

	// 旧注册清理的 stop 失败必须以 Warn 留痕（#196），并带 key 与
	// logx.ErrKey 字段（本仓日志红线）
	rec, ok := capture.find(logx.LevelWarn, "stop old registration during replace failed, left to TTL reap")
	if !ok {
		t.Fatalf("未捕获到替换旧注册 stop 失败的 Warn（#196）；捕获记录: %v", capture.snapshot())
	}
	if k, _ := rec.attr("key"); k != instanceKey("/t196", defaultGroup, "svc", "10.0.0.1:8080") {
		t.Fatalf("Warn 的 key 字段 = %v; want %s", k, instanceKey("/t196", defaultGroup, "svc", "10.0.0.1:8080"))
	}
	if _, ok := rec.attr(logx.ErrKey); !ok {
		t.Fatalf("Warn 记录缺 %s 字段（本仓日志红线）: %+v", logx.ErrKey, rec)
	}

	// 收尾：结束第二条注册的 supervise goroutine（Close 内 Revoke 仍失败，
	// 错误仅反映桩件恒失败，不参与断言）
	_ = reg.Close(context.Background())
}

// --- 桩件 ---

// okPutKV：Put 恒成功（两次 Register 的实例键写入都放行）。全部 KV 方法
// 显式覆盖，避免嵌入 nil 接口的自动转发 panic。
type okPutKV struct{ clientv3.KV }

func (okPutKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{}, nil
}
func (okPutKV) Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}
func (okPutKV) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}
func (okPutKV) Compact(context.Context, int64, ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}
func (okPutKV) Do(context.Context, clientv3.Op) (clientv3.OpResponse, error) {
	var resp clientv3.OpResponse
	return resp, nil // 不被触达；仅满足接口
}
func (okPutKV) Txn(context.Context) clientv3.Txn { return okTxn{} }

// okTxn：不被触达（Registrar 不走 Txn），仅满足 KV 接口。
type okTxn struct{ clientv3.Txn }

func (okTxn) If(...clientv3.Cmp) clientv3.Txn        { return okTxn{} }
func (okTxn) Then(...clientv3.Op) clientv3.Txn       { return okTxn{} }
func (okTxn) Else(...clientv3.Op) clientv3.Txn       { return okTxn{} }
func (okTxn) Commit() (*clientv3.TxnResponse, error) { return &clientv3.TxnResponse{}, nil }

// failRevokeLease：Grant 正常放行（注册要有租约可停），KeepAlive 挂起直到
// ctx 取消（supervise 被 stop cancel 后正常收尾），Revoke 恒失败（模拟替换
// 瞬间 etcd 黑洞，触发 stop 失败路径）。
type failRevokeLease struct{ clientv3.Lease }

func (failRevokeLease) Grant(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 196, TTL: 5}, nil
}
func (failRevokeLease) Revoke(context.Context, clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return nil, errors.New("etcd blackholed at revoke time (stub #196)")
}
func (failRevokeLease) KeepAlive(ctx context.Context, _ clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	async.GoSafe(func() { // 测试桩件 goroutine：ctx 取消即退出（与 supervise 收尾对接）
		<-ctx.Done()
		close(ch)
	})
	return ch, nil
}

// --- 捕获 logger（对齐 distlock/acquire_rollback_test.go 的同款基建） ---

type capturedRecord struct {
	level logx.Level
	msg   string
	kv    []any
}

func (r capturedRecord) attr(key string) (any, bool) {
	for i := 0; i+1 < len(r.kv); i += 2 {
		if k, ok := r.kv[i].(string); ok && k == key {
			return r.kv[i+1], true
		}
	}
	return nil, false
}

// captureLogger 捕获结构化日志（Debug/Info/Warn/Error），其余方法经
// logx.Nop 空转；With* 返回自身（Delegating 解析链 Default().WithAttrs
// (component) 后仍落回本捕获器，Warn 不丢失）。
type captureLogger struct {
	logx.Logger

	mu      sync.Mutex
	records []capturedRecord
}

func newCaptureLogger() *captureLogger { return &captureLogger{Logger: logx.Nop()} }

func (c *captureLogger) add(level logx.Level, msg string, kv []any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, capturedRecord{level: level, msg: msg, kv: kv})
}

func (c *captureLogger) Debug(msg string, kv ...any) { c.add(logx.LevelDebug, msg, kv) }
func (c *captureLogger) Info(msg string, kv ...any)  { c.add(logx.LevelInfo, msg, kv) }
func (c *captureLogger) Warn(msg string, kv ...any)  { c.add(logx.LevelWarn, msg, kv) }
func (c *captureLogger) Error(msg string, kv ...any) { c.add(logx.LevelError, msg, kv) }

func (c *captureLogger) WithAttrs(...logx.Attr) logx.Logger  { return c }
func (c *captureLogger) WithGroup(string) logx.Logger        { return c }
func (c *captureLogger) WithService(string) logx.Logger      { return c }
func (c *captureLogger) WithCtx(context.Context) logx.Logger { return c }
func (c *captureLogger) SetLevel(logx.Level)                 {}

func (c *captureLogger) find(level logx.Level, msgSubstring string) (capturedRecord, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.level == level && strings.Contains(r.msg, msgSubstring) {
			return r, true
		}
	}
	return capturedRecord{}, false
}

func (c *captureLogger) snapshot() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.records))
	for _, r := range c.records {
		out = append(out, fmt.Sprintf("[%v] %s %v", r.level, r.msg, r.kv))
	}
	return strings.Join(out, "; ")
}
