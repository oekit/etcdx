package distlock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oekit/logx"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// #126 回归：加锁失败回滚分支的 session.Close 失败必须留下可观测信号
// （Warn 日志），不得 `_ =` 静默吞错（仓规「所有 error 禁止 _ 忽略」）。
// 失败后果：租约存活到 TTL 才释放，多实例在该窗口内可重复抢锁——注释
// 同时钉住「失败由 TTL 兜底」的残余风险边界。
//
// 手法（对齐 session_grant_test.go 的桩件模式，把真实 etcd 测不到的
// 「Close 恰好失败」钉成确定性用例）：预检探活恒成功（探测时活着），
// Txn 的 Commit 返回 concurrency.ErrLocked（加锁被占 ⇒ 走 acquire 失败
// 回滚分支），Session.Close 内部的 Revoke 返回错误（模拟回滚瞬间 etcd
// 不可达）⇒ 此前 `_ = session.Close()` 吞掉的正是这条错误。
func TestFailedAcquireCloseErrorIsObservable(t *testing.T) {
	capture := newCaptureLogger()
	old := logx.Default()
	logx.SetDefault(capture)
	t.Cleanup(func() { logx.SetDefault(old) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel) // 桩件 goroutine 收尾

	cli := clientv3.NewCtxClient(ctx)
	cli.KV = busyKV{}
	cli.Lease = failRevokeLease{}

	l, err := NewEtcdLocker(cli, WithPrefix("t126"))
	if err != nil {
		t.Fatalf("NewEtcdLocker: %v", err)
	}
	_, err = l.TryLock(context.Background(), "job", 2*time.Second)
	if !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("TryLock err = %v; want ErrLockNotAcquired（哨兵分类不受回滚路径影响）", err)
	}

	// 失败回滚的 session.Close 错误必须以 Warn 留痕（#126），并带
	// logx.ErrKey 字段（本仓日志红线）
	rec, ok := capture.find(logx.LevelWarn, "session close after failed acquire left lease to TTL reap")
	if !ok {
		t.Fatalf("未捕获到失败回滚的 session.Close Warn（#126）；捕获记录: %v", capture.snapshot())
	}
	if _, ok := rec.attr(logx.ErrKey); !ok {
		t.Fatalf("Warn 记录缺 %s 字段（本仓日志红线）: %+v", logx.ErrKey, rec)
	}
}

// --- 桩件 ---

// busyKV：连接预检探活恒成功（探测时 etcd 活着），Txn 恒 busy（加锁被其他
// 持有者占用）——acquire 走到 do 失败后的回滚分支。全部 KV 方法显式覆盖，
// 避免嵌入 nil 接口的自动转发 panic。
type busyKV struct{ clientv3.KV }

func (busyKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{}, nil
}
func (busyKV) Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}
func (busyKV) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}
func (busyKV) Compact(context.Context, int64, ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}
func (busyKV) Do(context.Context, clientv3.Op) (clientv3.OpResponse, error) {
	var resp clientv3.OpResponse
	return resp, concurrency.ErrLocked // 不被触达；仅满足接口
}
func (busyKV) Txn(context.Context) clientv3.Txn { return busyTxn{} }

// busyTxn 的 Commit 恒返回 concurrency.ErrLocked：加锁被其他持有者占用，
// TryLock 映射为 ErrLockNotAcquired，acquire 走失败回滚分支。
type busyTxn struct{ clientv3.Txn }

func (busyTxn) If(...clientv3.Cmp) clientv3.Txn        { return busyTxn{} }
func (busyTxn) Then(...clientv3.Op) clientv3.Txn       { return busyTxn{} }
func (busyTxn) Else(...clientv3.Op) clientv3.Txn       { return busyTxn{} }
func (busyTxn) Commit() (*clientv3.TxnResponse, error) { return nil, concurrency.ErrLocked }

// failRevokeLease：Grant 正常放行（Session 建立成功，回滚才有东西可关），
// KeepAlive 挂起直到 ctx 取消（Orphan 可正常收尾），Revoke 恒失败
// （模拟回滚瞬间 etcd 不可达，触发 session.Close 失败路径）。
type failRevokeLease struct{ clientv3.Lease }

func (failRevokeLease) Grant(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 126, TTL: 2}, nil
}

func (failRevokeLease) Revoke(context.Context, clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return nil, errors.New("etcd blackholed at revoke time (stub #126)")
}

func (failRevokeLease) KeepAlive(ctx context.Context, _ clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	ch := make(chan *clientv3.LeaseKeepAliveResponse)
	go func() { // 测试桩件 goroutine：ctx 取消即退出（与 Session.Orphan 收尾对接）
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

// --- 捕获 logger（logx.Nop 兜底空转 + 结构化方法重载为记录） ---

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
// (component) 后仍落回本捕获器， Warn 不丢失）。
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
