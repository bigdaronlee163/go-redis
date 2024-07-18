package pool

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal/proto"
)

var noDeadline = time.Time{}

type Conn struct {
	usedAt int64 // atomic
	// 代表这tcp通信。
	netConn net.Conn

	rd *proto.Reader // 根据 Redis 通信协议实现的 Reader
	bw *bufio.Writer
	wr *proto.Writer // 根据 Redis 通信协议实现的 Writer

	Inited    bool      // 是否完成初始化（该连接是否初始化，比如如果需要执行命令之前需要执行的auth,select db 等的标识，代表已经auth,select过）
	pooled    bool      // 是否放进连接池的标志，有些场景产生的连接是不需要放入连接池的
	createdAt time.Time // 创建时间（超过maxconnage的连接需要淘汰）
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}``
	cn.rd = proto.NewReader(netConn)
	cn.bw = bufio.NewWriter(netConn)
	cn.wr = proto.NewWriter(cn.bw)
	cn.SetUsedAt(time.Now())
	return cn
}

// 返回创建的时间。
func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.rd.Reset(netConn)
	cn.bw.Reset(netConn)
}

func (cn *Conn) Write(b []byte) (int, error) {
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	if cn.netConn != nil {
		return cn.netConn.RemoteAddr()
	}
	return nil
}

// 自定义的方法。不是go内置的方法。 [看方法签名]
// 这个方法用于设置读取操作的超时并执行传入的函数 fn。
// 设置读取超时：如果 timeout 不为零，调用 cn.netConn.SetReadDeadline 设置读取超时。超时时间由 cn.deadline(ctx, timeout) 计算得到。
// 执行传入的函数：传入的函数 fn 接收 cn.rd（一个 proto.Reader）并执行。
func (cn *Conn) WithReader(ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error) error {
	if timeout != 0 {
		if err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout)); err != nil {
			return err
		}
	}
	// 这里我估计是go net.Conn 包中的执行。
	// 虽然fn是proto里面的，但是实现了具体的接口就行。
	return fn(cn.rd)
}

// 这个方法用于设置写入操作的超时、刷新缓冲区并执行传入的函数 fn。
// 设置写入超时：如果 timeout 不为零，调用 cn.netConn.SetWriteDeadline 设置写入超时。超时时间由 cn.deadline(ctx, timeout) 计算得到。
// 重置缓冲区：如果缓冲区中有数据（即 cn.bw.Buffered() > 0），调用 cn.bw.Reset(cn.netConn) 重置缓冲区。
// 执行传入的函数：传入的函数 fn 接收 cn.wr（一个 proto.Writer）并执行。
// 刷新缓冲区：调用 cn.bw.Flush() 刷新缓冲区，将所有数据写入底层连接。
func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	if timeout != 0 {
		if err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, timeout)); err != nil {
			return err
		}
	}

	if cn.bw.Buffered() > 0 {
		cn.bw.Reset(cn.netConn)
	}

	if err := fn(cn.wr); err != nil {
		return err
	}

	return cn.bw.Flush()
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

// 设置最近使用时间：调用 cn.SetUsedAt(tm) 记录当前时间。
// 计算超时时间：如果 timeout 大于零，当前时间 tm 增加 timeout。
// 处理上下文截止日期：如果上下文 ctx 不为空，并且有截止日期，则比较 ctx 的截止日期与计算的超时时间，取较早者。
// 返回截止日期：如果有超时时间，返回计算后的时间；否则返回 noDeadline（未定义的常量，表示没有超时）。
func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		tm = tm.Add(timeout)
	}

	if ctx != nil {
		deadline, ok := ctx.Deadline()
		if ok {
			if timeout == 0 {
				return deadline
			}
			if deadline.Before(tm) {
				return deadline
			}
			return tm
		}
	}

	if timeout > 0 {
		return tm
	}

	return noDeadline
}
