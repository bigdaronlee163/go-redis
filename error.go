package redis

import (
	"context"
	"io"
	"net"
	"strings"

	"github.com/go-redis/redis/v8/internal/pool"
	"github.com/go-redis/redis/v8/internal/proto"
)

// ErrClosed performs any operation on the closed client will return this error.
var ErrClosed = pool.ErrClosed

type Error interface {
	error

	// RedisError is a no-op function but
	// serves to distinguish types that are Redis
	// errors from ordinary errors: a type is a
	// Redis error if it has a RedisError method.
	RedisError()
}

var _ Error = proto.RedisError("")

func shouldRetry(err error, retryTimeout bool) bool {
	switch err {
	case io.EOF, io.ErrUnexpectedEOF:
		return true
	case nil, context.Canceled, context.DeadlineExceeded:
		return false
	}

	if v, ok := err.(timeoutError); ok {
		if v.Timeout() {
			return retryTimeout
		}
		return true
	}

	s := err.Error()
	if s == "ERR max number of clients reached" {
		return true
	}
	if strings.HasPrefix(s, "LOADING ") {
		return true
	}
	if strings.HasPrefix(s, "READONLY ") {
		return true
	}
	if strings.HasPrefix(s, "CLUSTERDOWN ") {
		return true
	}
	if strings.HasPrefix(s, "TRYAGAIN ") {
		return true
	}

	return false
}

func isRedisError(err error) bool {
	_, ok := err.(proto.RedisError)
	return ok
}

func isBadConn(err error, allowTimeout bool, addr string) bool {
	switch err {
	case nil:
		return false
	case context.Canceled, context.DeadlineExceeded:
		return true
	}

	if isRedisError(err) {
		switch {
		case isReadOnlyError(err):
			// Close connections in read only state in case domain addr is used
			// and domain resolves to a different Redis Server. See #790.
			return true
		case isMovedSameConnAddr(err, addr):
			// Close connections when we are asked to move to the same addr
			// of the connection. Force a DNS resolution when all connections
			// of the pool are recycled
			return true
		default:
			return false
		}
	}

	if allowTimeout {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}
	}

	return true
}

func isMovedError(err error) (moved bool, ask bool, addr string) {
	if !isRedisError(err) {
		return
	}

	s := err.Error()
	switch {
	case strings.HasPrefix(s, "MOVED "):
		moved = true
	case strings.HasPrefix(s, "ASK "):
		ask = true
	default:
		return
	}

	ind := strings.LastIndex(s, " ")
	if ind == -1 {
		return false, false, ""
	}
	// 返回正确的节点。
	addr = s[ind+1:]
	return
}

// 这个函数检查错误信息是否以 "LOADING " 开头。这个错误通常表示 Redis 服务器正在加载数据，无法处理请求。
func isLoadingError(err error) bool {
	return strings.HasPrefix(err.Error(), "LOADING ")
}

// 这个函数检查错误信息是否以 "READONLY " 开头。这个错误通常表示在主从复制的环境中，客户端试图在只读的从节点上执行写操作。
func isReadOnlyError(err error) bool {
	return strings.HasPrefix(err.Error(), "READONLY ")
}

// 这个函数检查错误信息是否以 "MOVED " 开头，并且错误信息的结尾是否包含指定的地址。这个错误通常表示请求的键被移动到另一个 Redis 节点，客户端需要更新其连接到新的节点。
func isMovedSameConnAddr(err error, addr string) bool {
	redisError := err.Error()
	if !strings.HasPrefix(redisError, "MOVED ") {
		return false
	}
	return strings.HasSuffix(redisError, " "+addr)
}

//------------------------------------------------------------------------------

type timeoutError interface {
	Timeout() bool
}
