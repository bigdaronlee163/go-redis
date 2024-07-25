package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
)

var (
	// ErrClosed performs any operation on the closed client will return this error.
	ErrClosed = errors.New("redis: client is closed")

	// ErrPoolTimeout timed out waiting to get a connection from the connection pool.
	ErrPoolTimeout = errors.New("redis: connection pool timeout")
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats contains pool state information and accumulated stats.
// 监控统计对调整连接池配置选项，优化连接池性能提供了重要的依据，在任何系统的设计中都是不必可少的组件。
type Stats struct {
	//Hits：连接池命中空闲连接次数
	Hits uint32 // number of times free connection was found in the pool
	//Misses：连接池没有空闲连接可用次数
	Misses uint32 // number of times free connection was NOT found in the pool
	//Timeouts：请求连接等待超时次数
	Timeouts uint32 // number of times a wait timeout occurred
	//TotalConns：连接池总连接数量
	TotalConns uint32 // number of total connections in the pool
	//IdleConns：连接池空闲连接数量
	IdleConns uint32 // number of idle connections in the pool
	//StaleConns：移除过期连接数量
	StaleConns uint32 // number of stale connections removed from the pool
}

type Pooler interface {
	NewConn(context.Context) (*Conn, error) // 创建连接
	CloseConn(*Conn) error                  // 关闭连接

	Get(context.Context) (*Conn, error)   // 获取连接
	Put(context.Context, *Conn)           // 放回连接
	Remove(context.Context, *Conn, error) // 移除连接

	Len() int      // 连接池长度
	IdleLen() int  // 空闲连接数量
	Stats() *Stats // 连接池统计

	Close() error // 关闭连接池
}

type Options struct {
	Dialer  func(context.Context) (net.Conn, error) // 新建连接的工厂函数
	OnClose func(*Conn) error                       // 关闭连接的回调函数，在连接关闭的时候的回调

	PoolFIFO           bool          // 是否使用先进先出（FIFO）策略，默认是使用后进先出（LIFO）策略
	PoolSize           int           // 连接池大小，连接池中的连接的最大数量
	MinIdleConns       int           // 最小空闲连接数
	MaxConnAge         time.Duration // 从连接池获取连接的超时时间
	PoolTimeout        time.Duration // 空闲连接的超时时间
	IdleTimeout        time.Duration // 空闲连接的超时时间
	IdleCheckFrequency time.Duration // 检查空闲连接频率（超时空闲连接清理的间隔时间）
}

type lastDialErrorWrap struct {
	err error
}

type ConnPool struct {
	opt *Options // 连接池参数配置，如上

	dialErrorsNum uint32 // atomic // 连接失败的错误次数，atomic
	/*
		lastDialErrorMu sync.RWMutex // 上一次连接错误锁，读写锁
		lastDialError   error // 上一次连接错误（保存了最近一次的连接错误）
	*/
	lastDialError atomic.Value // 上一次连接错误锁 【新版使用atomic】

	queue chan struct{} // 轮转队列，是一个 channel 结构（一个带容量（poolsize）的阻塞队列）

	connsMu      sync.Mutex // 连接队列锁
	conns        []*Conn    // 连接队列（连接队列，维护了未被删除所有连接，即连接池中所有的连接）
	idleConns    []*Conn    // 空闲连接队列（空闲连接队列，维护了所有的空闲连接）
	poolSize     int        // 连接池大小，注意如果没有可用的话要等待
	idleConnsLen int        // 连接池空闲连接队列长度

	stats Stats // 连接池统计的结构体（包含了使用数据）

	// 在这个上面的操作是 原子的。 atomic.CompareAndSwapUint32(&p._closed, 0, 1)
	_closed  uint32        // atomic  // 连接`池关闭标志，atomic
	closedCh chan struct{} // 通知连接池关闭通道（用于主协程通知子协程的常用方法）
}

/*
(*ConnPool)(nil) 将 nil 转换为 *ConnPool 类型的指针。
var _ Pooler 声明了一个名为 _ 的变量（使用 _ 是因为我们不需要使用这个变量，只是为了进行类型检查）。
这行代码实际上是在检查 *ConnPool 类型是否实现了 Pooler 接口。如果没有实现，编译器会报错。
*/
var _ Pooler = (*ConnPool)(nil)

// 使用工厂函数创建全局 Pool 对象，首先会初始化一个 ConnPool 实例，
// 初始化 PoolSize 大小的连接队列和轮转队列；然后在 checkMinIdleConns 方法中，
// 会根据 MinIdleConns 参数维持一个最小连接数，以保证连接池中有这么多数量的连接处于活跃状态
// （按照配置的最小空闲连接数，往池中添加MinIdleConns个连接）

// 注意到ConnPool.queue这个参数，该参数的作用是当client获取连接之前，
// 会向这个channel写入数据，如果能够写入说明不用等待就可以获取连接，
// 否则需要等待其他goroutine从这个channel取走数据才可以获取连接，
// 假如获取连接成功的话，在用完的时候，要向这个channel取出一个struct{}；
// 不然的话就会让别人一直阻塞，go-redis用这种方法保证池中的连接数量不会超过poolsize；
// 这种实现也算是最简单的令牌桶算法了
func NewConnPool(opt *Options) *ConnPool {
	p := &ConnPool{
		opt: opt,

		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleConns: make([]*Conn, 0, opt.PoolSize),
		closedCh:  make(chan struct{}),
	}

	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()
	// TODO(DDD) 为什么还需要判断 IdleTimeout ?
	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		// 如果配置了空闲连接超时时间 && 检查空闲连接频率
		// 则单独开启一个清理空闲连接的协程（定期）
		// 无限循环判断连接是否过期，过期的连接清理掉
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

// 根据 MinIdleConns 参数，初始化即创建指定数量的连接
func (p *ConnPool) checkMinIdleConns() {
	// 最少的空闲连接数为0，就是表示不需要空闲的连接。
	if p.opt.MinIdleConns == 0 {
		return
	}
	// 如果连接池中空闲连接数小于最小空闲连接数，则补充连接
	// 如果连接池中的 连接数已经达到最大连接数，则不再补充连接。【尽管此时没有空闲连接了，也不需要补充。】
	//
	// 这里是for循环，每个循环起一个协程，用于创建连接。
	for p.poolSize < p.opt.PoolSize && p.idleConnsLen < p.opt.MinIdleConns {
		// 调用 checkMinIdleConns 的地方已经加锁了。
		p.poolSize++
		p.idleConnsLen++

		go func() {
			// addIdleConn newConn 都用到了  dialConn 来创建新链接。
			err := p.addIdleConn()
			// 创建失败。回退之前的加操作。
			if err != nil && err != ErrClosed {
				p.connsMu.Lock()
				p.poolSize--
				p.idleConnsLen--
				p.connsMu.Unlock()
			}
		}()
	}
}

// 空闲连接的建立，向连接池中加新连接
func (p *ConnPool) addIdleConn() error {
	// 创建一个新连接
	cn, err := p.dialConn(context.TODO(), true)
	if err != nil {
		return err
	}
	// conns idleConns 是共享资源，需要加锁控制
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// It is not allowed to add new connections to the closed connection pool.
	if p.closed() {
		_ = cn.Close()
		return ErrClosed
	}

	p.conns = append(p.conns, cn)         // 增加连接队列
	p.idleConns = append(p.idleConns, cn) // 增加轮转队列
	return nil
}

// NewConn 到处给redis.go 用，在cluster sentinel都用到了。
// 和 conn 的区别是，conn是包装，而pool的NewConn是创建一个新链接，而
// pool的N ewConn 用conn NewConn 来包装 net.Conn添加 其他的信息（使用时间，创建时间。）
func (p *ConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.newConn(ctx, false)
}

// 通过 dialConn 方法生成新建的连接，注意参数 cn.pooled，通常情况下新建的连接会插入到连接池的 conns 队列中，
// 当发现连接池的大小超出了设定的连接大小时，这时候新建的连接的 cn.pooled 属性被设置为 false，
// 该连接未来将会被删除，不会落入连接池
func (p *ConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	// 拨号
	cn, err := p.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// It is not allowed to add new connections to the closed connection pool.
	if p.closed() {
		_ = cn.Close()
		return nil, ErrClosed
	}
	//conns 保存所有新建连接
	p.conns = append(p.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}

	return cn, nil
}

func (p *ConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	// 链接失败的次数。
	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}
	// 生成连接。【在集群模式下，会有很多redis实例。 】
	netConn, err := p.opt.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		// 在pool size 快慢的时候，再多尝试几次。tryDial
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}
	// 包装一下。
	cn := NewConn(netConn)
	cn.pooled = pooled
	return cn, nil
}

func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.opt.Dialer(context.Background())
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (p *ConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

// Get returns existed connection from the pool or creates a new one.

func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	// 如果能够写入说明不用等待就可以获取连接，否则需要等待其他地方从这个chan取走数据才可以获取连接，假如获取连接成功的话，在用完的时候，要向这个chan取出一个struct{}，不然的话就会让别人一直阻塞（如果在pooltimeout时间内没有等待到，就会超时返回），go-redis用这种方法保证池中的连接数量不会超过poolsize
	// 等候获取queue中的令牌 【不是error就可以获取到。】
	if err := p.waitTurn(ctx); err != nil {
		return nil, err
	}
	// for 循环 尝试拿一个连接出来。
	for {
		p.connsMu.Lock()
		// 从slice中拿出一个连接，通过popIdle方法的封装，可以完成不同的取连接的方式。
		// 虽然写在这里也行，但是封装还是更好的。
		cn, err := p.popIdle()
		p.connsMu.Unlock()

		if err != nil {
			return nil, err
		}

		if cn == nil {
			break
		}

		// 如果连接已经过期，那么强行关闭此连接，然后重新从空闲队列中获取（判断从空闲连接切片中拿出来的连接是否过期，兜底）
		if p.isStaleConn(cn) {
			_ = p.CloseConn(cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		// 从池中无法获取连接，则新建一个连接，如果没有返回错误则直接返回，如果新建连接时返回错误，则释放掉轮转队列中的位置，返回连接错误
		//如果没有空闲连接的话，就重新拨号建一个
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	newcn, err := p.newConn(ctx, true)
	if err != nil {
		p.freeTurn()
		return nil, err
	}

	return newcn, nil
}

// queue这里的功能固定数量的令牌桶（获取conn连接的令牌），用之前拿，用完之后放回，不会增加令牌数量也不会减少
func (p *ConnPool) getTurn() {
	p.queue <- struct{}{}
}

// 在每次真正执行操作之前，client 都会调用 ConnPool 的 Get 方法，在此Get 方法中实现了连接的创建和获取。可以看到，每次取出连接都是以出栈的方式取出切片里的最后一个空闲连接。从连接池中获取一个连接的过程如下：

// 1、首先，检查连接池是否被关闭，如果被关闭直接返回 ErrClosed 错误

// 2、尝试在轮转队列 queue 中占据一个位置（即尝试申请一个令牌），如果抢占的等待时间超过连接池的超时时间，会返回 ErrPoolTimeout 错误，见下面的 waitTurn 方法

// 轮转队列的主要作用是协调连接池的生产 - 消费过程，即使用令牌机制保证每往轮转队列中添加一个元素时，可用的连接资源的数量就减少一。若无法立即写入，该过程将尝试等待 PoolTimeout 时间后，返回相应结果。

// 注意下面的 timers 基于 sync.Pool 做了优化。
func (p *ConnPool) waitTurn(ctx context.Context) error {
	// 检查上下文是否已取消：如果上下文已取消，则立即返回取消错误 ctx.Err()。
	// default 分支：如果上下文未取消，继续执行后续逻辑。
	//【 这里的select相当于if判断，判断一下，然后走defualt， 然后在往下执行。】
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	// 尝试放入队列：尝试将一个空的结构体放入队列 p.queue。
	// 如果成功放入队列（即连接池有空闲连接），则返回 nil 表示成功。
	// default 分支：如果队列已满（即连接池没有空闲连接），继续执行后续逻辑。
	// 如果可以放入，就代表获取成功。
	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}
	// 设置定时器以等待超时
	// 下面的select顺序判断执行case:
	// 检查上下文是否已取消：
	//  如果上下文已取消，停止定时器并返回取消错误 ctx.Err()。
	//  如果定时器未能停止（可能定时器已触发），则读取定时器通道以防止泄漏。
	//  将定时器放回池中。
	// 尝试放入队列：
	//  如果成功放入队列，表示获取了连接，停止定时器并返回 nil 表示成功。
	//  如果定时器未能停止（可能定时器已触发），则读取定时器通道以防止泄漏。
	//  将定时器放回池中。
	// 等待超时：
	// 	如果定时器通道触发，表示等待超时，增加超时统计计数并返回超时错误 ErrPoolTimeout。
	//  将定时器放回池中。

	// 【在执行的执行下面的select语句的时候，
	//  如果指定到第二个case没有写入成功，
	//  然后继续往下执行，然后第三个也没有成功，
	//  那么select就会等待在这几个通道上面。
	//  此时，定时器到了，还没有可以往queue写入，
	//  则表示等待可用的连接超时。】
	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		// 表示复用定时器。
		timers.Put(timer)
		return ctx.Err()
	case p.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		timers.Put(timer)
		// 统计超时的次数。
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

// 放回令牌 【从q.queue中读取。】
func (p *ConnPool) freeTurn() {
	<-p.queue
}

// 通过 popIdle 方法，尝试从连接池的空闲连接队列 p.idleConns 中获取一个已有连接，
// 如果该连接已过期，则关闭并丢弃该连接，继续重复相同尝试操作，直至获取到一个连接或连接队列为空为止
func (p *ConnPool) popIdle() (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	n := len(p.idleConns)
	if n == 0 {
		return nil, nil
	}

	var cn *Conn
	// 这个是后面的优化。
	// 队列的数据结构。
	if p.opt.PoolFIFO {
		cn = p.idleConns[0]
		// 将 p.idleConns[1:] copy到 p.idleConns
		// 然后将最后的一个，丢弃掉。让gc回收。
		copy(p.idleConns, p.idleConns[1:])
		p.idleConns = p.idleConns[:n-1]
	} else {
		// 栈的数据结构（LIFO），取最后一个连接
		idx := n - 1
		cn = p.idleConns[idx]
		// 【写起来比上面简单。】
		p.idleConns = p.idleConns[:idx]
	}
	p.idleConnsLen--
	//  每次都强行补充连接至 MinIdleConns
	p.checkMinIdleConns()
	return cn, nil
}

// 从 ConnPool 中取出的连接一般来说都是要放回到连接池中的，具备包含 2 点：

// 直接向空闲连接队列中插入这个连接，并把轮转队列的资源释放掉
// 若连接的标记 cn.pooled 为不要被池化，则会直接释放这个连接

//连接用完之后（获取服务端响应后），要放回Pool中，最后放回令牌；一般连接用完之后都是放回空闲连接切片里

func (p *ConnPool) Put(ctx context.Context, cn *Conn) {
	if cn.rd.Buffered() > 0 {
		internal.Logger.Printf(ctx, "Conn has unread data")
		p.Remove(ctx, cn, BadConnError{})
		return
	}

	if !cn.pooled {
		p.Remove(ctx, cn, nil)
		return
	}

	// 连接队列上锁，将该连接加入空闲连接队列中，连接队列解锁，工作连接通道移除一个元素
	p.connsMu.Lock()
	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
	p.connsMu.Unlock()
	p.freeTurn()
}

// 删除方法 removeConn 会从连接池的 conns 队列中移除这个连接，在 ConnPool 的实现中，
// 移除（Remove）和关闭连接（CloseConn），底层调用的都是 removeConnWithLock 函数，
// 删除的方法是比较指针的值，然后进行 slice 的删除操作：
func (p *ConnPool) Remove(ctx context.Context, cn *Conn, reason error) {
	p.removeConnWithLock(cn)
	p.freeTurn()
	_ = p.closeConn(cn)
}

func (p *ConnPool) CloseConn(cn *Conn) error {
	p.removeConnWithLock(cn)
	return p.closeConn(cn)
}

func (p *ConnPool) removeConnWithLock(cn *Conn) {
	p.connsMu.Lock()
	p.removeConn(cn)
	p.connsMu.Unlock()
}

func (p *ConnPool) removeConn(cn *Conn) {
	// 遍历连接队列找到要关闭的连接，并将其移除出连接队列

	for i, c := range p.conns {
		// 比较指针的值

		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			if cn.pooled {
				// 如果 cn 这个连接是在 pool 中的，更新连接池统计数据
				p.poolSize--
				// 检查连接池最小空闲连接数量，如果不满足最小值，需要异步补充

				p.checkMinIdleConns()
			}
			return
		}
	}
}

// 关闭连接

func (p *ConnPool) closeConn(cn *Conn) error {
	// 若定义了回调函数（创建连接池时的配置选项传入）

	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(cn)
	}
	// 真正关闭连接

	return cn.Close()
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (p *ConnPool) IdleLen() int {
	p.connsMu.Lock()
	// conn 没有len变量。
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

func (p *ConnPool) Stats() *Stats {
	idleLen := p.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns: uint32(p.Len()),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *ConnPool) Filter(fn func(*Conn) bool) error {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	var firstErr error
	for _, cn := range p.conns {
		if fn(cn) {
			if err := p.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (p *ConnPool) Close() error {
	// 原子性检查连接池是否已经关闭，若没关闭，则将关闭标志置为 1

	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}

	// 关闭 closedCh 通道
	// 连接池中的所有协程都可以通过判断该通道是否关闭来确定连接池是否已经关闭
	close(p.closedCh)

	// 连接队列锁上锁，关闭队列中的所有连接，并置空所有维护连接池状态的数据结构
	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	p.connsMu.Unlock()

	return firstErr
}

// 连接池是怎么自动收割长时间不使用的空闲连接的？
// 注意到在 NewConnPool 方法中，若传入的 Option 配置了空闲连接超时和检查空闲连接频率，
// 则新启动一个用于检查并清理过期连接的 goroutine，
// 每隔 frequency 时间遍历检查（并取出）连接池中是否存在过期连接，
// 对过期连接做删除和关闭连接操作，并释放轮转资源。其代码如下：

func (p *ConnPool) reaper(frequency time.Duration) {
	//frequency 指定了多久进行一次检查，这里直接作为定时器 ticker 的触发间隔
	//无限循环判断连接是否过期，过期的连接清理掉
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		// 循环判断计时器是否到时

		case <-ticker.C:
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if p.closed() {
				return
			}
			// 移除空闲连接队列中的过期连接
			_, err := p.ReapStaleConns()
			if err != nil {
				internal.Logger.Printf(context.Background(), "ReapStaleConns failed: %s", err)
				continue
			}
			// 连接池是否关闭
			//pool 的结束标志触发，子协程退出常用范式
		case <-p.closedCh:
			return
		}
	}
}

// 移除空闲连接队列中的过期连接，无限循环判断连接是否过期
func (p *ConnPool) ReapStaleConns() (int, error) {
	var n int
	for {
		// 先获取令牌：需要向queue chan写进数据才能往下执行，否则就会阻塞，等queue有容量
		p.getTurn()

		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()

		// 用完之后，就要从queue chan读取出放进去的数据，让queue有容量写入
		p.freeTurn()

		if cn != nil {
			_ = p.closeConn(cn)
			n++
		} else {
			break
		}
	}
	atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	return n, nil
}

// 清理过期连接的实现比较简单，每次从idleConns的切片头部取出一个来判断是否过期，如果过期的话，更新idleConns，并且关闭过期连接（连接过期时间可以通过参数设置）
// 每次总idleConns的切片头部取出一个来判断是否过期,如果过期的话，更新idleConns，并且关闭过期连接
func (p *ConnPool) reapStaleConn() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	cn := p.idleConns[0]
	if !p.isStaleConn(cn) {
		return nil
	}

	p.idleConns = append(p.idleConns[:0], p.idleConns[1:]...)
	p.idleConnsLen--
	p.removeConn(cn)

	return cn
}

// isStaleConn方法，根据连接的出生时间点和上次使用的时间点，判断该Tcp连接是否过期
// TODO tcp 连接过期是什么意思？
func (p *ConnPool) isStaleConn(cn *Conn) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxConnAge == 0 {
		return connCheck(cn.netConn) != nil
	}

	now := time.Now()
	// 这两个太严格了。但是感觉应该还是有用的，因为需要检测链接存活的情况，不可能一直keeplive
	// 判断连接是否过期  【先长时间没用的。 再移除创建时间过长的连接。】
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		return true
	}

	if p.opt.MaxConnAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxConnAge {
		return true
	}
	// 【更新的内容。】
	return connCheck(cn.netConn) != nil
}
