package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
	"github.com/go-redis/redis/v8/internal/hashtag"
	"github.com/go-redis/redis/v8/internal/pool"
	"github.com/go-redis/redis/v8/internal/proto"
	"github.com/go-redis/redis/v8/internal/rand"
)

var errClusterNoNodes = fmt.Errorf("redis: cluster has no nodes")

// ClusterOptions are used to configure a cluster client and should be
// passed to NewClusterClient.
// 创建一个集群的 redis client
type ClusterOptions struct {
	// A seed list of host:port addresses of cluster nodes.
	Addrs []string

	// NewClient creates a cluster node client with provided name and options.
	NewClient func(opt *Options) *Client

	// The maximum number of retries before giving up. Command is retried
	// on network errors and MOVED/ASK redirects.
	// Default is 3 retries.
	MaxRedirects int
	// 下面这几个命令都是对只读命令的配置。
	// Enables read-only commands on slave nodes.
	ReadOnly bool
	// Allows routing read-only commands to the closest master or slave node.
	// It automatically enables ReadOnly.
	RouteByLatency bool
	// Allows routing read-only commands to the random master or slave node.
	// It automatically enables ReadOnly.
	RouteRandomly bool

	// Optional function that returns cluster slots information.
	// It is useful to manually create cluster of standalone Redis servers
	// and load-balance read/write operations between master and slaves.
	// It can use service like ZooKeeper to maintain configuration information
	// and Cluster.ReloadState to manually trigger state reloading.
	ClusterSlots func(context.Context) ([]ClusterSlot, error)

	// Following options are copied from Options struct.

	Dialer func(ctx context.Context, network, addr string) (net.Conn, error)

	OnConnect func(ctx context.Context, cn *Conn) error

	Username string
	Password string

	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// PoolFIFO uses FIFO mode for each node connection pool GET/PUT (default LIFO).
	PoolFIFO bool

	// PoolSize applies per cluster node and not for the whole cluster.
	PoolSize           int
	MinIdleConns       int
	MaxConnAge         time.Duration
	PoolTimeout        time.Duration
	IdleTimeout        time.Duration
	IdleCheckFrequency time.Duration

	TLSConfig *tls.Config
}

func (opt *ClusterOptions) init() {
	if opt.MaxRedirects == -1 {
		opt.MaxRedirects = 0
	} else if opt.MaxRedirects == 0 {
		opt.MaxRedirects = 3
	}

	if opt.RouteByLatency || opt.RouteRandomly {
		opt.ReadOnly = true
	}

	if opt.PoolSize == 0 {
		opt.PoolSize = 5 * runtime.GOMAXPROCS(0)
	}

	switch opt.ReadTimeout {
	case -1:
		opt.ReadTimeout = 0
	case 0:
		opt.ReadTimeout = 3 * time.Second
	}
	switch opt.WriteTimeout {
	case -1:
		opt.WriteTimeout = 0
	case 0:
		opt.WriteTimeout = opt.ReadTimeout
	}

	if opt.MaxRetries == 0 {
		opt.MaxRetries = -1
	}
	switch opt.MinRetryBackoff {
	case -1:
		opt.MinRetryBackoff = 0
	case 0:
		opt.MinRetryBackoff = 8 * time.Millisecond
	}
	switch opt.MaxRetryBackoff {
	case -1:
		opt.MaxRetryBackoff = 0
	case 0:
		opt.MaxRetryBackoff = 512 * time.Millisecond
	}

	if opt.NewClient == nil {
		// NewClient 是redis包中的创建单机RedisClient的函数。
		opt.NewClient = NewClient
	}
}

// 返回单个redis的配置。关于认证信息。 网络配置。连接池配置。证书等信息。
func (opt *ClusterOptions) clientOptions() *Options {
	const disableIdleCheck = -1

	return &Options{
		Dialer:    opt.Dialer,
		OnConnect: opt.OnConnect,

		Username: opt.Username,
		Password: opt.Password,

		MaxRetries:      opt.MaxRetries,
		MinRetryBackoff: opt.MinRetryBackoff,
		MaxRetryBackoff: opt.MaxRetryBackoff,

		DialTimeout:  opt.DialTimeout,
		ReadTimeout:  opt.ReadTimeout,
		WriteTimeout: opt.WriteTimeout,

		PoolFIFO:           opt.PoolFIFO,
		PoolSize:           opt.PoolSize,
		MinIdleConns:       opt.MinIdleConns,
		MaxConnAge:         opt.MaxConnAge,
		PoolTimeout:        opt.PoolTimeout,
		IdleTimeout:        opt.IdleTimeout,
		IdleCheckFrequency: disableIdleCheck,

		TLSConfig: opt.TLSConfig,
		// If ClusterSlots is populated, then we probably have an artificial
		// cluster whose nodes are not in clustering mode (otherwise there isn't
		// much use for ClusterSlots config).  This means we cannot execute the
		// READONLY command against that node -- setting readOnly to false in such
		// situations in the options below will prevent that from happening.
		readOnly: opt.ReadOnly && opt.ClusterSlots == nil,
	}
}

// ------------------------------------------------------------------------------
// clusterNode：集群节点抽象。client为连接此节点的客户端，generation与clusterState的generation关联。
type clusterNode struct {
	Client *Client
	// 记录节点的延迟，可以配置只读命令从哪个节点读取数据。
	latency    uint32 // atomic
	generation uint32 // atomic
	// 判断节点是否下线。
	failing uint32 // atomic
}

// 就是对 client 的包装。
func newClusterNode(clOpt *ClusterOptions, addr string) *clusterNode {
	// 从cluster中读取client的配置。然后根据这个配置创建客户端。
	opt := clOpt.clientOptions()
	opt.Addr = addr
	node := clusterNode{
		Client: clOpt.NewClient(opt),
	}

	node.latency = math.MaxUint32
	if clOpt.RouteByLatency {
		go node.updateLatency()
	}

	return &node
}

func (n *clusterNode) String() string {
	return n.Client.String()
}

func (n *clusterNode) Close() error {
	return n.Client.Close()
}

func (n *clusterNode) updateLatency() {
	const numProbe = 10
	var dur uint64

	for i := 0; i < numProbe; i++ {
		time.Sleep(time.Duration(10+rand.Intn(10)) * time.Millisecond)

		start := time.Now()
		// 使用 ping 命令来检测延迟。
		n.Client.Ping(context.TODO())
		dur += uint64(time.Since(start) / time.Microsecond)
	}

	latency := float64(dur) / float64(numProbe)
	atomic.StoreUint32(&n.latency, uint32(latency+0.5))
}

func (n *clusterNode) Latency() time.Duration {
	latency := atomic.LoadUint32(&n.latency)
	return time.Duration(latency) * time.Microsecond
}

func (n *clusterNode) MarkAsFailing() {
	atomic.StoreUint32(&n.failing, uint32(time.Now().Unix()))
}

func (n *clusterNode) Failing() bool {
	const timeout = 15 // 15 seconds

	failing := atomic.LoadUint32(&n.failing)
	if failing == 0 {
		return false
	}
	// 如果没过这个 timeout 持续时间，则认为节点还在下线中。
	if time.Now().Unix()-int64(failing) < timeout {
		return true
	}
	atomic.StoreUint32(&n.failing, 0)
	return false
}

func (n *clusterNode) Generation() uint32 {
	return atomic.LoadUint32(&n.generation)
}

func (n *clusterNode) SetGeneration(gen uint32) {
	for {
		v := atomic.LoadUint32(&n.generation)
		if gen < v || atomic.CompareAndSwapUint32(&n.generation, v, gen) {
			break
		}
	}
}

// ------------------------------------------------------------------------------
// clusterNodes：维护集群内「实例地址=>节点」的映射
type clusterNodes struct {
	opt *ClusterOptions

	mu sync.RWMutex
	// 所有的集群连接的地址。
	addrs []string
	// ip:port到节点的映射。
	//
	nodes map[string]*clusterNode
	// 会定期移除节点，通过 clusterNode 中的 generation
	activeAddrs []string
	closed      bool

	_generation uint32 // atomic
}

func newClusterNodes(opt *ClusterOptions) *clusterNodes {
	return &clusterNodes{
		opt: opt,

		addrs: opt.Addrs,
		nodes: make(map[string]*clusterNode),
	}
}

func (c *clusterNodes) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	var firstErr error
	for _, node := range c.nodes {
		if err := node.Client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	c.nodes = nil
	c.activeAddrs = nil

	return firstErr
}

func (c *clusterNodes) Addrs() ([]string, error) {
	var addrs []string

	c.mu.RLock()
	closed := c.closed //nolint:ifshort
	if !closed {
		if len(c.activeAddrs) > 0 {
			addrs = c.activeAddrs
		} else {
			addrs = c.addrs
		}
	}
	c.mu.RUnlock()

	if closed {
		return nil, pool.ErrClosed
	}
	if len(addrs) == 0 {
		return nil, errClusterNoNodes
	}
	return addrs, nil
}

func (c *clusterNodes) NextGeneration() uint32 {
	return atomic.AddUint32(&c._generation, 1)
}

// GC removes unused nodes.
/*
 GC 方法的条件是根据传入的 generation 参数判断节点是否仍在使用，如果节点的生成次数小于传入的 generation ，则将其视为未使用节点，删除并关闭其客户端连接。这样可以及时清理未使用的节点，释放资源并保持系统的稳定性。
*/
func (c *clusterNodes) GC(generation uint32) {
	//nolint:prealloc
	var collected []*clusterNode

	c.mu.Lock()

	c.activeAddrs = c.activeAddrs[:0]
	for addr, node := range c.nodes {
		if node.Generation() >= generation {
			c.activeAddrs = append(c.activeAddrs, addr)
			if c.opt.RouteByLatency {
				go node.updateLatency()
			}
			continue
		}

		delete(c.nodes, addr)
		collected = append(collected, node)
	}

	c.mu.Unlock()

	for _, node := range collected {
		_ = node.Client.Close()
	}
}

// 用到的时候，才创建。
func (c *clusterNodes) GetOrCreate(addr string) (*clusterNode, error) {
	node, err := c.get(addr)
	if err != nil { // 两个if判断的过程中，可能有别的节点创建了具体的node. [退出get方法，就释放了锁。]
		return nil, err
	}
	if node != nil {
		return node, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, pool.ErrClosed
	}

	node, ok := c.nodes[addr] // 这个地方为什么还需要重新判断一遍呢？get方法中已经判断过了。
	if ok {
		return node, nil
	}
	// 创建集群中的一个节点。
	node = newClusterNode(c.opt, addr)

	c.addrs = appendIfNotExists(c.addrs, addr)
	// 根据 id:port确定一个node.
	c.nodes[addr] = node

	return node, nil
}

func (c *clusterNodes) get(addr string) (*clusterNode, error) {
	var node *clusterNode
	var err error
	c.mu.RLock()
	if c.closed {
		err = pool.ErrClosed
	} else {
		node = c.nodes[addr]
	}
	c.mu.RUnlock()
	return node, err
}

func (c *clusterNodes) All() ([]*clusterNode, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, pool.ErrClosed
	}

	cp := make([]*clusterNode, 0, len(c.nodes))
	for _, node := range c.nodes {
		cp = append(cp, node)
	}
	return cp, nil
}

func (c *clusterNodes) Random() (*clusterNode, error) {
	addrs, err := c.Addrs()
	if err != nil {
		return nil, err
	}

	n := rand.Intn(len(addrs))
	return c.GetOrCreate(addrs[n])
}

// ------------------------------------------------------------------------------
// clusterSlot：一个范围的哈希槽以及负责这些槽的节点（第一个为主节点，其余为从节点），
// start为起始哈希槽编号，end为起结束希槽编号，nodes为负责这些槽的节点。
// 【其中一个节点，参与集群，其余的节点作为该节点的备份参与。】
type clusterSlot struct {
	start, end int
	nodes      []*clusterNode
}

type clusterSlotSlice []*clusterSlot

func (p clusterSlotSlice) Len() int {
	return len(p)
}

func (p clusterSlotSlice) Less(i, j int) bool {
	return p[i].start < p[j].start
}

func (p clusterSlotSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

// clusterState：维护集群内「哈希槽=>节点」的映射，创建后不可修改，只能通过新建替换更新，每次新建 generation 自增。createdAt 为创建时间。
// 包含所有的节点，然后包含主节点和从节点。
type clusterState struct {
	nodes   *clusterNodes
	Masters []*clusterNode
	Slaves  []*clusterNode
	// 哈希槽段到节点的映射。包含槽段和节点的信息。
	slots []*clusterSlot

	generation uint32
	createdAt  time.Time
}

// 工厂函数，创建一个 ClusterState 在这里设置集群的代数，初始的代数，从nodes中获取到。
func newClusterState(
	nodes *clusterNodes, slots []ClusterSlot, origin string,
) (*clusterState, error) {
	// 在函数内部，首先初始化了一个 clusterState 对象 c ，
	// 设置了节点列表、槽信息、生成次数、创建时间等字段的初始值。
	// 然后从源地址中提取主机名，并判断是否为回环地址。
	c := clusterState{
		nodes: nodes,

		slots: make([]*clusterSlot, 0, len(slots)),
		// 从nodes中获取。
		generation: nodes.NextGeneration(),
		createdAt:  time.Now(), // 创建时间判断，state的更新的状态。
	}
	// 从origin 中获取ip的信息。
	originHost, _, _ := net.SplitHostPort(origin)
	// 在函数内部，首先初始化了一个 clusterState 对象 c ，设置了节点列表、槽信息、生成次数、创建时间等字段的初始值。然后从源地址中提取主机名，并判断是否为回环地址。
	// 回环地址（Loopback Address）是指计算机网络中的一种特殊的IP地址，用于将数据包发送给本地主机而不经过网络通信。在IPv4中，回环地址通常表示为127.0.0.1，而在IPv6中，回环地址通常表示为::1。
	isLoopbackOrigin := isLoopback(originHost)

	for _, slot := range slots {
		// var 声明一个局部变量。
		var nodes []*clusterNode
		for i, slotNode := range slot.Nodes {
			addr := slotNode.Addr
			if !isLoopbackOrigin {
				addr = replaceLoopbackHost(addr, originHost)
			}
			// 如果已经创建，直接返回即可。
			node, err := c.nodes.GetOrCreate(addr)
			if err != nil {
				return nil, err
			}
			// 从集群状态中，获取创建时候的代数。
			node.SetGeneration(c.generation)
			nodes = append(nodes, node)
			// 第一个为master节点，后续的为salve节点。
			if i == 0 {
				c.Masters = appendUniqueNode(c.Masters, node)
			} else {
				c.Slaves = appendUniqueNode(c.Slaves, node)
			}
		}

		c.slots = append(c.slots, &clusterSlot{
			start: slot.Start,
			end:   slot.End,
			nodes: nodes,
		})
	}

	sort.Sort(clusterSlotSlice(c.slots))

	time.AfterFunc(time.Minute, func() {
		nodes.GC(c.generation)
	})

	return &c, nil
}

func replaceLoopbackHost(nodeAddr, originHost string) string {
	nodeHost, nodePort, err := net.SplitHostPort(nodeAddr)
	if err != nil {
		return nodeAddr
	}

	nodeIP := net.ParseIP(nodeHost)
	if nodeIP == nil {
		return nodeAddr
	}

	if !nodeIP.IsLoopback() {
		return nodeAddr
	}

	// Use origin host which is not loopback and node port.
	return net.JoinHostPort(originHost, nodePort)
}

func isLoopback(host string) bool {
	ip := net.ParseIP(host)
	if ip == nil {
		return true
	}
	return ip.IsLoopback()
}

// 根据 slot 找到负责这个槽段的节点（节点组成： 一个主节点参与集群，多个从节点参与主节点的备份。）
// 第一个节点就是master节点。
func (c *clusterState) slotMasterNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	if len(nodes) > 0 {
		return nodes[0], nil
	}
	// 为什么是随机创建一个节点？
	// [TODO(DDD)]有说法的。
	return c.nodes.Random()
}

// "All slaves are loading" 这个状态表示在集群中所有的从节点都处于负载过高的状态，无法正常处理请求。这种情况可能出现在集群负载过大、从节点资源不足、网络问题等情况下。
func (c *clusterState) slotSlaveNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	switch len(nodes) {
	case 0:
		return c.nodes.Random()
	case 1:
		return nodes[0], nil
	case 2:
		if slave := nodes[1]; !slave.Failing() {
			return slave, nil
		}
		return nodes[0], nil
	default: // 并且所有的从节点都处于负载过高的状态（即都处于 Failing() 状态），则会进入 default 分支。在这种情况下，代码会尝试随机选择一个非负载过高的从节点，最多尝试10次。如果所有的从节点都处于负载过高状态，那么最终会选择使用主节点来处理请求。
		var slave *clusterNode
		for i := 0; i < 10; i++ {
			n := rand.Intn(len(nodes)-1) + 1
			slave = nodes[n]
			if !slave.Failing() {
				return slave, nil
			}
		}

		// All slaves are loading - use master.
		return nodes[0], nil
	}
}

func (c *clusterState) slotClosestNode(slot int) (*clusterNode, error) {
	// 找到槽段对应的节点。
	nodes := c.slotNodes(slot)
	if len(nodes) == 0 {
		// 随机返回一个节点，对应的操作不在这个节点上怎么处理呢？
		return c.nodes.Random()
	}

	var node *clusterNode
	for _, n := range nodes {
		if n.Failing() {
			continue
		}
		// 找到槽段中延迟最小的节点。
		if node == nil || n.Latency() < node.Latency() {
			node = n
		}
	}
	if node != nil {
		return node, nil
	}

	// If all nodes are failing - return random node
	return c.nodes.Random()
}

func (c *clusterState) slotRandomNode(slot int) (*clusterNode, error) {
	nodes := c.slotNodes(slot)
	if len(nodes) == 0 {
		return c.nodes.Random()
	}
	if len(nodes) == 1 {
		return nodes[0], nil
	}
	randomNodes := rand.Perm(len(nodes))
	for _, idx := range randomNodes {
		if node := nodes[idx]; !node.Failing() {
			return node, nil
		}
	}
	return nodes[randomNodes[0]], nil
}

func (c *clusterState) slotNodes(slot int) []*clusterNode {
	// 找到比传入slot大的槽位对应的节点。
	i := sort.Search(len(c.slots), func(i int) bool {
		// 谓词函数。
		return c.slots[i].end >= slot // 是下一个槽的start
	})
	if i >= len(c.slots) {
		return nil
	}
	// 根据slot 找到对应的节点。【每个】
	x := c.slots[i]
	if slot >= x.start && slot <= x.end {
		return x.nodes
	}
	return nil
}

//------------------------------------------------------------------------------
/*
-  load ：一个函数类型，用于在给定的上下文中加载集群状态并返回。
-  state ：一个 atomic.Value 类型，用于存储集群状态。
-  reloading ：一个 uint32 类型，用于表示是否正在重新加载集群状态的标志，采用原子操作保证并发安全。

*/
type clusterStateHolder struct {
	load func(ctx context.Context) (*clusterState, error)

	state     atomic.Value
	reloading uint32 // atomic
}

// 就传入一个 fn，该fn返回 clusterState
func newClusterStateHolder(fn func(ctx context.Context) (*clusterState, error)) *clusterStateHolder {
	return &clusterStateHolder{
		// 设置如何加载集群的状态。
		load: fn,
	}
}

// 重新加载集群的信息  load 方法中就是随机选择一个集群节点，获取集群的状态信息。
func (c *clusterStateHolder) Reload(ctx context.Context) (*clusterState, error) {
	state, err := c.load(ctx)
	if err != nil {
		return nil, err
	}
	// 存储在 atomic.Value 中。
	c.state.Store(state)
	return state, nil
}

/*
这段代码中的Lazy设计体现在 LazyReload 函数中的goroutine调用。
在该函数中，首先通过 atomic.CompareAndSwapUint32 方法来判断是否可以进行reload操作，
如果可以则将 c.reloading 的值从0更新为1，然后启动一个goroutine执行reload操作。
在goroutine中，会调用 c.Reload 方法进行实际的reload操作，
然后通过 time.Sleep(200 * time.Millisecond) 来模拟延迟，
这里的延迟就是Lazy设计的一部分。

Lazy设计的目的是延迟加载或延迟执行操作，以提高性能或节省资源。
*/
func (c *clusterStateHolder) LazyReload() {
	//
	if !atomic.CompareAndSwapUint32(&c.reloading, 0, 1) {
		return
	}
	go func() {
		defer atomic.StoreUint32(&c.reloading, 0)

		_, err := c.Reload(context.Background())
		if err != nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}()
}

func (c *clusterStateHolder) Get(ctx context.Context) (*clusterState, error) {
	v := c.state.Load()
	if v == nil {
		return c.Reload(ctx)
	}

	state := v.(*clusterState)
	// 已经超过10秒没有更新了，就需要更新集群信息。
	if time.Since(state.createdAt) > 10*time.Second {
		c.LazyReload()
	}
	return state, nil
}

// 重新获取没有获取到，然后使用历史的集群状态。
func (c *clusterStateHolder) ReloadOrGet(ctx context.Context) (*clusterState, error) {
	state, err := c.Reload(ctx)
	if err == nil {
		return state, nil
	}
	// 历史的状态。
	return c.Get(ctx)
}

//------------------------------------------------------------------------------

type clusterClient struct {
	opt *ClusterOptions
	// nodes和state有一种分离的感觉。
	// clusterNodes 更多是维护到服务器节点的连接，
	// 而state，更多的事维护服务器（集群）的状态。
	// 客户端需要根据state来选择不同的服务器节点来执行相应的操作。
	// 并且go-redis还维护了节点的延迟，可以根据这些到服务器节点连接的状态信息，来选择执行不同的路由政策。
	nodes *clusterNodes
	state *clusterStateHolder //nolint:structcheck
	// redis服务器的所有的命令信息。
	cmdsInfoCache *cmdsInfoCache //nolint:structcheck
}

// ClusterClient is a Redis Cluster client representing a pool of zero
// or more underlying connections. It's safe for concurrent use by
// multiple goroutines.
// clusterClient：集群客户端抽象
type ClusterClient struct {
	*clusterClient
	cmdable
	hooks
	ctx context.Context
}

// NewClusterClient returns a Redis Cluster client as described in
// http://redis.io/topics/cluster-spec.
func NewClusterClient(opt *ClusterOptions) *ClusterClient {
	opt.init()

	c := &ClusterClient{
		clusterClient: &clusterClient{
			opt: opt,
			// 这里并没与创建具体的node,而是将传入的地址，
			// 记录在opt里面，并且创建ip:port 到node的map.
			nodes: newClusterNodes(opt),
		},
		ctx: context.Background(),
	}
	// 这里传入两个函数。cmdsInfo 获取所有cmd的信息。
	// cmdable也是一个函数。
	c.state = newClusterStateHolder(c.loadState)
	c.cmdsInfoCache = newCmdsInfoCache(c.cmdsInfo)
	c.cmdable = c.Process

	if opt.IdleCheckFrequency > 0 {
		go c.reaper(opt.IdleCheckFrequency)
	}

	return c
}

func (c *ClusterClient) Context() context.Context {
	return c.ctx
}

// 可以定义context，用于传递环境参数和控制超时时间。
// 创建一个cluster client 可以通过这个方法，来重新配置一个context，用于控制goroutine的超时、取消操作。
func (c *ClusterClient) WithContext(ctx context.Context) *ClusterClient {
	if ctx == nil {
		panic("nil context")
	}
	clone := *c
	clone.cmdable = clone.Process
	clone.hooks.lock()
	clone.ctx = ctx
	return &clone
}

// Options returns read-only Options that were used to create the client.
func (c *ClusterClient) Options() *ClusterOptions {
	return c.opt
}

// ReloadState reloads cluster state. If available it calls ClusterSlots func
// to get cluster slots information.
func (c *ClusterClient) ReloadState(ctx context.Context) {
	c.state.LazyReload()
}

// Close closes the cluster client, releasing any open resources.
//
// It is rare to Close a ClusterClient, as the ClusterClient is meant
// to be long-lived and shared between many goroutines.
func (c *ClusterClient) Close() error {
	return c.nodes.Close()
}

// Do creates a Cmd from the args and processes the cmd.
// 创建自定义的命令。
func (c *ClusterClient) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	_ = c.Process(ctx, cmd)
	return cmd
}

func (c *ClusterClient) Process(ctx context.Context, cmd Cmder) error {
	// 函数指针传入到 hooks 内。 c.process
	return c.hooks.process(ctx, cmd, c.process)
}

func (c *ClusterClient) process(ctx context.Context, cmd Cmder) error {
	// Name 返回命令的小写形式。【通过comand的信息获取的支持的cmd之后，才回去执行命令。】
	cmdInfo := c.cmdInfo(cmd.Name())
	slot := c.cmdSlot(cmd)
	// clusterNode 对 Client的封装。（client共享一个连接池吗？ ）
	var node *clusterNode
	var ask bool // for循环中可能被赋值。
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		// 尝试之前，需要随机等待一会儿。
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}

		if node == nil {
			var err error
			// 根据命令和slot找到对应的节点。
			// 并且根据cmd的信息，判断是否是只读命令，如果是只读命令，可以转发到对应槽位的slave节点。
			node, err = c.cmdNode(ctx, cmdInfo, slot)
			if err != nil {
				return err
			}
		}

		if ask {
			pipe := node.Client.Pipeline()
			_ = pipe.Process(ctx, NewCmd(ctx, "asking"))
			_ = pipe.Process(ctx, cmd)
			_, lastErr = pipe.Exec(ctx)
			_ = pipe.Close()
			ask = false
		} else {
			lastErr = node.Client.Process(ctx, cmd)
		}

		// If there is no error - we are done.
		if lastErr == nil {
			return nil
		}
		if isReadOnly := isReadOnlyError(lastErr); isReadOnly || lastErr == pool.ErrClosed {
			if isReadOnly {
				c.state.LazyReload()
			}
			node = nil
			continue
		}

		// If slave is loading - pick another node.
		if c.opt.ReadOnly && isLoadingError(lastErr) {
			node.MarkAsFailing()
			node = nil
			continue
		}

		var moved bool
		var addr string
		moved, ask, addr = isMovedError(lastErr)
		if moved || ask {
			c.state.LazyReload()

			var err error
			node, err = c.nodes.GetOrCreate(addr)
			if err != nil {
				return err
			}
			continue
		}

		if shouldRetry(lastErr, cmd.readTimeout() == nil) {
			// First retry the same node.
			if attempt == 0 {
				continue
			}

			// Second try another node.
			node.MarkAsFailing()
			node = nil
			continue
		}

		return lastErr
	}
	return lastErr
}

// ForEachMaster concurrently calls the fn on each master node in the cluster.
// It returns the first error if any.
func (c *ClusterClient) ForEachMaster(
	ctx context.Context,
	fn func(ctx context.Context, client *Client) error,
) error {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}
	// 需要同时在多个master上面执行同样的操作。
	// 传入fn代表要在master上执行的操作。
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for _, master := range state.Masters {
		wg.Add(1)
		go func(node *clusterNode) {
			defer wg.Done() // 如果发生错误，会再函数退出的时候，执行Done() 函数。
			err := fn(ctx, node.Client)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(master)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// ForEachSlave concurrently calls the fn on each slave node in the cluster.
// It returns the first error if any.
func (c *ClusterClient) ForEachSlave(
	ctx context.Context,
	fn func(ctx context.Context, client *Client) error,
) error {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	for _, slave := range state.Slaves {
		wg.Add(1)
		go func(node *clusterNode) {
			defer wg.Done()
			err := fn(ctx, node.Client)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(slave)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// ForEachShard concurrently calls the fn on each known node in the cluster.
// It returns the first error if any.
// 在每个节点上执行。
func (c *ClusterClient) ForEachShard(
	ctx context.Context,
	fn func(ctx context.Context, client *Client) error,
) error {
	state, err := c.state.ReloadOrGet(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	worker := func(node *clusterNode) {
		defer wg.Done()
		err := fn(ctx, node.Client)
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}

	for _, node := range state.Masters {
		wg.Add(1)
		go worker(node)
	}
	for _, node := range state.Slaves {
		wg.Add(1)
		go worker(node)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

// PoolStats returns accumulated connection pool stats.
func (c *ClusterClient) PoolStats() *PoolStats {
	var acc PoolStats

	state, _ := c.state.Get(context.TODO())
	if state == nil {
		return &acc
	}
	// 这里可以证明
	for _, node := range state.Masters {
		s := node.Client.connPool.Stats()
		acc.Hits += s.Hits
		acc.Misses += s.Misses
		acc.Timeouts += s.Timeouts

		acc.TotalConns += s.TotalConns
		acc.IdleConns += s.IdleConns
		acc.StaleConns += s.StaleConns
	}

	for _, node := range state.Slaves {
		s := node.Client.connPool.Stats()
		acc.Hits += s.Hits
		acc.Misses += s.Misses
		acc.Timeouts += s.Timeouts

		acc.TotalConns += s.TotalConns
		acc.IdleConns += s.IdleConns
		acc.StaleConns += s.StaleConns
	}

	return &acc
}

/*
1. 加载集群状态，go-redis 通过一个 clusterStateHolder 来完成延迟加载。（好设计呀。我的了。）
*/
func (c *ClusterClient) loadState(ctx context.Context) (*clusterState, error) {
	if c.opt.ClusterSlots != nil {
		// 调用cluster slots获取槽段信息。 node.Client.ClusterSlots(ctx).Result()
		// 槽段信息：节点。
		// opt 中的 ClusterSlots 是一个方法，可以用来获取槽位信息。（例如zookeeer）
		// 而下面的 ClusterSlots 就是利用 cluster的方法获取槽位信息的。（标准的获取方法。）
		slots, err := c.opt.ClusterSlots(ctx)
		if err != nil {
			return nil, err
		}
		return newClusterState(c.nodes, slots, "")
	}

	addrs, err := c.nodes.Addrs()
	if err != nil {
		return nil, err
	}

	var firstErr error

	for _, idx := range rand.Perm(len(addrs)) {
		addr := addrs[idx]

		node, err := c.nodes.GetOrCreate(addr)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// 使用 cluster slots 方法获取集群的信息。
		// slots 是 ClusterSlot
		slots, err := node.Client.ClusterSlots(ctx).Result()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// node.Client.opt.Addr 表示了当前的集群的state是从哪里获取的，并且可以在 newClusterState 中可以根据地址的信息，将其他集群中的节点的地址信息从域名转成ip地址。
		return newClusterState(c.nodes, slots, node.Client.opt.Addr)
	}

	/*
	 * No node is connectable. It's possible that all nodes' IP has changed.
	 * Clear activeAddrs to let client be able to re-connect using the initial
	 * setting of the addresses (e.g. [redis-cluster-0:6379, redis-cluster-1:6379]),
	 * which might have chance to resolve domain name and get updated IP address.
	 */
	c.nodes.mu.Lock()
	c.nodes.activeAddrs = nil
	c.nodes.mu.Unlock()

	return nil, firstErr
}

// reaper closes idle connections to the cluster.
func (c *ClusterClient) reaper(idleCheckFrequency time.Duration) {
	ticker := time.NewTicker(idleCheckFrequency)
	defer ticker.Stop()

	for range ticker.C {
		nodes, err := c.nodes.All()
		if err != nil {
			break
		}
		// 对集群中的节点所建立的连接进行清理。
		// ClusterClient是多个到集群中节点的连接的集合。
		for _, node := range nodes {
			_, err := node.Client.connPool.(*pool.ConnPool).ReapStaleConns()
			if err != nil {
				internal.Logger.Printf(c.Context(), "ReapStaleConns failed: %s", err)
			}
		}
	}
}

// 返回一个可以执行pipeline操作的管道客户端。
func (c *ClusterClient) Pipeline() Pipeliner {
	pipe := Pipeline{
		ctx:  c.ctx,
		exec: c.processPipeline,
	}
	pipe.init()
	return &pipe
}

func (c *ClusterClient) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipeline().Pipelined(ctx, fn)
}

func (c *ClusterClient) processPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processPipeline(ctx, cmds, c._processPipeline)
}

// 集群上的管道操作。
func (c *ClusterClient) _processPipeline(ctx context.Context, cmds []Cmder) error {
	// 一个节点和可能的多个命令的map
	cmdsMap := newCmdsMap()
	err := c.mapCmdsByNode(ctx, cmdsMap, cmds)
	if err != nil {
		setCmdsErr(cmds, err)
		return err
	}
	// 开始执行。
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				setCmdsErr(cmds, err)
				return err
			}
		}
		// 记录执行失败的命令。
		failedCmds := newCmdsMap()
		var wg sync.WaitGroup
		// 这里就是go的并发执行，将命令分发到对应的节点上执行。
		for node, cmds := range cmdsMap.m {
			wg.Add(1)
			go func(node *clusterNode, cmds []Cmder) {
				defer wg.Done()

				err := c._processPipelineNode(ctx, node, cmds, failedCmds)
				if err == nil {
					return
				}
				if attempt < c.opt.MaxRedirects {
					if err := c.mapCmdsByNode(ctx, failedCmds, cmds); err != nil {
						setCmdsErr(cmds, err)
					}
				} else {
					setCmdsErr(cmds, err)
				}
			}(node, cmds)
		}

		wg.Wait()
		if len(failedCmds.m) == 0 {
			break
		}
		cmdsMap = failedCmds
	}

	return cmdsFirstErr(cmds)
}

func (c *ClusterClient) mapCmdsByNode(ctx context.Context, cmdsMap *cmdsMap, cmds []Cmder) error {
	state, err := c.state.Get(ctx)
	if err != nil {
		return err
	}
	// 如果命令是只读命令，并且客户端配置了只读模式，那么只从从节点执行命令。
	if c.opt.ReadOnly && c.cmdsAreReadOnly(cmds) {
		for _, cmd := range cmds {
			// 获取 cmd 的 slot
			slot := c.cmdSlot(cmd)
			// 根据slot获取可读节点。
			node, err := c.slotReadOnlyNode(state, slot)
			if err != nil {
				return err
			}
			// 添加到对应的节点上。
			cmdsMap.Add(node, cmd)
		}
		return nil
	}
	// 这个地方为什么选择master节点，因为没有配置只读模式。
	for _, cmd := range cmds {
		slot := c.cmdSlot(cmd)
		node, err := state.slotMasterNode(slot)
		if err != nil {
			return err
		}
		cmdsMap.Add(node, cmd)
	}
	return nil
}

func (c *ClusterClient) cmdsAreReadOnly(cmds []Cmder) bool {
	for _, cmd := range cmds {
		cmdInfo := c.cmdInfo(cmd.Name())
		if cmdInfo == nil || !cmdInfo.ReadOnly {
			return false
		}
	}
	return true
}

func (c *ClusterClient) _processPipelineNode(
	ctx context.Context, node *clusterNode, cmds []Cmder, failedCmds *cmdsMap,
) error {
	return node.Client.hooks.processPipeline(ctx, cmds, func(ctx context.Context, cmds []Cmder) error {
		// clusterNode 找到底层的 clusterNode 执行读写操作。
		return node.Client.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
			err := cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
				return writeCmds(wr, cmds)
			})
			if err != nil {
				return err
			}

			return cn.WithReader(ctx, c.opt.ReadTimeout, func(rd *proto.Reader) error {
				return c.pipelineReadCmds(ctx, node, rd, cmds, failedCmds)
			})
		})
	})
}

// pipelineReadCmds 读取pipeline命令的结果。
func (c *ClusterClient) pipelineReadCmds(
	ctx context.Context,
	node *clusterNode,
	rd *proto.Reader,
	cmds []Cmder,
	failedCmds *cmdsMap,
) error {
	for _, cmd := range cmds {
		// 每个cmd都有自己的radReply函数。如果有响应的话，每个命令的响应，按照命令请求的顺序返回。
		err := cmd.readReply(rd)
		cmd.SetErr(err)

		if err == nil {
			continue
		}
		// 处理moved和ask错误。【可能在命令的执行过程中，存在slot切换的坑你吧。 】
		if c.checkMovedErr(ctx, cmd, err, failedCmds) {
			continue
		}

		if c.opt.ReadOnly && isLoadingError(err) {
			node.MarkAsFailing()
			return err
		}
		if isRedisError(err) {
			continue
		}
		return err
	}
	return nil
}

// 处理moved操作的函数。
func (c *ClusterClient) checkMovedErr(
	ctx context.Context, cmd Cmder, err error, failedCmds *cmdsMap,
) bool {
	// 判断是否是moved或者ask错误。 （isMovedError 通过前缀来判断）
	moved, ask, addr := isMovedError(err)
	if !moved && !ask {
		return false
	}
	// 获取节点。
	node, err := c.nodes.GetOrCreate(addr)
	if err != nil {
		return false
	}

	if moved {
		c.state.LazyReload()
		failedCmds.Add(node, cmd)
		return true
	}

	if ask {
		failedCmds.Add(node, NewCmd(ctx, "asking"), cmd)
		return true
	}
	// 这个地方，会直接panic，表示集群处于不可用的状态中。
	panic("not reached")
}

// TxPipeline acts like Pipeline, but wraps queued commands with MULTI/EXEC.
func (c *ClusterClient) TxPipeline() Pipeliner {
	pipe := Pipeline{
		ctx:  c.ctx,
		exec: c.processTxPipeline,
	}
	pipe.init()
	return &pipe
}

func (c *ClusterClient) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.TxPipeline().Pipelined(ctx, fn)
}

func (c *ClusterClient) processTxPipeline(ctx context.Context, cmds []Cmder) error {
	return c.hooks.processTxPipeline(ctx, cmds, c._processTxPipeline)
}

func (c *ClusterClient) _processTxPipeline(ctx context.Context, cmds []Cmder) error {
	// Trim multi .. exec.
	cmds = cmds[1 : len(cmds)-1]

	state, err := c.state.Get(ctx)
	if err != nil {
		setCmdsErr(cmds, err)
		return err
	}

	cmdsMap := c.mapCmdsBySlot(cmds)
	for slot, cmds := range cmdsMap {
		node, err := state.slotMasterNode(slot)
		if err != nil {
			setCmdsErr(cmds, err)
			continue
		}

		cmdsMap := map[*clusterNode][]Cmder{node: cmds}
		for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
			if attempt > 0 {
				if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
					setCmdsErr(cmds, err)
					return err
				}
			}

			failedCmds := newCmdsMap()
			var wg sync.WaitGroup

			for node, cmds := range cmdsMap {
				wg.Add(1)
				go func(node *clusterNode, cmds []Cmder) {
					defer wg.Done()

					err := c._processTxPipelineNode(ctx, node, cmds, failedCmds)
					if err == nil {
						return
					}

					if attempt < c.opt.MaxRedirects {
						if err := c.mapCmdsByNode(ctx, failedCmds, cmds); err != nil {
							setCmdsErr(cmds, err)
						}
					} else {
						setCmdsErr(cmds, err)
					}
				}(node, cmds)
			}

			wg.Wait()
			if len(failedCmds.m) == 0 {
				break
			}
			cmdsMap = failedCmds.m
		}
	}

	return cmdsFirstErr(cmds)
}

// 按slot分配命令，mapCmdsByNode 按 node 分组。（本质也是按slot分组，然后将同一个slot的归到同一个节点上。 ）
func (c *ClusterClient) mapCmdsBySlot(cmds []Cmder) map[int][]Cmder {
	cmdsMap := make(map[int][]Cmder)
	for _, cmd := range cmds {
		slot := c.cmdSlot(cmd)
		cmdsMap[slot] = append(cmdsMap[slot], cmd)
	}
	return cmdsMap
}

func (c *ClusterClient) _processTxPipelineNode(
	ctx context.Context, node *clusterNode, cmds []Cmder, failedCmds *cmdsMap,
) error {
	return node.Client.hooks.processTxPipeline(ctx, cmds, func(ctx context.Context, cmds []Cmder) error {
		return node.Client.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
			err := cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
				return writeCmds(wr, cmds)
			})
			if err != nil {
				return err
			}

			return cn.WithReader(ctx, c.opt.ReadTimeout, func(rd *proto.Reader) error {
				statusCmd := cmds[0].(*StatusCmd)
				// Trim multi and exec.
				cmds = cmds[1 : len(cmds)-1]

				err := c.txPipelineReadQueued(ctx, rd, statusCmd, cmds, failedCmds)
				if err != nil {
					moved, ask, addr := isMovedError(err)
					if moved || ask {
						return c.cmdsMoved(ctx, cmds, moved, ask, addr, failedCmds)
					}
					return err
				}

				return pipelineReadCmds(rd, cmds)
			})
		})
	})
}

func (c *ClusterClient) txPipelineReadQueued(
	ctx context.Context,
	rd *proto.Reader,
	statusCmd *StatusCmd,
	cmds []Cmder,
	failedCmds *cmdsMap,
) error {
	// Parse queued replies.
	if err := statusCmd.readReply(rd); err != nil {
		return err
	}

	for _, cmd := range cmds {
		err := statusCmd.readReply(rd)
		if err == nil || c.checkMovedErr(ctx, cmd, err, failedCmds) || isRedisError(err) {
			continue
		}
		return err
	}

	// Parse number of replies.
	line, err := rd.ReadLine()
	if err != nil {
		if err == Nil {
			err = TxFailedErr
		}
		return err
	}

	switch line[0] {
	case proto.ErrorReply:
		return proto.ParseErrorReply(line)
	case proto.ArrayReply:
		// ok
	default:
		return fmt.Errorf("redis: expected '*', but got line %q", line)
	}

	return nil
}

func (c *ClusterClient) cmdsMoved(
	ctx context.Context, cmds []Cmder,
	moved, ask bool,
	addr string,
	failedCmds *cmdsMap,
) error {
	node, err := c.nodes.GetOrCreate(addr)
	if err != nil {
		return err
	}

	if moved {
		c.state.LazyReload()
		for _, cmd := range cmds {
			failedCmds.Add(node, cmd)
		}
		return nil
	}

	if ask {
		for _, cmd := range cmds {
			failedCmds.Add(node, NewCmd(ctx, "asking"), cmd)
		}
		return nil
	}

	return nil
}

func (c *ClusterClient) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	if len(keys) == 0 {
		return fmt.Errorf("redis: Watch requires at least one key")
	}

	slot := hashtag.Slot(keys[0])
	for _, key := range keys[1:] {
		if hashtag.Slot(key) != slot {
			err := fmt.Errorf("redis: Watch requires all keys to be in the same slot")
			return err
		}
	}

	node, err := c.slotMasterNode(ctx, slot)
	if err != nil {
		return err
	}

	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}

		err = node.Client.Watch(ctx, fn, keys...)
		if err == nil {
			break
		}

		moved, ask, addr := isMovedError(err)
		if moved || ask {
			node, err = c.nodes.GetOrCreate(addr)
			if err != nil {
				return err
			}
			continue
		}

		if isReadOnly := isReadOnlyError(err); isReadOnly || err == pool.ErrClosed {
			if isReadOnly {
				c.state.LazyReload()
			}
			node, err = c.slotMasterNode(ctx, slot)
			if err != nil {
				return err
			}
			continue
		}

		if shouldRetry(err, true) {
			continue
		}

		return err
	}

	return err
}

func (c *ClusterClient) pubSub() *PubSub {
	var node *clusterNode
	pubsub := &PubSub{
		opt: c.opt.clientOptions(),

		newConn: func(ctx context.Context, channels []string) (*pool.Conn, error) {
			if node != nil {
				panic("node != nil")
			}

			var err error
			if len(channels) > 0 {
				slot := hashtag.Slot(channels[0])
				node, err = c.slotMasterNode(ctx, slot)
			} else {
				node, err = c.nodes.Random()
			}
			if err != nil {
				return nil, err
			}

			cn, err := node.Client.newConn(context.TODO())
			if err != nil {
				node = nil

				return nil, err
			}

			return cn, nil
		},
		closeConn: func(cn *pool.Conn) error {
			err := node.Client.connPool.CloseConn(cn)
			node = nil
			return err
		},
	}
	pubsub.init()

	return pubsub
}

// Subscribe subscribes the client to the specified channels.
// Channels can be omitted to create empty subscription.
func (c *ClusterClient) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)
	}
	return pubsub
}

// PSubscribe subscribes the client to the given patterns.
// Patterns can be omitted to create empty subscription.
func (c *ClusterClient) PSubscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.pubSub()
	if len(channels) > 0 {
		_ = pubsub.PSubscribe(ctx, channels...)
	}
	return pubsub
}

func (c *ClusterClient) retryBackoff(attempt int) time.Duration {
	return internal.RetryBackoff(attempt, c.opt.MinRetryBackoff, c.opt.MaxRetryBackoff)
}

func (c *ClusterClient) cmdsInfo(ctx context.Context) (map[string]*CommandInfo, error) {
	// Try 3 random nodes.
	const nodeLimit = 3

	addrs, err := c.nodes.Addrs()
	if err != nil {
		return nil, err
	}

	var firstErr error
	// Permutation, 返回一个随机序列。
	perm := rand.Perm(len(addrs))
	// 最多尝试三个节点。
	if len(perm) > nodeLimit {
		perm = perm[:nodeLimit]
	}

	for _, idx := range perm {
		addr := addrs[idx]

		node, err := c.nodes.GetOrCreate(addr)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// 在这里调用 client 的方法。
		// 后面就是 client 的方法调用了。
		info, err := node.Client.Command(ctx).Result()
		if err == nil {
			return info, nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	// 为什么 firstErr 等于 nil的时候，是这个报错。
	if firstErr == nil {
		panic("not reached")
	}
	return nil, firstErr
}

func (c *ClusterClient) cmdInfo(name string) *CommandInfo {
	cmdsInfo, err := c.cmdsInfoCache.Get(c.ctx) // 在这里会从服务器获取cmd的信息。
	if err != nil {
		return nil
	}

	info := cmdsInfo[name]
	if info == nil {
		internal.Logger.Printf(c.Context(), "info for cmd=%s not found", name)
	}
	return info
}

func (c *ClusterClient) cmdSlot(cmd Cmder) int {
	args := cmd.Args()
	if args[0] == "cluster" && args[1] == "getkeysinslot" {
		return args[2].(int)
	}

	cmdInfo := c.cmdInfo(cmd.Name())
	return cmdSlot(cmd, cmdFirstKeyPos(cmd, cmdInfo))
}

func cmdSlot(cmd Cmder, pos int) int {
	if pos == 0 {
		return hashtag.RandomSlot()
	}
	firstKey := cmd.stringArg(pos)
	return hashtag.Slot(firstKey)
}

func (c *ClusterClient) cmdNode(
	ctx context.Context,
	cmdInfo *CommandInfo,
	slot int,
) (*clusterNode, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}
	// 悬停鼠标看 ReadOnly参数的注释。
	if c.opt.ReadOnly && cmdInfo != nil && cmdInfo.ReadOnly {
		return c.slotReadOnlyNode(state, slot)
	}
	// 默认操作还是给到在集群中的主节点。
	return state.slotMasterNode(slot)
}

// 默认是slave节点读取，如果有其他的配置，就使用其他的节点来完成读取的操作。
func (c *clusterClient) slotReadOnlyNode(state *clusterState, slot int) (*clusterNode, error) {
	if c.opt.RouteByLatency {
		return state.slotClosestNode(slot)
	}
	if c.opt.RouteRandomly {
		return state.slotRandomNode(slot)
	}
	return state.slotSlaveNode(slot)
}

// 通过slot选择，master节点。
func (c *ClusterClient) slotMasterNode(ctx context.Context, slot int) (*clusterNode, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}
	return state.slotMasterNode(slot)
}

// SlaveForKey gets a client for a replica node to run any command on it.
// This is especially useful if we want to run a particular lua script which has
// only read only commands on the replica.
// This is because other redis commands generally have a flag that points that
// they are read only and automatically run on the replica nodes
// if ClusterOptions.ReadOnly flag is set to true.
// 根据key计算出来的slot，找到一个slave节点，完成只读操作（或者只包含只读命令的lua脚本）
// 这个方法和下面的MasterForKey方法，都是为了对 key 完成节点的选择。
func (c *ClusterClient) SlaveForKey(ctx context.Context, key string) (*Client, error) {
	state, err := c.state.Get(ctx)
	if err != nil {
		return nil, err
	}
	slot := hashtag.Slot(key)
	node, err := c.slotReadOnlyNode(state, slot)
	if err != nil {
		return nil, err
	}
	return node.Client, err
}

// MasterForKey return a client to the master node for a particular key.
func (c *ClusterClient) MasterForKey(ctx context.Context, key string) (*Client, error) {
	slot := hashtag.Slot(key)
	node, err := c.slotMasterNode(ctx, slot)
	if err != nil {
		return nil, err
	}
	return node.Client, err
}

func appendUniqueNode(nodes []*clusterNode, node *clusterNode) []*clusterNode {
	for _, n := range nodes {
		// 如果已经存在了，就不添加了。
		if n == node {
			return nodes
		}
	}
	return append(nodes, node)
}

// 不存在才添加。 ip:port 信息。
func appendIfNotExists(ss []string, es ...string) []string {
loop:
	for _, e := range es {
		for _, s := range ss {
			if s == e {
				continue loop
			}
		}
		ss = append(ss, e)
	}
	return ss
}

// ------------------------------------------------------------------------------
// 在 pipeline 中的操作。
type cmdsMap struct {
	mu sync.Mutex
	m  map[*clusterNode][]Cmder
}

func newCmdsMap() *cmdsMap {
	return &cmdsMap{
		m: make(map[*clusterNode][]Cmder),
	}
}

func (m *cmdsMap) Add(node *clusterNode, cmds ...Cmder) {
	m.mu.Lock()
	m.m[node] = append(m.m[node], cmds...)
	m.mu.Unlock()
}
