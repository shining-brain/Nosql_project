package pool

import (
	"errors"
	"sync"
)

var (
	ErrClosed = errors.New("pool closed")
	ErrMax    = errors.New("reach max connection limit")
)

type request chan interface{}

// Config 池的配置文件类
type Config struct {
	MaxIdle   uint //可闲置的连接的最大数量
	MaxActive uint //池中的最大活跃连接数
}

// Pool 存储各类对象以供重用，比如redis连接
type Pool struct {
	Config
	factory     func() (interface{}, error)
	finalizer   func(x interface{})
	idles       chan interface{} //当前闲置的连接，用管道来存储
	waitingReqs []request        //等待队列，在闲置队列中没有连接，并且池的连接数已达到最大时使用
	activeCount uint             //在建立新连接时+1, 销毁已有连接时-1
	mu          sync.Mutex
	closed      bool
}

// New 创建一个新的连接池
func New(factory func() (interface{}, error), finalizer func(x interface{}), cfg Config) *Pool {
	return &Pool{
		factory:     factory,
		finalizer:   finalizer,
		idles:       make(chan interface{}, cfg.MaxIdle),
		waitingReqs: make([]request, 0),
		Config:      cfg,
	}
}

// getOnNoIdle try to create a new connection or waiting for connection being returned
// invoker should have pool.mu
//创建或等待连接。
//如果当前线程池中活跃的连接数大于了最大活跃数，加入等待队列中并阻塞（直到从等待队列中取出为止）。
//否则就创建新的连接，并返回这个连接
func (pool *Pool) getOnNoIdle() (interface{}, error) {
	//如果当前线程池中活跃的连接数大于了最大活跃数，加入等待队列中。
	if pool.activeCount >= pool.MaxActive {
		// waiting for connection being returned
		req := make(chan interface{}, 1)
		pool.waitingReqs = append(pool.waitingReqs, req)
		pool.mu.Unlock()
		//会产生阻塞
		x, ok := <-req
		if !ok {
			return nil, ErrMax
		}
		return x, nil
	}

	// create a new connection
	pool.activeCount++ // hold a place for new connection
	pool.mu.Unlock()
	x, err := pool.factory()
	//没有成功创建新连接的时候，要记得加锁并减掉活跃连接数。
	if err != nil {
		// create failed return token
		pool.mu.Lock()
		pool.activeCount-- // release the holding place
		pool.mu.Unlock()
		return nil, err
	}
	return x, nil
}

// Get 从线程池中取出一个连接。
//如果线程池中还没有连接或者线程池中连接都被占用中，则调用新创建函数（不一定会立刻创建新的连接）。
//Get并不会对活跃连接数进行更改
func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil, ErrClosed
	}

	select {
	case item := <-pool.idles:
		pool.mu.Unlock()
		return item, nil
	default:
		// no pooled item, create one
		return pool.getOnNoIdle()
	}
}

// Put 将一个连接丢进连接池里。
//如果连接池已经没了，则直接销毁这个连接。
//如果等待队列里有等待的管道，则直接把连接塞进这个管道里。
//如果闲置管道已经满了，则直接销毁这个连接。
//否则，就放进连接池中待命，此时不会对活跃连接数进行更改。
func (pool *Pool) Put(x interface{}) {
	pool.mu.Lock()

	//若线程池已经被关闭，则就地将这个连接处理掉
	if pool.closed {
		pool.mu.Unlock()
		pool.finalizer(x)
		return
	}

	//如果等待队列中有正在等待的管道，则直接把Put进去的连接丢进这个管道里
	if len(pool.waitingReqs) > 0 {
		req := pool.waitingReqs[0]
		copy(pool.waitingReqs, pool.waitingReqs[1:])
		pool.waitingReqs = pool.waitingReqs[:len(pool.waitingReqs)-1]
		req <- x
		pool.mu.Unlock()
		return
	}

	select {
	case pool.idles <- x:
		pool.mu.Unlock()
		return
	default:
		// reach max idle, destroy redundant item
		pool.mu.Unlock()
		pool.activeCount--
		pool.finalizer(x)
	}
}

// Close 关闭线程池，现将closed参数设置为true，然后再依次对当前的闲置连接调用finalizer
func (pool *Pool) Close() {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true
	close(pool.idles)
	pool.mu.Unlock()

	for x := range pool.idles {
		//终结池中当前的连接
		pool.finalizer(x)
	}
}
