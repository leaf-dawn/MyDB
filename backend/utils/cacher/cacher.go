package cacher

import (
	"briefDb/backend/utils"
	"errors"
	"sync"
	"time"
)

/**
缓存管理，线程安全的
* 这里每次获取缓存时都要加lock?
*/

var (
	ErrCacherFull = errors.New("Cacher is full")
)

var (
	//等待时间
	_TIME_WAIT = time.Millisecond
)

type Cacher interface {

	//获取缓存里的资源
	Get(uid utils.UUID) (interface{}, error)
	//释放id未uid的资源
	Release(uid utils.UUID)
	//全部关闭
	Close()
}

type Options struct {

	// 当uid不在缓存中时, 则调用该函数取得对应资源.
	//这里必须传入的是线程安全的
	Get func(uid utils.UUID) (interface{}, error)

	Release func(underlying interface{})

	//允许最大资源数
	MaxHandles uint32
}

type cacher struct {
	options *Options

	cache   map[utils.UUID]interface{}
	refs    map[utils.UUID]uint32 //元素引用次数
	getting map[utils.UUID]bool   //map正在获取，但未获取成功的资源
	count   uint32                //表示cache中资源数目
	lock    sync.Mutex            //lock保护上面所有变量
}

func NewCacher(options *Options) *cacher {
	return &cacher{
		options: options,
		cache:   make(map[utils.UUID]interface{}),
		getting: make(map[utils.UUID]bool),
		refs:    make(map[utils.UUID]uint32),
	}
}

func (c *cacher) Get(uid utils.UUID) (interface{}, error) {

	for {
		c.lock.Lock()
		//检验是否有其他线程正在获取资源
		if _, ok := c.getting[uid]; ok {
			//如果正在被使用，等待
			c.lock.Unlock()
			time.Sleep(_TIME_WAIT)
			//继续获取资源
			continue
		}
		//在缓存中获取
		if _, ok := c.cache[uid]; ok {
			h := c.cache[uid]
			c.refs[uid]++
			c.lock.Unlock()
			return h, nil
		}
		//cache中没有该资源,,那么在options.Get中获取
		if c.options.MaxHandles > 0 && c.count == c.options.MaxHandles {
			//资源数已满
			c.lock.Unlock()
			return nil, ErrCacherFull
		} else {
			c.count++             //为马上新建的handle预留位置
			c.getting[uid] = true //表标记该资源正在获取
			c.lock.Unlock()
			break
		}
	}

	//获取资源
	underlying, err := c.options.Get(uid)
	if err != nil {
		c.lock.Lock()
		//无法获取资源
		c.count--
		delete(c.getting, uid)
		c.lock.Unlock()
		return nil, err
	}

	//如果获取不到资源，那么在options.Get中获取，并添加到缓存里面去
	c.lock.Lock()
	delete(c.getting, uid)
	c.cache[uid] = underlying
	c.refs[uid] = 1
	c.lock.Unlock()
	return underlying, nil
}

func (c *cacher) Close() {
	//判断是否有资源正在获取，等待一定次数
	c.lock.Lock()
	if len(c.getting) != 0 {

	}
}

func (c *cacher) Release(uid utils.UUID) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.refs[uid]--
	//如果已经没有人依赖这个资源了,那么删除
	if c.refs[uid] == 0 {
		//删除该资源
		underlying := c.cache[uid]
		c.options.Release(underlying)
		delete(c.refs, uid)
		delete(c.cache, uid)
		c.count--
	}
}
