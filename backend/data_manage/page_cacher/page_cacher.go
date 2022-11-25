// Package page_cacher
//   page_cacher 实现了对页的缓存.
//   实际上pageCacher已经将缓存的逻辑托管给了cacher.Cacher了.
//   所以在pageCacher中, 只需要实现对磁盘操作的部分逻辑.
package page_cacher

import (
	"briefDb/backend/utils"
	"briefDb/backend/utils/cacher"
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

var (
	ErrMemTooSmall = errors.New("Memory is too small.")
)

const (
	PAGE_SIZE = 1 << 13 //一个页大小
	_MEM_LIM  = 10      //页缓存的数目

	SUFFIX_DB = ".db"
)

type PageCacher interface {
	/*
		该函数返回一个PageNum, 而不是一个Page, 原因是:
		如果返回一个Page, 则实际上整个过程是需要两步, 1)创建新页, 2)从cache中取得新页.
		问题出在, 如果1)成功, 而2)因为cache full而失败, 则将不能返回Page, 导致新页不能马上
		被利用, 因此还不如不要2)过程, 直回PageNum.
		将2)过程交给用户去调用GetPage()接返
	*/
	NewPage(initData []byte) PageNum       // 新创建一页, 返回新页页号
	GetPage(pageNum PageNum) (Page, error) // 根据叶号取得一页
	Close()

	/*
		下面的方法只有在Recovery的时候才会被调用.
		下面的方法不需要支持并发.
	*/
	TruncateByPageNum(maxPageNum PageNum) // 将DB扩充为maxPageNum这么多页的空间大小
	NoPages() int                         // 返回DB中一共有多少页
	FlushPage(pg Page)                    // 强制刷新pg到磁盘
}

type pageCacher struct {
	file     *os.File //缓存的文件
	fileLock sync.Mutex

	noPages uint32 //文件中页的数目

	cacher cacher.Cacher
}

//创建一个文件，并对文件进行页缓存
//path:文件路径
// mem:缓存大小
func Create(path string, mem int64) *pageCacher {
	file, err := os.OpenFile(path+SUFFIX_DB, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}

	return newPageCacher(file, mem)
}

//打开一个文件，并对文件进行页缓存
func Open(path string, mem int64) *pageCacher {
	file, err := os.OpenFile(path+SUFFIX_DB, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	return newPageCacher(file, mem)
}

func newPageCacher(file *os.File, mem int64) *pageCacher {
	if mem/PAGE_SIZE < _MEM_LIM {
		panic(ErrMemTooSmall)
	}

	info, err := file.Stat()
	if err != nil {
		panic(err)
	}
	size := info.Size()

	//在options添加到cacher
	p := new(pageCacher)
	options := new(cacher.Options)
	options.Get = p.getForCacher
	options.MaxHandles = uint32(mem / PAGE_SIZE) //设置资源数
	options.Release = p.releaseForCacher
	c := cacher.NewCacher(options)
	p.cacher = c
	p.file = file
	p.noPages = uint32(size / PAGE_SIZE) //获取文件页的总数

	return p
}

func (p *pageCacher) Close() {
	p.cacher.Close()
}

func (p *pageCacher) NewPage(initData []byte) PageNum {
	// 将noPages增加1, 且预留出一个页号的位置.
	pageNum := PageNum(atomic.AddUint32(&p.noPages, 1))
	pg := NewPage(pageNum, initData, nil)
	p.flush(pg)
	return pageNum
}

func (p *pageCacher) GetPage(pageNum PageNum) (Page, error) {
	uid := PageNum2UUID(pageNum)
	underlying, err := p.cacher.Get(uid)
	if err != nil {
		return nil, err
	}
	return underlying.(*page), nil
}

// get 根据pageNum从DB文件中读取页的内容, 并包裹成一页返回.
// get必须能够支持并发.
func (p *pageCacher) getForCacher(uid utils.UUID) (interface{}, error) {
	pageNum := UUID2PageNum(uid)
	offset := pageOffset(pageNum)

	buf := make([]byte, PAGE_SIZE)
	p.fileLock.Lock()
	_, err := p.file.ReadAt(buf, offset)
	if err != nil {
		utils.Fatal(uid, " Read: ", pageNum, ", ", offset, " ", err) // 如果DB文件出了问题, 则应该立即停止
	}
	p.fileLock.Unlock()

	pg := NewPage(pageNum, buf, p)
	return pg, nil
}

// release 释放掉该页的内容, 也就是刷新该页, 然后从内存中释放掉.
func (p *pageCacher) releaseForCacher(underlying interface{}) {
	pg := underlying.(*page)
	if pg.dirty == true {
		p.flush(pg)
		pg.dirty = false
	}
}

func (p *pageCacher) release(pg *page) {
	p.cacher.Release(PageNum2UUID(pg.pageNum))
}

// flush 刷新某一页的内容到DB文件.
// 因为flush为被release调用, 所以flush也必须是支持并发的.
func (p *pageCacher) flush(pg *page) {
	pageNum := pg.pageNum
	// 计算在磁盘中的偏移量
	offset := pageOffset(pageNum)

	p.fileLock.Lock()
	defer p.fileLock.Unlock()
	// 写入文件并刷新
	_, err := p.file.WriteAt(pg.data, offset)
	if err != nil {
		panic(err) // 如果DB文件出现了问题, 那么直接结束.
	}
	err = p.file.Sync()
	if err != nil {
		panic(err)
	}
}

func (p *pageCacher) TruncateByPageNum(maxPageNum PageNum) {
	size := pageOffset(maxPageNum + 1)
	err := p.file.Truncate(size)
	if err != nil {
		panic(err)
	}
	p.noPages = uint32(maxPageNum)
}

func (p *pageCacher) NoPages() int {
	return int(p.noPages)
}

func (p *pageCacher) FlushPage(pgi Page) {
	pg := pgi.(*page)
	p.flush(pg)
}

func pageOffset(pageNum PageNum) int64 {
	// 页号从1开始, 所以需要-1
	return int64(pageNum-1) * PAGE_SIZE
}
