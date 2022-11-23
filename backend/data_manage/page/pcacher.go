package page

import (
	"briefDb/backend/utils"
	"briefDb/backend/utils/cacher"
	"errors"
	"os"
	"sync"
)

/**
  pcacher实现对页的缓存
  页面的缓存
*/

var (
	//设置的mem过小
	ErrMemTooSmall = errors.New("Memory is too small.")
)

const (
	PAGE_SIZE = 1 << 13
	_MEN_LIM  = 10 //最小的页数量

	SUFFIX_DB = ".db"
)

type Pcacher interface {

	/**
	  返回pgno而不是page，因为有cache full的情况
	  添加了页，产生了pgno但是却无法返回page，所以干脆直接返回pgno
	*/
	NewPage(initData []byte) Pgno    // 新建一个页返回页码
	GetPage(pano Pgno) (Page, error) //根据页号获取一个页
	Close()

	/**
	  只能再Recovery得时候被调用
	*/
	TruncateByPgno(maxPgno Pgno) // 将DB扩充为maxPgno这么多页的空间大小
	NoPages() int                // 返回DB中一共有多少页
	FlushPage(pg Page)           // 强制刷新pg到磁盘
}

type pcacher struct {
	file     *os.File
	fileLock sync.Mutex

	noPage uint32

	cacher cacher.Cacher
}

//men传递最大空间大小
func Create(path string, mem int64) *pcacher {
	file, err := os.OpenFile(path+SUFFIX_DB, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}

}

func Open(path string, mem int64) *pcacher {
	file, err := os.OpenFile(path+SUFFIX_DB, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	return
}

func newPcacher(file *os.File, mem int64) *pcacher {

	if mem/PAGE_SIZE < _MEN_LIM {
		panic(ErrMemTooSmall)
	}

	info, err := file.Stat()
	if err != nil {
		panic(err)
	}
	size := info.Size()

	p := new(pcacher)
	options := new(cacher.Options)

}

//从文件中获取page并返回
func (p *pcacher) getForCacher(uid utils.UUID) (interface{}, error) {
	pgno := UUIDToPgno(uid)
	offset := pageOffset(pgno)

	//读取
	buf := make([]byte, PAGE_SIZE)
	p.fileLock.Lock()
	_, err := p.file.ReadAt(buf, offset)
	if err != nil {

	}
}

//获取pgno在文件的起始位置
func pageOffset(pgno Pgno) int64 {
	return int64(pgno-1) * PAGE_SIZE
}
