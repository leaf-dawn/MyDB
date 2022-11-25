//   page_free_manage 实现了对(PageNum, FreeSpace)键值对的缓存.
//   其中FreeSpace表示的是PageNum这一页还剩多少空间可用.
//   pageFreeManager存在目的在于, 当DM执行Insert操作时, 可用根据数据大小, 快速的选出有适合空间的页.
//
//   目前pageFreeManager的算法非常简单.
//   设置 threshold := page_cacher.PAGE_SIZE / _NO_INTERVALS,
//   然后划分出_NO_INTERVALS端区间, 分别表示FreeSpace大小为:
//   [-1, threshold), [threshold, 2*threshold), ...
//   每个区间内的页用链表组织起来.
package page_free_manage

import (
	"briefDb/backend/data_manage/page_cacher"
	"container/list"
	"sync"
)

const (
	_NO_INTERVALS = 40                                    //map大小，把页空间分为40份。
	_THRESHOLD    = page_cacher.PAGE_SIZE / _NO_INTERVALS //一份空间大小
)

type PageFreeManager interface {
	// 	Add将该键值对加入到Pindex中.
	Add(pgno page_cacher.PageNum, freeSpace int)
	// 	Select为spaceSize选择适当的PageNum, 并暂时将PageNum从Pindex中移除.
	Select(spaceSize int) (page_cacher.PageNum, int, bool)
}

type pageFreeManager struct {
	lock  sync.Mutex
	lists [_NO_INTERVALS + 1]list.List
}

type pair struct {
	pgno      page_cacher.PageNum
	freeSpace int
}

func NewPageFreeManager() *pageFreeManager {
	return &pageFreeManager{
		lists: [_NO_INTERVALS + 1]list.List{},
	}
}

func (pi *pageFreeManager) Add(pgno page_cacher.PageNum, freeSpace int) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	no := freeSpace / _THRESHOLD
	pi.lists[no].PushBack(&pair{pgno, freeSpace})
}

func (pi *pageFreeManager) Select(spaceSize int) (page_cacher.PageNum, int, bool) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	//获取需要空间数目
	no := spaceSize / _THRESHOLD
	if no < _NO_INTERVALS {
		no++
	}
	for no <= _NO_INTERVALS {
		//如果刚好剩余空间数的页不足，找更大剩余空间的页
		if pi.lists[no].Len() == 0 {
			no++
			continue
		}
		//获取页
		e := pi.lists[no].Front()
		v := pi.lists[no].Remove(e)
		pr := v.(*pair)
		return pr.pgno, pr.freeSpace, true
	}
	return 0, 0, false
}
