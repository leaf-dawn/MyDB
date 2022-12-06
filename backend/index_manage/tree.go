package index_manage

import (
	dm "briefDb/backend/data_manage"
	"briefDb/backend/utils"
	"sync"
)

type BPlusTree interface {
	Insert(key, uuid utils.UUID) error
	Search(key utils.UUID) ([]utils.UUID, error)
	SearchRange(leftKey, rightKey utils.UUID) ([]utils.UUID, error)
}

//
// bPlusTree
// @Description: b+树的一个实现
//
type bPlusTree struct {
	bootUUID     utils.UUID  //该uuid位置存储了head的uuid
	bootDataItem dm.DataItem //根节点数据
	bootLock     sync.Mutex  //锁根节点的

	DataManager dm.DataManager //存储该树的文件的datamanager
}
