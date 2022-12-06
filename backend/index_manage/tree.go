package index_manage

import (
	dm "briefDb/backend/data_manage"
	tm "briefDb/backend/transaction_manage"
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

// Create
// 创建一棵B+树, 并返回其bootUUID.
func Create(dm dm.DataManager) (utils.UUID, error) {
	// 创建以空根节点
	rawRoot := newNilRootRaw()
	// 添加到文件中
	rootUUID, err := dm.Insert(tm.SUPER_TRANSACTION_ID, rawRoot)
	if err != nil {
		return utils.NilUUID, err
	}
	// 根节点uuid保存到文件，并返回存储根节点uuid的dataitem的uuid
	bootUUID, err := dm.Insert(tm.SUPER_TRANSACTION_ID, utils.UUIDToRaw(rootUUID))
	if err != nil {
		return utils.NilUUID, err
	}

	return bootUUID, nil
}
