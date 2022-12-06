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
// todo: 是否可保证并发安全
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

// Load 通过bootUUIDd读取b+树
// todo:是否设置为存储根节点位置在起始位置
func Load(bootUUID utils.UUID, dm dm.DataManager) (BPlusTree, error) {
	bootItem, ok, err := dm.Read(bootUUID)
	if err != nil {
		return nil, err
	}
	utils.Assert(ok)

	tree := &bPlusTree{
		bootUUID:     bootUUID,
		DataManager:  dm,
		bootDataItem: bootItem,
	}
	return tree, nil
}

// Insert
// 向树种添加一个key-value
func (bt *bPlusTree) Insert(key, uuid utils.UUID) error {
	// 读取根节点
	rootUUID := bt.rootUUID()
	// 递归插入
	newNode, newKey, err := bt.insert(rootUUID, uuid, key)
	if err != nil {
		return err
	}

	// 如果需要更新根节点
	// todo: 如果多个线程同时都需要添加怎么办？
	if newNode != utils.NilUUID { // 更新根节点
		err := bt.updateRootUUID(rootUUID, newNode, newKey)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bt *bPlusTree) Search(key utils.UUID) ([]utils.UUID, error) {
	return nil, nil
}

func (bt *bPlusTree) SearchRange(leftKey, rightKey utils.UUID) ([]utils.UUID, error) {
	return nil, nil
}

// insert 将(uuid, key)插入到B+树中, 如果有分裂, 则将分裂产生的新节点也返回.
func (bt *bPlusTree) insert(nodeUUID, uuid, key utils.UUID) (newNodeUUID, newNodeKey utils.UUID, err error) {
	// 读取当前节点
	var node *node
	node, err = loadNode(bt, nodeUUID)
	if err != nil {
		return
	}

	isLeaf := node.IsLeaf()
	node.Release()

	if isLeaf {
		// 如果是叶子节点，无法继续向下寻找，直接插入
		newNodeUUID, newNodeKey, err = bt.insertAndSplit(nodeUUID, uuid, key)
	} else {
		// 获取要插入的下一个节点
		var next utils.UUID
		next, err = bt.searchNext(nodeUUID, key)
		if err != nil {
			return
		}
		// 插入下一个节点
		var newSonUUId utils.UUID
		var newSonKey utils.UUID
		newSonUUId, newSonKey, err = bt.insert(next, uuid, key)
		if err != nil {
			return
		}
		// 插入下一个节点以后，如果有分裂，继续插入向上分裂
		if newSonUUId != utils.NilUUID { // split
			newNodeUUID, newNodeKey, err = bt.insertAndSplit(nodeUUID, newSonUUId, newSonKey)
		}
	}
	return
}

// insertAndSplit
// 函数从node开始, 不断的向右试探兄弟节点, 直到找到一个节点, 能够插入进对应的值
func (bt *bPlusTree) insertAndSplit(nodeUUID, uuid, key utils.UUID) (utils.UUID, utils.UUID, error) {
	for {
		node, err := loadNode(bt, nodeUUID)
		if err != nil {
			return utils.NilUUID, utils.NilUUID, err
		}
		siblingSon, newNodeSon, newNodeKey, err := node.InsertAndSplit(uuid, key)
		node.Release()

		if siblingSon != utils.NilUUID { // 继续向sibling尝试
			nodeUUID = siblingSon
		} else {
			return newNodeSon, newNodeKey, err
		}
	}

}

// searchNext
// 从nodeUUID对应节点开始, 不断的向右试探兄弟节点, 找到对应key的next uuid
func (bt *bPlusTree) searchNext(nodeUUID, key utils.UUID) (utils.UUID, error) {
	for {
		node, err := loadNode(bt, nodeUUID)
		if err != nil {
			return utils.NilUUID, err
		}
		next, siblingUUID := node.SearchNext(key)
		node.Release()
		// 找得到
		if next != utils.NilUUID {
			return next, nil
		}
		// 找不到继续向兄弟节点找
		nodeUUID = siblingUUID
	}
}

// rootUUID
// 获取根节点地址
func (bt *bPlusTree) rootUUID() utils.UUID {
	bt.bootLock.Lock()
	defer bt.bootLock.Unlock()
	return utils.ParseUUID(bt.bootDataItem.Data())
}

// updateRootUUID
// 更新该树的根节点
func (bt *bPlusTree) updateRootUUID(left, right, rightKey utils.UUID) error {
	bt.bootLock.Lock()
	defer bt.bootLock.Unlock()

	// 创建新根节点
	rootRaw := newRootRaw(left, right, rightKey)
	// 插入并获取新根节点uuid
	newRootUUID, err := bt.DataManager.Insert(tm.SUPER_TRANSACTION_ID, rootRaw)
	if err != nil {
		return err
	}
	// 拷贝到boot
	bt.bootDataItem.Before()
	copy(bt.bootDataItem.Data(), utils.UUIDToRaw(newRootUUID))
	bt.bootDataItem.After(tm.SUPER_TRANSACTION_ID)
	return nil
}
