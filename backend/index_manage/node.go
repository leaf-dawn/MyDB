package index_manage

import (
	dm "fansDB/backend/data_manage"
	tm "fansDB/backend/transaction_manage"
	"fansDB/backend/utils"
)

const (
	_IS_LEAF_OFFSET   = 0                                //是否是叶子节点的数据偏移
	_NO_KEYS_OFFSET   = _IS_LEAF_OFFSET + 1              //节点数目的数据偏移
	_SIBLING_OFFSET   = _NO_KEYS_OFFSET + 2              // 右节点的uuid
	_NODE_HEADER_SIZE = _SIBLING_OFFSET + utils.LEN_UUID // 节点头部信息大小

	_BALANCE_NUMBER = 32 // 平衡数目，一个节点到达该数目两倍，则分裂
	_NODE_SIZE      = _NODE_HEADER_SIZE + (2*utils.LEN_UUID)*(_BALANCE_NUMBER*2+2)
)

/**
 * 结构如下
 * [leaf flag]        叶子节点标志1字节
 * [no of keys]       keys数目2字节
 * [sibling uuid]     右节点uuid，8字节
 * [son0],[key0],[son1],[key1]...[sonN],[keyN]
 */
type node struct {
	bPlusTree *bPlusTree
	dataItem  dm.DataItem //当前节点所代表的dataitem

	raw      []byte     //用于存储树节点信息
	selfUUID utils.UUID //存储改树节点信息的uuid，也是节点的索引值
}

func setRawIsLeaf(raw []byte, isLeaf bool) {
	if isLeaf {
		raw[_IS_LEAF_OFFSET] = byte(1)
	} else {
		raw[_IS_LEAF_OFFSET] = byte(0)
	}
}

func getRawIsLeaf(raw []byte) bool {
	return raw[_IS_LEAF_OFFSET] == byte(1)
}

func setRawNoKeys(raw []byte, noKeys int) {
	utils.PutUint16(raw[_NO_KEYS_OFFSET:], uint16(noKeys))
}

func getRawNoKeys(raw []byte) int {
	return int(utils.ParseUint16(raw[_NO_KEYS_OFFSET:]))
}

func setRawSibling(raw []byte, sibling utils.UUID) {
	utils.PutUUID(raw[_SIBLING_OFFSET:], sibling)
}

func getRawSibling(raw []byte) utils.UUID {
	return utils.ParseUUID(raw[_SIBLING_OFFSET:])
}

func setRawKthSon(raw []byte, uid utils.UUID, kth int) {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2)
	utils.PutUUID(raw[offset:], uid)
}

func getRawKthSon(raw []byte, kth int) utils.UUID {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2)
	return utils.ParseUUID(raw[offset:])
}

func setRawKthKey(raw []byte, key utils.UUID, kth int) {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2) + utils.LEN_UUID
	utils.PutUUID(raw[offset:], key)
}

func getRawKthKey(raw []byte, kth int) utils.UUID {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2) + utils.LEN_UUID
	return utils.ParseUUID(raw[offset:])
}

// 将kth位置的key和son向后移动一位
func shiftRawKth(raw []byte, kth int) {
	begin := _NODE_HEADER_SIZE + (kth+1)*(utils.LEN_UUID*2)
	end := _NODE_SIZE - 1
	for i := end; i >= begin; i-- { // copy(raw, raw) is dangerous
		raw[i] = raw[i-(utils.LEN_UUID*2)]
	}
}

//
// copyRawFromKth
//  @Description: 拷贝一个from中的第kth的key-value的据到to中
//  @param from
//  @param to
//  @param kth
//
func copyRawFromKth(from, to []byte, kth int) {
	offset := _NODE_HEADER_SIZE + kth*(utils.LEN_UUID*2)
	copy(to[_NODE_HEADER_SIZE:], from[offset:])
}

// newRootRaw 新建一个根节点, 该根节点的初始两个子节点为left和right, 初始键值为key
func newRootRaw(left, right, key utils.UUID) []byte {
	raw := make([]byte, _NODE_SIZE)
	setRawIsLeaf(raw, false)
	setRawNoKeys(raw, 2)
	setRawSibling(raw, utils.NilUUID)
	setRawKthSon(raw, left, 0)
	setRawKthKey(raw, key, 0)
	setRawKthSon(raw, right, 1)
	setRawKthKey(raw, utils.INF, 1)
	return raw
}

// newNilRootRaw 新建一个空的根节点, 返回其二进制内容.
func newNilRootRaw() []byte {
	raw := make([]byte, _NODE_SIZE)
	setRawIsLeaf(raw, true)
	setRawNoKeys(raw, 0)
	setRawSibling(raw, utils.NilUUID)
	return raw
}

// loadNode 读入一个节点, 其自身地址为selfuuid
func loadNode(bt *bPlusTree, selfUUID utils.UUID) (*node, error) {
	//读取selfUUID的数据项
	dataitem, ok, err := bt.DataManager.Read(selfUUID)
	if err != nil {
		return nil, err
	}
	utils.Assert(ok == true)

	return &node{
		bPlusTree: bt,
		dataItem:  dataitem,
		raw:       dataitem.Data(),
		selfUUID:  selfUUID,
	}, nil
}

// Release 释放一个节点空间
func (u *node) Release() {
	u.dataItem.Release()
}

// IsLeaf 判断一个节点是否是叶子节点
func (u *node) IsLeaf() bool {
	// 加锁
	u.dataItem.RLock()
	defer u.dataItem.RUnlock()
	return getRawIsLeaf(u.raw)
}

// SearchNext
// 寻找对应key的son, 如果找不到, 则返回sibling uuid
func (u *node) SearchNext(key utils.UUID) (utils.UUID, utils.UUID) {
	// 锁当前节点
	u.dataItem.RLock()
	defer u.dataItem.RUnlock()
	// 获取节点数目
	noKeys := getRawNoKeys(u.raw)
	for i := 0; i < noKeys; i++ {
		// 遍历并读取每一个节点
		ik := getRawKthKey(u.raw, i)
		// 如果key小于ik意味着，key在当前的son节点下，返回son
		if key < ik {
			return getRawKthSon(u.raw, i), utils.NilUUID
		}
	}
	//无法找到，返回兄弟节点的uuid
	return utils.NilUUID, getRawSibling(u.raw)
}

// LeafSearchRange
// 范围查询
// 在该节点上查询属于[leftKey, rightKey]的地址,
// 如果rightKey大于等于该节点的最大的key, 则还返回一个sibling uuid.
func (u *node) LeafSearchRange(leftKey, rightKey utils.UUID) ([]utils.UUID, utils.UUID) {
	//锁住当前item
	u.dataItem.RLock()
	defer u.dataItem.RUnlock()

	// 读取key数目
	noKeys := getRawNoKeys(u.raw)
	var kth int
	for kth < noKeys {
		// 遍历所有key
		ik := getRawKthKey(u.raw, kth)
		// 找到满足ik
		if ik >= leftKey {
			break
		}
		kth++
	}
	// 遍历并添加
	var uuids []utils.UUID
	for kth < noKeys {
		ik := getRawKthKey(u.raw, kth)
		if ik <= rightKey {
			uuids = append(uuids, getRawKthSon(u.raw, kth))
			kth++
		} else {
			break
		}
	}

	var sibling utils.UUID = utils.NilUUID
	if kth == noKeys {
		sibling = getRawSibling(u.raw)
	}

	return uuids, sibling
}

/*
		      p, k         p', k'
				 |         |
 			 	 v         v
	p0, k0, p1, k1         p2, k2, p3, INF
*/
// InsertAndSplit 将对应的数据插入该节点, 并尝试进行分裂.
// 如果该份数据不应该插入到此节点, 则返回一个sibling uuid.
func (u *node) InsertAndSplit(uuid, key utils.UUID) (utils.UUID, utils.UUID, utils.UUID, error) {
	var succ bool
	var err error

	u.dataItem.Before()
	defer func() {
		if err == nil && succ {
			u.dataItem.After(tm.SUPER_TRANSACTION_ID)
		} else {
			// 如果失败, 则复原当前节点
			u.dataItem.UnBefore()
		}
	}()

	succ = u.insert(uuid, key)
	if succ == false {
		return getRawSibling(u.raw), utils.NilUUID, utils.NilUUID, nil
	}

	if u.needSplit() {
		var newSon utils.UUID
		var newKey utils.UUID
		newSon, newKey, err = u.split()
		return utils.NilUUID, newSon, newKey, err
	} else {
		return utils.NilUUID, utils.NilUUID, utils.NilUUID, nil
	}
}

// insert 向一个节点中添加一个key，
// 如果是叶子节点，添加的就是key-value（uuid，存放value的dataItem的uuid）
// 如果不是叶子节点，则插入的是key位置，以及key位置右边的son。
// todo: 需要优化，设置成无论是分裂时插入到非叶子节点，还是添加到叶子节点，insert操作都一样
func (u *node) insert(uuid utils.UUID, key utils.UUID) bool {
	// 获取原本key数目
	noKeys := getRawNoKeys(u.raw)
	var kth int
	// 获取插入位置
	for kth < noKeys {
		ik := getRawKthKey(u.raw, kth)
		if ik < key {
			kth++
		} else {
			break
		}
	}

	if kth == noKeys && getRawSibling(u.raw) != utils.NilUUID {
		// 如果该节点有右继节点, 且该key大于该节点所有key
		// 则让该key被插入到右继节点去
		return false
	}

	// 如果是叶子节点,不需要设置
	if getRawIsLeaf(u.raw) == true {
		// kth位置向后移动一位
		shiftRawKth(u.raw, kth)
		// 设置key
		setRawKthKey(u.raw, key, kth)
		// 设置son为uuid
		setRawKthSon(u.raw, uuid, kth)
		// 更新节点数目
		setRawNoKeys(u.raw, noKeys+1)
	} else {
		// 非叶子节点，获kth位置的key
		kk := getRawKthKey(u.raw, kth)
		// 设置key
		setRawKthKey(u.raw, key, kth)
		// 向后kth+1向后移动
		shiftRawKth(u.raw, kth+1)
		// 将kk设置到kth+1位置
		setRawKthKey(u.raw, kk, kth+1)
		setRawKthSon(u.raw, uuid, kth+1)
		setRawNoKeys(u.raw, noKeys+1)
	}
	return true
}

func (u *node) needSplit() bool {
	return _BALANCE_NUMBER*2 == getRawNoKeys(u.raw)
}

func (u *node) split() (utils.UUID, utils.UUID, error) {
	// 创建并拷贝到新节点
	nodeRaw := make([]byte, _NODE_SIZE)
	setRawIsLeaf(nodeRaw, getRawIsLeaf(u.raw))
	setRawNoKeys(nodeRaw, _BALANCE_NUMBER)
	setRawSibling(nodeRaw, getRawSibling(u.raw))
	copyRawFromKth(u.raw, nodeRaw, _BALANCE_NUMBER)
	// 添加到文件中，并获取uuid
	son, err := u.bPlusTree.DataManager.Insert(tm.SUPER_TRANSACTION_ID, nodeRaw)

	if err != nil {
		return utils.NilUUID, utils.NilUUID, err
	}
	// 更新原来节点，更新其兄弟节点为分裂的新节点，更新原节点的key数目
	setRawNoKeys(u.raw, _BALANCE_NUMBER)
	setRawSibling(u.raw, son)

	return son, getRawKthKey(nodeRaw, 0), nil
}
