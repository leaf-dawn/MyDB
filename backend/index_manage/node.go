package index_manage

import (
	dm "briefDb/backend/data_manage"
	"briefDb/backend/utils"
)

const (
	_IS_LEAF_OFFSET   = 0                                //是否是叶子节点的数据偏移
	_NO_KEYS_OFFSET   = _IS_LEAF_OFFSET + 1              //节点数目的数据偏移
	_SIBLING_OFFSET   = _NO_KEYS_OFFSET + 2              // 右节点的uuid
	_NODE_HEADER_SIZE = _SIBLING_OFFSET + utils.LEN_UUID // 节点头部信息大小

	_BALANCE_NUMBER = 32 //一个节点存放32个key-value
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
