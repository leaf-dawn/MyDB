package index_manage

import (
	dm "briefDb/backend/data_manage"
	"briefDb/backend/utils"
)

type node struct {
	dataitem dm.DataItem

	raw      []byte     //用于存储树节点信息
	selfUUID utils.UUID //存储改树节点信息的uuid，也是节点的索引值
}
