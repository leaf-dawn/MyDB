package transaction_manage

import "briefDb/backend/utils"

//xid用来标识事务的
// xxid的别名
type XID utils.UUID

const (
	LEN_XID   = utils.LEN_UUID
	SUPER_XID = 0 //xid无效值，nil,xid从1开始
)

//添加xid到byte数组
func PutXID(buf []byte, xid XID) {
	utils.PutUUID(buf, utils.UUID(xid))
}

//从raw中读取xid
func ParseXID(raw []byte) XID {
	return XID(utils.ParseUUID(raw))
}

//添加xid到一个byte数组并返回
func XIDToRaw(xid XID) []byte {
	return utils.UUIDToRaw(utils.UUID(xid))
}
