package transaction_manage

import "briefDb/backend/utils"

//TransactionID事务id
// xxid的别名
type TransactionID utils.UUID

const (
	LEN_XID   = utils.LEN_UUID
	SUPER_XID = 0 //xid无效值，nil,xid从1开始
)

//添加xid到byte数组
func PutXID(buf []byte, xid TransactionID) {
	utils.PutUUID(buf, utils.UUID(xid))
}

//从raw中读取xid
func ParseXID(raw []byte) TransactionID {
	return TransactionID(utils.ParseUUID(raw))
}

//添加xid到一个byte数组并返回
func XIDToRaw(xid TransactionID) []byte {
	return utils.UUIDToRaw(utils.UUID(xid))
}
