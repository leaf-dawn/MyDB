package transaction_manage

import "fansDB/backend/utils"

//TransactionID事务id
// xxid的别名
type TransactionID utils.UUID

const (
	LEN_TRANSACTION_ID   = utils.LEN_UUID
	SUPER_TRANSACTION_ID = 0 //xid无效值，nil,xid从1开始
)

//添加transactionID到byte数组
func PutTransactionID(buf []byte, xid TransactionID) {
	utils.PutUUID(buf, utils.UUID(xid))
}

//从raw中读取xid
func ParseTransactionID(raw []byte) TransactionID {
	return TransactionID(utils.ParseUUID(raw))
}

//添加xid到一个byte数组并返回
func XIDToRaw(xid TransactionID) []byte {
	return utils.UUIDToRaw(utils.UUID(xid))
}
