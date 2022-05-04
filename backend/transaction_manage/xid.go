package transaction_manage

import "briefDb/backend/utils"

//xid用来标识事务的
type XID utils.UUID

const (
	LEN_XID   = utils.LEN_UUID
	SUPER_XID = 0 //xid无效值，nil,xid从1开始
)

func PutXID(buf []byte, xid XID) {
	utils.PutUUID(buf, utils.UUID(xid))
}

func ParseXID(raw []byte) XID {
	return XID(utils.ParseUUID(raw))
}

func XIDToRaw(xid XID, len int) []byte {
	return utils.UUIDToRaw(utils.UUID(xid), len)
}
