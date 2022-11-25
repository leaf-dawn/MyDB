package data_manage

import (
	"briefDb/backend/data_manage/page_cacher"
	"briefDb/backend/utils"
)

func UUID2Address(uid utils.UUID) (page_cacher.PageNum, Offset) {
	u := uint64(uid)
	offset := Offset(u & ((1 << 16) - 1))
	u >>= 32
	pageNum := page_cacher.PageNum(u & ((1 << 32) - 1))
	return pageNum, offset
}

func Address2UUID(pageNum page_cacher.PageNum, offset Offset) utils.UUID {
	u0 := uint64(pageNum)
	u1 := uint64(offset)
	return utils.UUID((u0 << 32) | u1)
}

type Offset uint16 //定义偏移量，该偏移量是页内空闲位置的偏移

const LEN_OFFSET = 4

func PutOffset(buf []byte, offset Offset) {
	utils.PutUint16(buf, uint16(offset))
}

func ParseOffset(raw []byte) Offset {
	return Offset(utils.ParseUint16(raw))
}

func OffsetToRaw(offset Offset) []byte {
	return utils.Uint16ToRaw(uint16(offset))
}
