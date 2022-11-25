package page_cacher

import "briefDb/backend/utils"

type PageNum uint32 //页号

const (
	LEN_PGNO = 4
)

func PageNum2UUID(pgno PageNum) utils.UUID {
	return utils.UUID(pgno)
}
func UUID2PageNum(uuid utils.UUID) PageNum {
	return PageNum(uuid)
}
func PutPageNum(buf []byte, pgno PageNum) {
	utils.PutUint32(buf, uint32(pgno))
}
func ParsePageNum(raw []byte) PageNum {
	return PageNum(utils.ParseUint32(raw))
}
