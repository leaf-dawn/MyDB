/*
   pageX.go 实现了对普通页的管理.

   普通页的结构如下:
   [Free Space Offset] uint16
   [Data] *

   [Free Space Offset] 表示空闲空间的位置指针.
*/
package data_manage

import "briefDb/backend/data_manage/pcacher"

const (
	_PX_OF_FREE = 0 //页内空闲空间偏移
	_PX_OF_DATA = 2 //页内数据偏移
)

// PXInitData 返回创建普通页时的初始内容
func PXInitRaw() []byte {
	raw := make([]byte, pcacher.PAGE_SIZE)
	// 初始时将FSO初始化为DATA的位移
	pxRawUpdateFSO(raw, _PX_OF_DATA)
	return raw
}

// PXMaxFreeSpace 返回普通页最大的FreeSpace
func PXMaxFreeSpace() int {
	return pcacher.PAGE_SIZE - _PX_OF_DATA
}

// pxRawFSO 通过raw, 取得free space offset的内容
func pxRawFSO(raw []byte) Offset {
	return ParseOffset(raw[_PX_OF_FREE:])
}

func PxFSO(pg pcacher.Page) Offset {
	return pxRawFSO(pg.Data())
}

// pxRawUpdateFSO 更新raw中FSO的内容
func pxRawUpdateFSO(raw []byte, offset Offset) {
	PutOffset(raw[_PX_OF_FREE:], offset)
}

// PXInsert 将raw插入到pg这一页内, 并返回插入的位移
func PXInsert(pg pcacher.Page, raw []byte) Offset {
	pg.Dirty()
	//在空闲位置以后写入数据
	offset := pxRawFSO(pg.Data())
	copy(pg.Data()[offset:], raw)
	//更新空闲指针
	pxRawUpdateFSO(pg.Data(), offset+Offset(len(raw)))
	return offset
}

// PXFreeSpace 返回pg的free space大小
func PXFreeSpace(pg pcacher.Page) int {
	return pcacher.PAGE_SIZE - int(pxRawFSO(pg.Data()))
}

// PXRecoverUpdate 辅助Recovery, 直接将raw的值复制到pg的offset位置.
func PXRecoverUpdate(pg pcacher.Page, offset Offset, raw []byte) {
	pg.Dirty()
	copy(pg.Data()[offset:], raw)
}

// PXRecoverInsert 辅助Recovery, 直接将raw复制到pg的offset位置.
// 然后将pg的FSO设置为较大的那一个.
// 可能会有一个BUG, 见recovery.go
func PXRecoverInsert(pg pcacher.Page, offset Offset, raw []byte) {
	pg.Dirty()
	copy(pg.Data()[offset:], raw)

	maxFSO := pxRawFSO(pg.Data())
	fso2 := offset + Offset(len(raw))
	if fso2 > maxFSO {
		maxFSO = fso2
	}
	pxRawUpdateFSO(pg.Data(), maxFSO)
}
