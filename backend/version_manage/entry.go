package version_manage

import (
	dm "briefDb/backend/data_manage"
	tm "briefDb/backend/transaction_manage"
	"briefDb/backend/utils"
)

/*
	Entry.go 维护了serializability_managerM一个记录的结构.
	虽然提供了多版本, 但是由于SM并没有提供Update操作, 所以对于每条entry, 有且只有一个版本.
	entry的二进制结构:
	[XMIN] [XMAX] [Data]
*/
const (
	_ENTRY_OF_XMIN = 0                                      //xmin偏移
	_ENTRY_OF_XMAX = _ENTRY_OF_XMIN + tm.LEN_TRANSACTION_ID //xmax偏移
	_ENTRY_DATA    = _ENTRY_OF_XMAX + tm.LEN_TRANSACTION_ID //entry_data偏移
)

type entry struct {
	selfUUID utils.UUID
	dataitem dm.DataItem //一个entry需要一个数据项去存储

}

func newEntry(di dm.DataItem, uuid utils.UUID) *entry {
	return &entry{
		selfUUID: uuid,
		dataitem: di,
	}
}

// 通过uuid读取一个entry
func LoadEntry(sm *serializabilityManager, uuid utils.UUID) (*entry, bool, error) {
	// 通过sm中的dataitem读取uuid中的数据快
	di, ok, err := sm.DM.Read(uuid)
	if err != nil {
		return nil, false, err
	}
	if ok == false {
		return nil, false, nil
	}
	return newEntry(di, uuid), true, nil
}

// WrapEntryRaw 将transactionID和data包裹成entry的二进制数据.
func WrapEntryRaw(transactionID tm.TransactionID, data []byte) []byte {
	// 数组
	raw := make([]byte, _ENTRY_DATA+len(data))
	//添加事务id到，xmin
	tm.PutTransactionID(raw[_ENTRY_OF_XMIN:], transactionID)
	//拷贝数据
	copy(raw[_ENTRY_DATA:], data)
	return raw
}

// 在entry中读取xmin
func (e *entry) XMIN() tm.TransactionID {
	e.dataitem.RLock()
	defer e.dataitem.RUnlock()
	return tm.ParseTransactionID(e.dataitem.Data()[_ENTRY_OF_XMIN:])
}

func (e *entry) XMAX() tm.TransactionID {
	e.dataitem.RLock()
	defer e.dataitem.RUnlock()

	return tm.ParseTransactionID(e.dataitem.Data()[_ENTRY_OF_XMAX:])
}

func (e *entry) SetXMAX(transactionID tm.TransactionID) {
	e.dataitem.Before()
	defer e.dataitem.After(transactionID)
	tm.PutTransactionID(e.dataitem.Data()[_ENTRY_OF_XMAX:], transactionID)
}
