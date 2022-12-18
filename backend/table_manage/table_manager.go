package table_manage

import (
	"errors"
	dm "fansDB/backend/data_manage"
	statement "fansDB/backend/parser"
	tm "fansDB/backend/transaction_manage"
	"fansDB/backend/utils"
	sm "fansDB/backend/version_manage"
	"nyadb2/backend/utils/booter"
	"sync"
)

var (
	ErrDuplicatedTable = errors.New("Duplicated table.")
	ErrNoThatTable     = errors.New("No that table.")
)

type TableManager interface {
	Begin(begin *statement.Begin) (tm.TransactionID, []byte)
	Commit(xid tm.TransactionID) ([]byte, error)
	Abort(xid tm.TransactionID) []byte

	Show(xid tm.TransactionID) []byte
	Create(xid tm.TransactionID, create *statement.Create) ([]byte, error)

	Insert(xid tm.TransactionID, insert *statement.Insert) ([]byte, error)
	Read(xid tm.TransactionID, read *statement.Read) ([]byte, error)
	Update(xid tm.TransactionID, update *statement.Update) ([]byte, error)
	Delete(xid tm.TransactionID, delete *statement.Delete) ([]byte, error)
}

type tableManager struct {
	DataManager            dm.DataManager
	SerializabilityManager sm.SerializabilityManager

	booter booter.Booter

	tableCacher        map[string]*table             // 表缓存
	transactionIDTable map[tm.TransactionID][]*table // xid 创建了哪些表
	lock               sync.Mutex
}

func newTableManager(sm sm.SerializabilityManager, dm dm.DataManager, booter booter.Booter) *tableManager {
	tbm := &tableManager{
		DataManager:            dm,
		SerializabilityManager: sm,
		booter:                 booter,
		tableCacher:            make(map[string]*table),
		transactionIDTable:     make(map[tm.TransactionID][]*table),
	}

	tbm.loadTables()
	return tbm
}

func Create(path string, sm sm.SerializabilityManager, dm dm.DataManager) *tableManager {
	booter := booter.Create(path)
	booter.Update(utils.UUIDToRaw(utils.NilUUID))
	return newTableManager(sm, dm, booter)
}

func Open(path string, sm sm.SerializabilityManager, dm dm.DataManager) *tableManager {
	booter := booter.Open(path)
	return newTableManager(sm, dm, booter)
}

// loadTables 将所有的table读入内存.
func (tbm *tableManager) loadTables() {
	uuid := tbm.firstTableUUID()
	for uuid != utils.NilUUID {
		tb := LoadTable(tbm, uuid)
		uuid = tb.Next
		tbm.tableCacher[tb.Name] = tb
	}
}

func (tbm *tableManager) firstTableUUID() utils.UUID {
	raw := tbm.booter.Load()
	return utils.ParseUUID(raw)
}

func (tbm *tableManager) updateFirstTableUUID(uuid utils.UUID) {
	raw := utils.UUIDToRaw(uuid)
	tbm.booter.Update(raw)
}

func (tbm *tableManager) Read(xid tm.TransactionID, read *statement.Read) ([]byte, error) {
	tbm.lock.Lock()
	tb, ok := tbm.tableCacher[read.TableName]
	tbm.lock.Unlock()
	if ok == false {
		return nil, ErrNoThatTable
	}

	result, err := tb.Read(xid, read)
	if err != nil {
		return nil, err
	}
	return []byte(result), nil
}

func (tbm *tableManager) Update(xid tm.TransactionID, update *statement.Update) ([]byte, error) {
	tbm.lock.Lock()
	tb, ok := tbm.tableCacher[update.TableName]
	tbm.lock.Unlock()
	if ok == false {
		return nil, ErrNoThatTable
	}

	count, err := tb.Update(xid, update)
	if err != nil {
		return nil, err
	}
	return []byte("Update " + utils.Uint32ToStr(uint32(count))), nil
}

func (tbm *tableManager) Delete(xid tm.TransactionID, delete *statement.Delete) ([]byte, error) {
	tbm.lock.Lock()
	tb, ok := tbm.tableCacher[delete.TableName]
	tbm.lock.Unlock()
	if ok == false {
		return nil, ErrNoThatTable
	}

	count, err := tb.Delete(xid, delete)
	if err != nil {
		return nil, err
	}
	return []byte("Delete " + utils.Uint32ToStr(uint32(count))), nil
}

func (tbm *tableManager) Insert(xid tm.TransactionID, insert *statement.Insert) ([]byte, error) {
	tbm.lock.Lock()
	tb, ok := tbm.tableCacher[insert.TableName]
	tbm.lock.Unlock()
	if ok == false {
		return nil, ErrNoThatTable
	}

	err := tb.Insert(xid, insert)
	if err != nil {
		return nil, err
	}
	return []byte("Insert"), nil
}

func (tbm *tableManager) Create(xid tm.TransactionID, create *statement.Create) ([]byte, error) {
	tbm.lock.Lock()
	defer tbm.lock.Unlock()

	_, ok := tbm.tableCacher[create.TableName]
	if ok == true { // 已经存在
		return nil, ErrDuplicatedTable
	}

	// 直接创建新表
	tb, err := CreateTable(tbm, tbm.firstTableUUID(), xid, create)
	if err != nil {
		return nil, err
	} else { // 创建成功
		tbm.updateFirstTableUUID(tb.SelfUUID)
		tbm.tableCacher[create.TableName] = tb
		tbm.transactionIDTable[xid] = append(tbm.transactionIDTable[xid], tb)
		return []byte("create " + create.TableName), nil
	}
}

/*
	Show 返回所有的表名.
*/
func (tbm *tableManager) Show(xid tm.TransactionID) []byte {
	tbm.lock.Lock()
	defer tbm.lock.Unlock()
	var results []byte
	for _, t := range tbm.tableCacher { // 打印已经提交的表
		tPrint := t.Print()
		results = append(results, tPrint...)
		results = append(results, '\n')
	}

	for _, t := range tbm.transactionIDTable[xid] { // 打印它自己创建的表
		tPrint := t.Print()
		results = append(results, tPrint...)
		results = append(results, '\n')
	}

	return results
}

func (tbm *tableManager) Begin(begin *statement.Begin) (tm.TransactionID, []byte) {
	var level int
	if begin.IsRepeatableRead {
		level = 1
	}
	xid := tbm.SerializabilityManager.Begin(level)
	return xid, []byte("begin")
}

func (tbm *tableManager) Commit(xid tm.TransactionID) ([]byte, error) {
	err := tbm.SerializabilityManager.Commit(xid)
	if err != nil {
		return nil, err
	}
	return []byte("commit"), nil
}

func (tbm *tableManager) Abort(xid tm.TransactionID) []byte {
	tbm.SerializabilityManager.Abort(xid)
	return []byte("abort")
}
