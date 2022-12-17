/*
	serializability_manager.go 保证了调度的可串行化, 同时实现了MVCC.

	当事务发生ErrCannotSR错误时, SM会对该事务进行自动回滚.
*/
package version_manage

import (
	"errors"
	tm "fansDB/backend/Transaction_manage"
	dm "fansDB/backend/data_manage"
	"fansDB/backend/utils"
	"fansDB/backend/utils/cacher"
	"fansDB/backend/version_manage/locktable"
	"sync"
)

var (
	ErrNilEntry = errors.New("Nil Entry.")
	ErrCannotSR = errors.New("Could not serialize access due to concurrent update!")
)

type SerializabilityManager interface {
	// Read 在事务内中读取uuid内容，
	Read(TransactionID tm.TransactionID, uuid utils.UUID) ([]byte, bool, error)
	// Insert 在事务中添加
	Insert(TransactionID tm.TransactionID, data []byte) (utils.UUID, error)
	// Delete 在事务中删除uuid内容
	Delete(TransactionID tm.TransactionID, uuid utils.UUID) (bool, error)
	// Begin 启动一个事务
	Begin(level int) tm.TransactionID
	// Commit 提交一个事务
	Commit(TransactionID tm.TransactionID) error
	// 回滚一个事务
	Abort(TransactionID tm.TransactionID)
}

type serializabilityManager struct {
	TM                tm.TransactionManager
	DM                dm.DataManager
	entryCacher       cacher.Cacher                        // entry的缓存
	transactionCacher map[tm.TransactionID]*tm.Transaction // 运行时事务的缓存
	lock              sync.Mutex

	lockTable locktable.LockTable
}

func NewSerializabilityManager(tm0 tm.TransactionManager, dm dm.DataManager) *serializabilityManager {
	sm := &serializabilityManager{
		TM:                tm0,
		DM:                dm,
		transactionCacher: make(map[tm.TransactionID]*tm.Transaction),
		lockTable:         locktable.NewLockTable(),
	}
	//
	options := new(cacher.Options)
	options.MaxHandles = 0
	options.Get = sm.getForCacher
	options.Release = sm.releaseForCacher
	ec := cacher.NewCacher(options)
	sm.entryCacher = ec

	sm.transactionCacher[tm.SUPER_TRANSACTION_ID] = tm.NewTransaction(tm.SUPER_TRANSACTION_ID, 0, nil)

	return sm
}

// Begin 启动一个事务
func (sm *serializabilityManager) Begin(level int) tm.TransactionID {
	sm.lock.Lock()
	defer sm.lock.Unlock()
	// 启动一个事务，获取事务id
	transactionID := sm.TM.Begin()
	// 创建一个事务，并拍快照
	t := tm.NewTransaction(transactionID, level, sm.transactionCacher)
	// 添加当前事务到事务缓存上
	sm.transactionCacher[transactionID] = t
	return transactionID
}

// Insert 向事务id种添加一个data数据
func (sm *serializabilityManager) Insert(transactionID tm.TransactionID, data []byte) (utils.UUID, error) {
	// 读取事务id
	sm.lock.Lock()
	t := sm.transactionCacher[transactionID]
	sm.lock.Unlock()

	if t.Err != nil {
		return utils.NilUUID, t.Err
	}
	//创建entry
	raw := WrapEntryRaw(transactionID, data)
	//添加
	return sm.DM.Insert(transactionID, raw)
}

// Commit 提交一个事务
func (sm *serializabilityManager) Commit(transactionID tm.TransactionID) error {
	sm.lock.Lock()
	t := sm.transactionCacher[transactionID]
	sm.lock.Unlock()

	if t.Err != nil { // 只能被撤销
		return t.Err
	}

	sm.lock.Lock()
	delete(sm.transactionCacher, transactionID)
	sm.lock.Unlock()

	sm.lockTable.Remove(utils.UUID(transactionID))
	sm.TM.Commit(transactionID)
	return nil
}

// Read 在transaction事务中读取uuid
func (sm *serializabilityManager) Read(transactionID tm.TransactionID, uuid utils.UUID) ([]byte, bool, error) {
	// 加锁获取事务
	sm.lock.Lock()
	t := sm.transactionCacher[transactionID]
	sm.lock.Unlock()

	if t.Err != nil {
		return nil, false, t.Err
	}

	// 根据uuid读取当前uuid创建时的entry
	handle, err := sm.entryCacher.Get(uuid)
	if err == ErrNilEntry {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	e := handle.(*entry)
	defer e.Release()

	// 检验是否有效
	if IsVisible(sm.TM, t, e) {
		return e.Data(), true, nil
	} else {
		return nil, false, nil
	}
}

func (sm *serializabilityManager) Delete(transactionID tm.TransactionID, uuid utils.UUID) (bool, error) {
	sm.lock.Lock()
	t := sm.transactionCacher[transactionID]
	sm.lock.Unlock()

	if t.Err != nil {
		return false, t.Err
	}

	/*
		先读取并判空, 再判断死锁.
	*/
	handle, err := sm.entryCacher.Get(uuid)
	if err == ErrNilEntry {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	e := handle.(*entry)
	defer e.Release()

	if IsVisible(sm.TM, t, e) == false { // 如果本身对其不可见, 则直接返回
		return false, nil
	}

	ok, ch := sm.lockTable.Add(utils.UUID(transactionID), uuid)
	if ok == false {
		t.Err = ErrCannotSR
		sm.abort(transactionID, true) // 自动撤销
		t.AutoAbortted = true
		return false, t.Err
	}
	<-ch

	// 如果之前已经被它自身所删除, 则直接返回.
	if e.XMAX() == transactionID {
		return false, nil
	}

	// 获得锁后, 还得进行版本跳跃检查
	skip := IsVersionSkip(sm.TM, t, e)
	if skip == true {
		t.Err = ErrCannotSR
		sm.abort(transactionID, true) // 自动撤销
		t.AutoAbortted = true
		return false, t.Err
	}

	// 更新其XMAX
	e.SetXMAX(transactionID)
	return true, nil
}

func (sm *serializabilityManager) abort(transactionID tm.TransactionID, auto bool) {
	sm.lock.Lock()
	t := sm.transactionCacher[transactionID]
	if auto == false { // 如果自动撤销, 不完全注销该事务, 只是潜在的将其回滚; 如果是手动, 则彻底注销.
		delete(sm.transactionCacher, transactionID)
	}
	sm.lock.Unlock()

	if t.AutoAbortted == true { // 已经被自动撤销过了
		return
	}

	sm.lockTable.Remove(utils.UUID(transactionID))
	sm.TM.Abort(transactionID)
}

func (sm *serializabilityManager) Abort(transactionID tm.TransactionID) {
	sm.abort(transactionID, false) // 手动撤销
}

func (sm *serializabilityManager) ReleaseEntry(e *entry) {
	sm.entryCacher.Release(e.selfUUID)
}

// =======================================================================
// entry相关操作，在这上会再添加一层缓存
func (sm *serializabilityManager) getForCacher(uuid utils.UUID) (interface{}, error) {
	// 构建一个entry
	e, ok, err := LoadEntry(sm, uuid)
	if err != nil {
		return nil, err
	}
	if ok == false { // 该entry由active事务产生, 且在恢复时已经被清除
		return nil, ErrNilEntry
	}
	return e, nil
}

func (sm *serializabilityManager) releaseForCacher(underlying interface{}) {
	e := underlying.(*entry)
	e.Remove()
}
