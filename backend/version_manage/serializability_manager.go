/*
	serializability_manager.go 保证了调度的可串行化, 同时实现了MVCC.

	当事务发生ErrCannotSR错误时, SM会对该事务进行自动回滚.
*/
package version_manage

import (
	tm "briefDb/backend/Transaction_manage"
	dm "briefDb/backend/data_manage"
	"briefDb/backend/utils"
	"briefDb/backend/utils/cacher"
	"briefDb/backend/version_manage/locktable"
	"errors"
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
