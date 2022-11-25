package locktable

import (
	"briefDb/backend/utils"
	"container/list"
	"sync"
)

// 	锁表维护了一个有向图. 每次添加边的时候, 就会进行死锁检测.
type LockTable interface {
	// Add 向锁表中加入一条transactionID到uid的变, 如果返回false, 则表示造成死锁
	Add(transactionID, uid utils.UUID) (bool, chan struct{})
	// Remove 移除transactionID占用的所有uid
	Remove(transactionID utils.UUID)
}

type lockTable struct {
	transactionID2UID map[utils.UUID]*list.List    // transactionID已经获得的资源uid
	uid2TransactionID map[utils.UUID]utils.UUID    // uid被哪个transactionID获得
	wait              map[utils.UUID]*list.List    // 表示有哪些transactionID在等待这个uid, uwait和transactionID2UID应该是对偶关系
	waitCh            map[utils.UUID]chan struct{} // 用于对等待队列进行恢复
	transactionWait   map[utils.UUID]utils.UUID    // 表示transactionID在等待哪个uid
	lock              sync.Mutex
}

func NewLockTable() *lockTable {
	return &lockTable{
		transactionID2UID: make(map[utils.UUID]*list.List),
		uid2TransactionID: make(map[utils.UUID]utils.UUID),
		wait:              make(map[utils.UUID]*list.List),
		waitCh:            make(map[utils.UUID]chan struct{}),
		transactionWait:   make(map[utils.UUID]utils.UUID),
	}
}
