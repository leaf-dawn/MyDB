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
	wait              map[utils.UUID]*list.List    // 表示有哪些transactionID在等待这个uid, wait和transactionID2UID应该是对偶关系
	waitCh            map[utils.UUID]chan struct{} // 用于对等待队列进行恢复
	transactionWait   map[utils.UUID]utils.UUID    // transaction在等待哪个uid
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

func (lt *lockTable) Add(transactionID, uid utils.UUID) (bool, chan struct{}) {
	//加锁，map不是线程安全的，而且需要同时改变多个map
	lt.lock.Lock()
	defer lt.lock.Unlock()
	// 如果已经包含该uid，直接返回ture
	if isInList(lt.transactionID2UID, transactionID, uid) == true {
		// 创建并返回chan
		ch := make(chan struct{})
		go func() {
			ch <- struct{}{}
		}()
		return true, ch
	}

	// 如果uid还未被其他事务占用
	if _, ok := lt.uid2TransactionID[uid]; ok == false {
		//添加到uid2TransactionID和transactionID2UID
		lt.uid2TransactionID[uid] = transactionID
		putIntoList(lt.transactionID2UID, transactionID, uid)
		// 获取资源成功
		ch := make(chan struct{})
		go func() {
			ch <- struct{}{}
		}()
		return true, ch
	}

	// 已经被其他事务占用
	// 添加到wait和transactionWait
	lt.transactionWait[transactionID] = uid
	putIntoList(lt.wait, uid, transactionID)
	// 判断是否产生环路，及死锁
	if lt.hasDeadLock() == true {
		// 如果死锁，则添加失败
		delete(lt.transactionWait, transactionID)
		removeFromList(lt.wait, uid, transactionID)
		return false, nil
	}
	// 如果不会造成死锁, 则等待回应
	ch := make(chan struct{})
	lt.waitCh[transactionID] = ch
	return true, ch
}

// Remove 移除一个transactionID,是否其占有资源
func (lt *lockTable) Remove(transactionID utils.UUID) {
	lt.lock.Lock()
	defer lt.lock.Unlock()
	//获取所有uuid，逐个释放，并同时等待该uuid的事务
	l := lt.transactionID2UID[transactionID]
	if l != nil { // 释放它占用的uid
		for l.Len() > 0 {
			e := l.Front()
			v := l.Remove(e)
			uid := v.(utils.UUID)
			lt.selectNewXID(uid)
		}
	}

	delete(lt.transactionWait, transactionID)
	delete(lt.transactionID2UID, transactionID)
	delete(lt.waitCh, transactionID)
}

var (
	transactionIDStamp map[utils.UUID]int
	stamp              int
)

// hasDeadLock 检验是否有环路，及死锁
func (lt *lockTable) hasDeadLock() bool {
	transactionIDStamp = make(map[utils.UUID]int)
	stamp = 1
	for transactionID, _ := range lt.transactionID2UID {
		if transactionIDStamp[transactionID] > 0 { // 已经dfs过了
			continue
		}
		stamp++
		if lt.dfs(transactionID) == true {
			return true
		}
	}
	return false
}

func (lt *lockTable) dfs(transactionID utils.UUID) bool {
	stp, ok := transactionIDStamp[transactionID]
	if ok && stp == stamp {
		return true // 有环
	}
	if ok && stp < stamp {
		return false // 该节点之前已经被遍历过且无环
	}
	transactionIDStamp[transactionID] = stamp

	uid, ok := lt.transactionWait[transactionID]
	if ok == false {
		return false
	}
	transactionID, ok = lt.uid2TransactionID[uid]
	utils.Assert(ok)

	return lt.dfs(transactionID)
}

// selectNewXID 为uid从等待队列中, 选择下一个transactionID来占用它.
func (lt *lockTable) selectNewXID(uid utils.UUID) {
	// 先将原来的事务删除
	delete(lt.uid2TransactionID, uid)
	l := lt.wait[uid]
	if l == nil {
		return
	}
	utils.Assert(l.Len() > 0)

	for l.Len() > 0 {
		e := l.Front()
		v := l.Remove(e)
		transactionID := v.(utils.UUID)
		// 有可能该事务已经被撤销，继续下一个
		if _, ok := lt.waitCh[transactionID]; ok == false {
			continue
		} else {
			// 将该uid指向transactionID
			lt.uid2TransactionID[uid] = transactionID
			//todo:t添加transactionid-uid
			// 对transactionID进行回应
			ch := lt.waitCh[transactionID]
			// 删除该transactionID的等待通道
			delete(lt.waitCh, transactionID)
			// 删除transactionID对uid的等待关系
			delete(lt.transactionWait, transactionID)
			// 回应
			ch <- struct{}{}
			break
		}
	}

	if l.Len() == 0 {
		delete(lt.wait, uid)
	}
}

//
// removeFromList
//  @Description: 在listMap中删除uid0对应的list的一个元素
//  @param listMap listMap
//  @param uid0     获取list用的key
//  @param uid1     需要移除的uid
//
func removeFromList(listMap map[utils.UUID]*list.List, uid0, uid1 utils.UUID) {
	l := listMap[uid0]
	e := l.Front()
	for e != nil {
		uid := e.Value.(utils.UUID)
		if uid == uid1 {
			l.Remove(e)
			break
		}
	}
	if l.Len() == 0 {
		delete(listMap, uid0)
	}
}

//
// isInList
//  @Description: 遍历所有的list，判断在list是否含有该uuid
//  @param listMap
//  @param uid0 获取list的key
//  @param uid1 list需要检验的uuid
//  @return bool list是否含有该uuid
//
func isInList(listMap map[utils.UUID]*list.List, uid0, uid1 utils.UUID) bool {
	// 检验uid0是否存在，如果不存在说明，不含uid1，返回false
	if _, ok := listMap[uid0]; ok == false {
		return false
	}
	l := listMap[uid0]
	// 迭代器遍历list，检验是否含有uid1
	e := l.Front()
	for e != nil {
		uid := e.Value.(utils.UUID)
		if uid == uid1 {
			return true
		}
		e = e.Next()
	}
	return false
}

func putIntoList(listMap map[utils.UUID]*list.List, uid0, uid1 utils.UUID) {
	if _, ok := listMap[uid0]; ok == false {
		listMap[uid0] = new(list.List)
	}
	listMap[uid0].PushFront(uid1)
}
