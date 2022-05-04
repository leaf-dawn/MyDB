package transaction_manage

import (
	"os"
	"sync"
)

/**
  文件头部记录事务id的数量
  某事务byte的位移为(xid - 1)*_XID_FIELD_SIZE + XID_FILE_HEADER_SIZE。
  其中xid － 1是因为事务xid从1开始标号。

  | 8字节长度存储事务数量 | {一个字节长度的事务信息}{}{}{}  |
*/

const (
	_XID_FILE_HEADER_SIZE = 8 //文件元信息的信息长度
	_XID_FIELD_SIZE       = 1 //事务长度

	_FIELD_TRAN_ACTIVE   = 0 //事务状态
	_FIELD_TRAN_COMMITED = 1
	_FIELD_TRAN_ABORTED  = 2

	XID_FILE_TYPE = ".xid"
)

type TransactionManager interface {
	Begin() XID
	Commit(xid XID)
	Abort(xid XID)
	IsActive(xid XID) bool
	IsCommitted(xid XID) bool
	IsAborted(xid XID) bool
	Close()
}

type transactionManager struct {
	file        *os.File
	xidCounter  XID        //数量
	counterLock sync.Mutex //互斥锁
}

/**
 * 新建一个存储事务的文件，并返回transactionManager
 * 就是新建一个transactionManager的意思
 */
func Create(path string) *transactionManager {
	//创建文件,并设置为读，写，且如果文件存在，则清空
	file, err := os.OpenFile(path+XID_FILE_TYPE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}
	xidCounterInit := make([]byte, LEN_XID)
	_, err = file.WriteAt(xidCounterInit, 0)
	if err != nil {
		panic(err)
	}

	return newTransactionManager(file)
}

/**
 * 用已有的文件来创建transactionManager
 */
func Open(path string) *transactionManager {
	file, err := os.OpenFile(path+XID_FILE_TYPE, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	return newTransactionManager(file)
}

func newTransactionManager(file *os.File) *transactionManager {
	tm := new(transactionManager)
	tm.file = file
	tm.checkXIDCounter() //检验文件合法性
	return tm
}

/**
检验xid文件合法性，原理：获取头文件信息判断数据大小
和实际文件进行比较
*/
func (tm *transactionManager) checkXIDCounter() {
	//判断文件详细信息
	state, err := tm.file.Stat()
	if err != nil {
		panic(err)
	}
	if state.Size() < _XID_FILE_HEADER_SIZE {
		panic(ErrBadXIDFile)
	}
	tmp := make([]byte, _XID_FILE_HEADER_SIZE)
	_, err = tm.file.ReadAt(tmp, 0)
	if err != nil {
		panic(err)
	}
	//获取数量，也是最后一个xid
	tm.xidCounter = ParseXID(tmp)
	//获取最后一个xid位置
	lastXIDPosition, _ := xidPosition(tm.xidCounter)
	//判断真实文件长度是否等于计算出来的文件长度
	if lastXIDPosition+_XID_FIELD_SIZE != state.Size() {
		panic(ErrBadXIDFile)
	}

}

//根据xid来获取位置
func xidPosition(xid XID) (int64, int) {
	position := _XID_FILE_HEADER_SIZE + (xid-1)*_XID_FILE_HEADER_SIZE
	return int64(position), _XID_FIELD_SIZE
}

//让xid数量递增，注意，是不安全的，更新文件头部时
func (t *transactionManager) increaseXIDCounter() {
	t.xidCounter++
	buf := XIDToRaw(t.xidCounter, _XID_FILE_HEADER_SIZE)
	_, err := t.file.WriteAt(buf, 0)
	if err != nil {
		panic(err)
	}
	//刷新文件
	err = t.file.Sync()
	if err != nil {
		panic(err)
	}
}

//更新xid的事务为state的状态
func (t *transactionManager) updateTransactionState(xid XID, state int) {
	position, length := xidPosition(xid) //获取位置
	tmp := make([]byte, length)
	tmp[0] = byte(state)
	_, err := t.file.WriteAt(tmp, position)
	if err != nil {
		panic(err)
	}
	//刷新
	err = t.file.Sync()
	if err != nil {
		panic(err)
	}
}

//开启事务
func (t *transactionManager) Begin() XID {
	t.counterLock.Lock()
	defer t.counterLock.Lock()
	xid := t.xidCounter
	//更新事务状态，这里相当于追加
	t.updateTransactionState(xid, _FIELD_TRAN_ACTIVE)
	//更新头文件
	t.increaseXIDCounter()
	return xid
}

//提交事务
func (t *transactionManager) Commit(xid XID) {
	t.updateTransactionState(xid, _FIELD_TRAN_COMMITED)
}

//回滚事务
func (t *transactionManager) Abort(xid XID) {
	t.updateTransactionState(xid, _FIELD_TRAN_ABORTED)
}

//判断xid这个事务是否处于state的状态
func (t *transactionManager) checkXID(xid XID, state int) bool {
	position, length := xidPosition(xid)
	tmp := make([]byte, length)
	_, err := t.file.ReadAt(tmp, position)
	if err != nil {
		panic(err)
	}
	return tmp[0] == byte(state)
}

func (t *transactionManager) IsActive(xid XID) bool {
	if xid == SUPER_XID {
		return false
	}
	return t.checkXID(xid, _FIELD_TRAN_ACTIVE)
}

//todo:为什么为super_xid时返回true
func (t *transactionManager) IsCommitted(xid XID) bool {
	if xid == SUPER_XID {
		return true
	}
	return t.checkXID(xid, _FIELD_TRAN_COMMITED)
}

func (t *transactionManager) IsAborted(xid XID) bool {
	if xid == SUPER_XID {
		return false
	}
	return t.checkXID(xid, _FIELD_TRAN_ABORTED)
}

func (t *transactionManager) Close() {
	err := t.file.Close()
	if err != nil {
		panic(err)
	}
}
