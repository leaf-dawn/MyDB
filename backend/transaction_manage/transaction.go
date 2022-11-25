package transaction_manage

// transaction.go 实现了sm内部的transaction结构, 该结构内保存了sm中事务需要的必要的信息
// 一个运行时的事务
type transaction struct {
	TransactionID TransactionID
	Level         int                    // 隔级别
	snapshot      map[TransactionID]bool // 快照
	Err           error                  // 发生的错误， 该事务只能被回滚
	AutoAbortted  bool                   // 该事务是否被自动回滚
}

//
// NewTransaction
//  @Description: 创建事务运行时
//  @param transactionID 事务id
//  @param level 事务隔离级别
//  @param active 当前时间点，进行中的事务，拍快照
//  @return *transaction
//
func NewTransaction(transactionID TransactionID, level int, active map[TransactionID]*transaction) *transaction {
	t := &transaction{
		TransactionID: transactionID,
		Level:         level,
		snapshot:      nil,
	}
	// 如果不是当前读，则拍快照
	if level != 0 {
		t.snapshot = make(map[TransactionID]bool)
		for transactionID, _ := range active {
			t.snapshot[transactionID] = true
		}
	}
	return t
}

func (t *transaction) InSnapShot(transactionID TransactionID) bool {
	if transactionID == SUPER_TRANSACTION_ID { // 忽略SUPER_TransactionID
		return false
	}
	_, ok := t.snapshot[transactionID]
	return ok
}
