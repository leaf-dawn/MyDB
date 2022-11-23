package transaction_manage

import "errors"

//这里用事务管理的异常
var (
	ErrBadXIDFile = errors.New("Bad xid file")
)
