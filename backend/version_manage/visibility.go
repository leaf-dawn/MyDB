package version_manage

import tm "fansDB/backend/Transaction_manage"

// 可见性相关函数，用于判断一个事务是否对另一个事务可见的。

// IsVersionSkip 检测是否发生了版本跳跃
func IsVersionSkip(tm tm.TransactionManager, t *tm.Transaction, e *entry) bool {
	xmax := e.XMAX()
	if t.Level == 0 {
		// readCommitted 不判断版本跳跃, 直接返回false
		return false
	} else {
		return tm.IsCommitted(xmax) && (xmax > t.TransactionID || t.InSnapShot(xmax))
	}
}

// IsVisible 测试e是否对t可见.
func IsVisible(tm tm.TransactionManager, t *tm.Transaction, e *entry) bool {
	if t.Level == 0 {
		// 读提交
		return readCommitted(tm, t, e)
	} else {
		// 可重复读
		return repeatableRead(tm, t, e)
	}
	return false
}

// readCommitted 提交检验entry是否对事务t可见
func readCommitted(tm tm.TransactionManager, t *tm.Transaction, e *entry) bool {
	xid := t.TransactionID
	// 获取最大事务id和最小事务id
	xmin := e.XMIN()
	xmax := e.XMAX()

	if xmin == xid && xmax == 0 {
		return true
	}
	// 检验xmin是否已提交
	isCommitted := tm.IsCommitted(xmin)
	if isCommitted {
		if xmax == 0 {
			return true
		}
		// xmax还未提交
		if xmax != xid {
			isCommitted = tm.IsCommitted(xmax)
			if isCommitted == false {
				return true
			}
		}
	}
	return false
}

func repeatableRead(tm tm.TransactionManager, t *tm.Transaction, e *entry) bool {
	xid := t.TransactionID
	xmin := e.XMIN()
	xmax := e.XMAX()

	if xmin == xid && xmax == 0 {
		return true
	}

	isCommitted := tm.IsCommitted(xmin)
	if isCommitted && xmin < xid && t.InSnapShot(xmin) == false {
		if xmax == 0 {
			return true
		}
		if xmax != xid {
			isCommitted = tm.IsCommitted(xmax)
			if isCommitted == false || xmax > xid || t.InSnapShot(xmax) {
				return true
			}
		}
	}
	return false
}
