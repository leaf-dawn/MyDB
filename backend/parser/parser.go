package parser

import (
	"errors"
)

var (
	ErrInvalidStat = errors.New("Invalid command.")
	ErrHasNoIndex  = errors.New("Table has no index.")
)

//
// Parse
//  @Description:简单的语法分析器
//  @param statement
//  @return interface{}
//  @return error
//
func Parse(statement []byte) (interface{}, error) {
	// 读取第一个token查看需要进行什么解析
	tokener := newTokener(statement)
	token, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	tokener.Pop()

	var stat interface{}
	var staterr error

	switch token {
	case "begin":
		stat, staterr = parseBegin(tokener)
	case "commit":
		stat, staterr = parseCommit(tokener)
	case "abort":
		stat, staterr = parseAbort(tokener)
	case "create":
		stat, staterr = parseCreate(tokener)
	case "drop":
		stat, staterr = parseDrop(tokener)
	case "read":
		stat, staterr = parseRead(tokener)
	case "insert":
		stat, staterr = parseInsert(tokener)
	case "delete":
		stat, staterr = parseDelete(tokener)
	case "update":
		stat, staterr = parseUpdate(tokener)
	case "show":
		stat, staterr = parseShow(tokener)
	default:
		return nil, ErrInvalidStat
	}

	next, err := tokener.Peek()
	if err == nil && next != "" {
		errStat := tokener.ErrStat()
		staterr = errors.New("Invalid Stat: " + string(errStat))
	}

	return stat, staterr
}

func parseShow(tokener *tokener) (*Show, error) {
	tmp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" {
		return new(Show), nil
	} else {
		return nil, ErrInvalidStat
	}
}

// 简单的解析
// set tablename fieldname = value
func parseUpdate(tokener *tokener) (*Update, error) {
	var err error
	update := new(Update)
	update.TableName, err = tokener.Peek()
	if err != nil {
		return nil, err
	}
	tokener.Pop()

	set, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if set != "set" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()

	// field = value
	update.FieldName, err = tokener.Peek()
	if err != nil {
		return nil, err
	}
	tokener.Pop()
	tmp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp != "=" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()
	update.Value, err = tokener.Peek()
	if err != nil {
		return nil, err
	}
	tokener.Pop()

	// 如果没有where，直接返回
	tmp, err = tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" {
		update.Where = nil
		return update, nil
	}

	// 解析后面表达式
	where, err := parseWhere(tokener)
	if err != nil {
		return nil, err
	}
	update.Where = where
	return update, nil
}

func parseDelete(tokener *tokener) (*Delete, error) {
	deleteStat := new(Delete)
	// from
	from, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if from != "from" {
		return nil, ErrInvalidStat
	}

	// tableName
	tokener.Pop()
	tableName, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(tableName) == false {
		return nil, ErrInvalidStat
	}
	deleteStat.TableName = tableName

	// where
	tokener.Pop()
	where, err := parseWhere(tokener)
	if err != nil {
		return nil, err
	}
	deleteStat.Where = where
	return deleteStat, nil
}

// 解析简单insert语法
// insert into tableName values value1 value2...
func parseInsert(tokener *tokener) (*Insert, error) {
	insert := new(Insert)

	into, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if into != "into" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()

	// tableName
	tableName, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(tableName) == false {
		return nil, ErrInvalidStat
	}
	insert.TableName = tableName
	tokener.Pop()

	// 读取values
	values, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if values != "values" {
		return nil, ErrInvalidStat
	}

	for {
		tokener.Pop()
		value, err := tokener.Peek()
		if err != nil {
			return nil, err
		}
		if value == "" {
			break
		} else {
			insert.Values = append(insert.Values, value)
		}
	}

	return insert, nil
}

func parseRead(tokener *tokener) (*Read, error) {
	read := new(Read)

	asterisk, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if asterisk == "*" {
		// 读取全部字段
		read.Fields = append(read.Fields, "*")
		tokener.Pop()
	} else {
		// 读取多个字段
		for {
			field, err := tokener.Peek()
			if err != nil {
				return nil, err
			}
			if isName(field) == false {
				return nil, ErrInvalidStat
			}
			read.Fields = append(read.Fields, field)
			tokener.Pop()

			// 分隔符，如果没有分隔符则退出
			comma, err := tokener.Peek()
			if err != nil {
				return nil, err
			}
			if comma == "," {
				tokener.Pop()
			} else {
				break
			}
		}
	}

	// from tableName
	from, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if from != "from" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()

	tableName, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(tableName) == false {
		return nil, ErrInvalidStat
	}
	read.TableName = tableName
	tokener.Pop()

	// where
	tmp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if tmp == "" {
		read.Where = nil
		return read, nil
	}

	where, err := parseWhere(tokener)
	if err != nil {
		return nil, err
	}
	read.Where = where
	return read, nil
}

// 解析where后面的表达式
// 只支持 简单逻辑
func parseWhere(tokener *tokener) (*Where, error) {
	where := new(Where)
	// 读取where
	whereStr, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if whereStr != "where" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()
	// 读取后面的表达式1
	sexp1, err := parseSingleExpr(tokener)
	if err != nil {
		return nil, err
	}
	where.SingleExp1 = sexp1

	// 读取逻辑链接词
	logicOp, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if logicOp == "" {
		where.LogicOp = ""
		return where, nil
	}
	if isLogicOp(logicOp) == false {
		return nil, ErrInvalidStat
	}
	where.LogicOp = logicOp
	tokener.Pop()

	// 解析表达式2
	sexp2, err := parseSingleExpr(tokener)
	if err != nil {
		return nil, err
	}
	where.SingleExp2 = sexp2

	eof, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if eof != "" {
		return nil, ErrInvalidStat
	}

	return where, nil
}

//
// parseSingleExpr
// 解析一个简单表达式 a = b, a > b, a < b
//
func parseSingleExpr(tokener *tokener) (*SingleExp, error) {
	singleExp := new(SingleExp)

	field, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(field) == false {
		return nil, ErrInvalidStat
	}
	singleExp.Field = field
	tokener.Pop()

	op, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isCmpOp(op) == false {
		return nil, ErrInvalidStat
	}
	singleExp.CmpOp = op
	tokener.Pop()

	value, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	singleExp.Value = value
	tokener.Pop()

	return singleExp, nil
}

// 解析create语句
func parseCreate(tokener *tokener) (*Create, error) {

	// create table
	table, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if table != "table" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()

	create := new(Create)
	// table 名称
	name, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(name) == false {
		return nil, ErrInvalidStat
	}
	create.TableName = name

	// 解析创建的字段
	for {
		tokener.Pop()
		field, err := tokener.Peek()

		if field == "(" {
			break
		}
		if err != nil {
			return nil, err
		}

		// 解析一个字段名
		if isName(field) == false {
			return nil, ErrInvalidStat
		}
		tokener.Pop()

		// 类型
		ftype, err := tokener.Peek()
		if err != nil {
			return nil, err
		}
		if isType(ftype) == false {
			return nil, ErrInvalidStat
		}

		create.FieldName = append(create.FieldName, field)
		create.FieldType = append(create.FieldType, ftype)
		tokener.Pop()

		// 下一个如果是,继续解析，如果
		next, err := tokener.Peek()
		if err != nil {
			return nil, err
		}
		if next == "," {
			// 解析下一个
		} else if next == "" {
			return nil, ErrHasNoIndex
		} else if next == "(" {
			// 解析到左括号退出，说明解析到索引了
			break
		} else {
			return nil, ErrInvalidStat
		}
	}

	// 解析index
	// 弹出左括号
	tokener.Pop()

	// index
	index, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if index != "index" {
		return nil, ErrInvalidStat
	}
	for {
		// 循环遍历每一个index
		tokener.Pop()
		field, err := tokener.Peek()
		if err != nil {
			return nil, err
		}
		// 左括号退出
		if field == ")" {
			break
		} else if isName(field) == false {
			return nil, ErrInvalidStat
		} else {
			create.Index = append(create.Index, field)
		}
	}
	// 弹出右括号
	tokener.Pop()
	eof, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if eof != "" {
		return nil, ErrInvalidStat
	}
	return create, nil
}

// 删除table
func parseDrop(tokener *tokener) (*Drop, error) {
	// table
	table, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if table != "table" {
		return nil, ErrInvalidStat
	}
	tokener.Pop()

	// 读取table名称
	tableName, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if isName(tableName) == false {
		return nil, ErrInvalidStat
	}
	tokener.Pop()

	// 结束
	eof, err := tokener.Peek()
	if err != nil {
		return nil, err
	}
	if eof != "" {
		return nil, ErrInvalidStat
	}

	drop := new(Drop)
	drop.TableName = tableName
	return drop, nil
}

func parseBegin(tokener *tokener) (*Begin, error) {
	return nil, nil
}

func parseCommit(tokener *tokener) (*Commit, error) {
	return nil, nil
}

func parseAbort(tokener *tokener) (*Abort, error) {
	return nil, nil
}

// 是否是逻辑语句
func isLogicOp(op string) bool {
	return op == "and" || op == "or"
}

func isType(tp string) bool {
	return tp == "uint32" || tp == "uint64" ||
		tp == "string"
}

func isName(name string) bool {
	return !(len(name) == 1 && isAlphaBeta(name[0]) == false)
}

func isCmpOp(op string) bool {
	return op == "=" || op == ">" || op == "<"
}
