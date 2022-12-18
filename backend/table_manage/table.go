package table_manage

import (
	"errors"
	statement "fansDB/backend/parser"
	tm "fansDB/backend/transaction_manage"
	"fansDB/backend/utils"
)

var (
	ErrInvalidValues   = errors.New("Invalid values.")
	ErrInvalidLogOP    = errors.New("Invalid logic operation.")
	ErrNoThatField     = errors.New("No that field.")
	ErrFieldHasNoField = errors.New("Field has no index.")
)

// map[Field]Value
type entry map[string]interface{}

type table struct {
	TableManager *tableManager
	SelfUUID     utils.UUID

	Name   string
	status byte
	Next   utils.UUID
	fields []*field
}

/*
	LoadTable 从数据库中将uuid指定的table读入内存.
	该函数只会在TM启动时被调用.
	因为该函数被调用时, 为单线程, 所以不会有ErrCacheFull之类的错误, 因此一旦遇到错误, 那一定
	是不可恢复的错误, 应该直接panic.
*/
func LoadTable(tbm *tableManager, uuid utils.UUID) *table {
	raw, ok, err := tbm.SerializabilityManager.Read(tm.SUPER_TRANSACTION_ID, uuid)
	utils.Assert(ok)
	if err != nil {
		panic(err)
	}

	tb := &table{
		TableManager: tbm,
		SelfUUID:     uuid,
	}

	tb.parseSelf(raw)
	return tb
}

// parseSelf 通过raw解析出table自己的信息.
func (t *table) parseSelf(raw []byte) {
	var pos, shift int
	t.Name, shift = utils.ParseVarStr(raw[pos:])
	pos += shift
	t.Next = utils.ParseUUID(raw[pos:])
	pos += utils.LEN_UUID

	for pos < len(raw) {
		uuid := utils.ParseUUID(raw[pos:])
		pos += utils.LEN_UUID
		f := LoadField(t, uuid)
		t.fields = append(t.fields, f)
	}
}

// CreateTable 创建一张表, 并返回其指针.
func CreateTable(tbm *tableManager, next utils.UUID, xid tm.TransactionID, create *statement.Create) (*table, error) {
	tb := &table{
		TableManager: tbm,
		Name:         create.TableName,
		Next:         next,
	}

	for i := 0; i < len(create.FieldName); i++ {
		fname := create.FieldName[i]
		ftype := create.FieldType[i]
		indexed := false
		for j := 0; j < len(create.Index); j++ {
			if create.Index[j] == fname {
				indexed = true
				break
			}
		}
		field, err := CreateField(tb, xid, fname, ftype, indexed)
		if err != nil {
			return nil, err
		}
		tb.fields = append(tb.fields, field)
	}

	err := tb.persistSelf(xid)
	if err != nil {
		return nil, err
	}

	return tb, nil
}

// persist 将t自身持久化到磁盘上, 该函数只会在CreateTable的时候被调用
func (t *table) persistSelf(xid tm.TransactionID) error {
	raw := utils.VarStrToRaw(t.Name)
	raw = append(raw, utils.UUIDToRaw(t.Next)...)
	for _, f := range t.fields {
		raw = append(raw, utils.UUIDToRaw(f.SelfUUID)...)
	}

	self, err := t.TableManager.SerializabilityManager.Insert(xid, raw)
	if err != nil {
		return err
	}

	t.SelfUUID = self
	return nil
}

func (t *table) Print() string {
	str := "{"
	str += t.Name + ": "
	for i := 0; i < len(t.fields); i++ {
		str += t.fields[i].Print()
		if i == len(t.fields)-1 {
			str += "}"
		} else {
			str += ", "
		}
	}
	return str
}

func (t *table) Delete(xid tm.TransactionID, delete *statement.Delete) (int, error) {
	uuids, err := t.parseWhere(delete.Where)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, uuid := range uuids {
		ok, err := t.TableManager.SerializabilityManager.Delete(xid, uuid)
		if err != nil {
			return 0, err
		}
		if ok {
			count++
		}
	}

	return count, nil
}

func (t *table) Update(xid tm.TransactionID, update *statement.Update) (int, error) {
	uuids, err := t.parseWhere(update.Where)
	if err != nil {
		return 0, err
	}

	var fd *field
	for _, f := range t.fields {
		if f.FName == update.FieldName {
			fd = f
			break
		}
	}
	if fd == nil {
		return 0, ErrNoThatField
	}
	v, err := fd.StrToValue(update.Value)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, uuid := range uuids {
		raw, ok, err := t.TableManager.SerializabilityManager.Read(xid, uuid)
		if err != nil {
			return 0, err
		}
		if ok == false {
			continue
		}

		_, err = t.TableManager.SerializabilityManager.Delete(xid, uuid) // 删除原来的entry
		if err != nil {
			return 0, err
		}

		e := t.parseEntry(raw) // 读取并解析entry
		e[fd.FName] = v        // 更新entry
		raw = t.entryToRaw(e)  // 将新entry存储进DB
		uuid, err = t.TableManager.SerializabilityManager.Insert(xid, raw)
		if err != nil {
			return 0, err
		}

		count++

		for _, f := range t.fields { // 更新对应的索引
			if f.IsIndexed() {
				err := f.Insert(e[f.FName], uuid)
				if err != nil {
					return 0, err
				}
			}
		}
	}

	return count, nil
}

func (t *table) Read(xid tm.TransactionID, read *statement.Read) (string, error) {
	uuids, err := t.parseWhere(read.Where)
	if err != nil {
		return "", err
	}

	result := ""
	for _, uuid := range uuids {
		raw, ok, err := t.TableManager.SerializabilityManager.Read(xid, uuid)
		if err != nil {
			return "", err
		}
		if ok == false {
			continue
		}
		e := t.parseEntry(raw)
		result += t.entryPrint(e) + "\n"
	}

	return result, nil
}

// parseWhere 对where语句进行解析, 返回field, 该where对应区间内的uuid
func (t *table) parseWhere(where *statement.Where) ([]utils.UUID, error) {
	var l0, r0, l1, r1 utils.UUID
	single := false
	var err error
	var fd *field

	if where == nil {
		for _, f := range t.fields {
			if f.IsIndexed() {
				fd = f
				break
			}
		}
		l0, r0 = 0, utils.INF
		single = true
	} else if where != nil {
		for _, f := range t.fields {
			if f.FName == where.SingleExp1.Field {
				if f.IsIndexed() == false {
					return nil, ErrFieldHasNoField
				}
				fd = f
				break
			}
		}
		if fd == nil {
			return nil, ErrNoThatField
		}

		l0, r0, l1, r1, single, err = t.calWhere(fd, where)
		if err != nil {
			return nil, err
		}
	}

	uuids, err := fd.Search(l0, r0)
	if err != nil {
		return nil, err
	}
	if single == false {
		tmp, err := fd.Search(l1, r1)
		if err != nil {
			return nil, err
		}
		uuids = append(uuids, tmp...)
	}

	return uuids, nil
}

// calWhere 计算该where语句所表示的key的区间.
// 由于where或许有or, 所以区间可能为2个.
func (t *table) calWhere(fd *field, where *statement.Where) (l0, r0, l1, r1 utils.UUID, single bool, err error) {
	if where.LogicOp == "" { // single
		single = true
		l0, r0, err = fd.CalExp(where.SingleExp1)
	} else if where.LogicOp == "or" {
		single = false
		l0, r0, err = fd.CalExp(where.SingleExp1)
		if err != nil {
			return
		}
		l1, r1, err = fd.CalExp(where.SingleExp2)
	} else if where.LogicOp == "and" {
		single = true
		l0, r0, err = fd.CalExp(where.SingleExp1)
		if err != nil {
			return
		}
		l1, r1, err = fd.CalExp(where.SingleExp2)
		// 合并[l0, r0], [l1, r1]两个区间
		if l1 > l0 {
			l0 = l1
		}
		if r1 < r0 {
			r0 = r1
		}
		return
	} else {
		err = ErrInvalidLogOP
	}
	return
}

// Insert 对该表执行insert语句.
func (t *table) Insert(xid tm.TransactionID, insert *statement.Insert) error {
	e, err := t.strToEntry(insert.Values) // 将insert的values转换为entry
	if err != nil {
		return err
	}

	raw := t.entryToRaw(e) // 将该entry插入到DB
	uuid, err := t.TableManager.SerializabilityManager.Insert(xid, raw)
	if err != nil {
		return err
	}

	for _, f := range t.fields { // 更新对应的索引
		if f.IsIndexed() {
			err := f.Insert(e[f.FName], uuid)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *table) strToEntry(values []string) (entry, error) {
	if len(values) != len(t.fields) {
		return nil, ErrInvalidValues
	}

	e := entry{}
	for i, f := range t.fields {
		v, err := f.StrToValue(values[i])
		if err != nil {
			return nil, err
		}
		e[f.FName] = v
	}

	return e, nil
}

func (t *table) entryToRaw(e entry) []byte {
	var raw []byte
	for _, f := range t.fields {
		raw = append(raw, f.ValueToRaw(e[f.FName])...)
	}
	return raw
}

func (t *table) parseEntry(raw []byte) entry {
	var pos, shift int
	e := entry{}
	for _, f := range t.fields {
		e[f.FName], shift = f.ParseValue(raw[pos:])
		pos += shift
	}
	return e
}

func (t *table) entryPrint(e entry) string {
	str := "["
	for i, f := range t.fields {
		str += f.ValuePrint(e[f.FName])
		if i == len(t.fields)-1 {
			str += "]"
		} else {
			str += ", "
		}
	}
	return str
}
