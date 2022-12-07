package parser

import "errors"

var (
	ErrInvalidStat = errors.New("Invalid command.")
	ErrHasNoIndex  = errors.New("Table has no index.")
)

func Parse(statement []byte) (interface{}, error) {
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
	return nil, nil
}

func parseUpdate(tokener *tokener) (*Update, error) {
	return nil, nil
}

func parseDelete(tokener *tokener) (*Delete, error) {
	return nil, nil
}

func parseInsert(tokener *tokener) (*Insert, error) {
	return nil, nil
}

func parseRead(tokener *tokener) (*Read, error) {
	return nil, nil
}

func parseWhere(tokener *tokener) (*Where, error) {
	return nil, nil
}

func parseSingleExpr(tokener *tokener) (*SingleExp, error) {
	return nil, nil
}

func parseDrop(tokener *tokener) (*Drop, error) {
	return nil, nil
}

func parseCreate(tokener *tokener) (*Create, error) {
	return nil, nil
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
