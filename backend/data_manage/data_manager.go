package data_manage

import "errors"

//定义一些错误
var (
	ErrBusy        = errors.New("Database is busy")
	ErrDataTooLong = errors.New("Data is to long")
)

type DataManager interface {
	Read(uid)
}
