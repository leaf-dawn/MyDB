// Package table_manage
// @Author: fzw
// @Create: ${YEAR}-${MONTH}-${DAY} ${HOUR}:${MINUTE}
// @Description: 字段管理，管理具体字段
// 格式为 [Field Name] [Type Name] [Index UUID]
package table_manage

import (
	"briefDb/backend/utils"
	"errors"
)

var (
	ErrInvaliFieldType   = errors.New("Invalid field type.")
	ErrInvalidFieldValue = errors.New("Invalid field value.")
)

type field struct {
	SelfUUID utils.UUID

	FieldName string
	FieldType string
	index     utils.UUID
}
