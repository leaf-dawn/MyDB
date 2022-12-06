package index_manage

import (
	dm "briefDb/backend/data_manage"
	"briefDb/backend/utils"
)

type node struct {
	dataitem dm.DataItem

	raw      []byte
	selfUUID utils.UUID
}
