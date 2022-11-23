package page

import "briefDb/backend/utils"

/**
 * 页的唯一标识
 */

type Pgno uint32

const (
	LEN_PGNO = 4
)

func PgnoToUUID(pgno Pgno) utils.UUID {
	return utils.UUID(pgno)
}

func UUIDToPgno(uuid utils.UUID) Pgno {
	return Pgno(uuid)
}

func PutPgno(buf []byte, pgno Pgno) {
	utils.PutUint32(buf, uint32(pgno))
}

func ParsePgno(raw []byte) Pgno {
	return Pgno(utils.ParseUint32(raw))
}
