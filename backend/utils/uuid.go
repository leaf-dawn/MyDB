package utils

type UUID uint64

/**
 * 通用唯一识别码
 * 长度为8个字节
 */

var (
	INF     UUID = (1 << 63) - 1 + (1 << 63)
	NILUUID UUID = 0
)

const (
	LEN_UUID = 8
)

func PutUUID(buf []byte, uuid UUID) {
	PutUint64(buf, uint64(uuid))
}

func ParseUUID(raw []byte) UUID {
	return UUID(ParseUint64(raw))
}

func UUIDToRaw(uid UUID, len int) []byte {
	return Uint64ToRaw(uint64(uid), len)
}
