package utils

import (
	"bytes"
	"encoding/binary"
)

func PutUint16(buf []byte, num uint16) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	binary.Write(buffer, binary.LittleEndian, num)
}

func ParseUint16(raw []byte) uint16 {
	var num uint16
	reader := bytes.NewReader(raw)
	binary.Read(reader, binary.LittleEndian, &num)

	return num
}

func Uint16ToRaw(num uint16) []byte {
	buf := make([]byte, 4)
	PutUint16(buf, num)
	return buf
}

func PutUint32(buf []byte, num uint32) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	binary.Write(buffer, binary.LittleEndian, num)
}

func ParseUint32(raw []byte) uint32 {
	var num uint32
	reader := bytes.NewReader(raw)
	binary.Read(reader, binary.LittleEndian, &num)

	return num
}

func Uint32ToRaw(num uint32) []byte {
	buf := make([]byte, 4)
	PutUint32(buf, num)
	return buf
}

func PutInt32(buf []byte, num int32) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	binary.Write(buffer, binary.LittleEndian, num)
}

func ParseInt32(raw []byte) int32 {
	var num int32
	reader := bytes.NewReader(raw)
	binary.Read(reader, binary.LittleEndian, &num)

	return num
}

func Int32ToRaw(num int32) []byte {
	buf := make([]byte, 4)
	PutInt32(buf, num)
	return buf
}

func PutInt64(buf []byte, num int64) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	binary.Write(buffer, binary.LittleEndian, num)
}

func ParseInt64(raw []byte) int64 {
	var num int64
	reader := bytes.NewReader(raw)
	binary.Read(reader, binary.LittleEndian, &num)

	return num
}

func Int64ToRaw(num int64) []byte {
	buf := make([]byte, 8)
	PutInt64(buf, num)
	return buf
}

// 添加unint64到byte数组，小顶端模式
func PutUint64(buf []byte, num uint64) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	binary.Write(buffer, binary.LittleEndian, num)
}

// 从raw中从头读取int64
func ParseUint64(raw []byte) uint64 {
	var num uint64
	reader := bytes.NewReader(raw)
	binary.Read(reader, binary.LittleEndian, &num)

	return num
}

// 添加int64到一个byte数组并返回
func Uint64ToRaw(num uint64) []byte {
	buf := make([]byte, 8)
	PutUint64(buf, num)
	return buf
}
