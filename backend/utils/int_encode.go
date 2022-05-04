package utils

import (
	"bytes"
	"encoding/binary"
)

/**
 * 该工具用于int类型相关转换的
 */

/**
 *把num添加到buf中，而且从index=0开始
 */
func PutUint16(buf []byte, num uint16) {
	buffer := bytes.NewBuffer(buf)
	//不开辟新空间，直接覆盖之前的
	buffer.Reset()
	//binary.LittleEndian小端模式
	_ = binary.Write(buffer, binary.LittleEndian, num)
}

/**
 * 读取raw
 */
func ParseUint16(raw []byte) uint16 {
	var num uint16
	reader := bytes.NewReader(raw)
	_ = binary.Read(reader, binary.LittleEndian, num)
	return num
}

/**
 * 读取num到len长度的byte中并返回
 */
func Uint16ToRaw(num uint16, len int) []byte {
	buf := make([]byte, len)
	PutUint16(buf, num)
	return buf
}

func PutUint32(buf []byte, num uint32) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	_ = binary.Write(buffer, binary.LittleEndian, num)
}

func ParseUint32(raw []byte) uint32 {
	var num uint32
	reader := bytes.NewReader(raw)
	_ = binary.Read(reader, binary.LittleEndian, num)
	return num
}

func Uint32ToRaw(num uint32, len int) []byte {
	buf := make([]byte, len)
	PutUint32(buf, num)
	return buf
}

func PutInt32(buf []byte, num int32) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	_ = binary.Write(buffer, binary.LittleEndian, num)
}

func ParseInt32(raw []byte) int32 {
	var num int32
	reader := bytes.NewReader(raw)
	_ = binary.Read(reader, binary.LittleEndian, num)
	return num
}

func Int32ToRaw(num int32, len int) []byte {
	buf := make([]byte, len)
	PutInt32(buf, num)
	return buf
}

func PutUint64(buf []byte, num uint64) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	_ = binary.Write(buffer, binary.LittleEndian, num)
}

func ParseUint64(raw []byte) uint64 {
	var num uint64
	reader := bytes.NewReader(raw)
	_ = binary.Read(reader, binary.LittleEndian, num)
	return num
}

func Uint64ToRaw(num uint64, len int) []byte {
	buf := make([]byte, len)
	PutUint64(buf, num)
	return buf
}

func PutInt64(buf []byte, num int64) {
	buffer := bytes.NewBuffer(buf)
	buffer.Reset()
	_ = binary.Write(buffer, binary.LittleEndian, num)
}

func ParseInt64(raw []byte) int64 {
	var num int64
	reader := bytes.NewReader(raw)
	_ = binary.Read(reader, binary.LittleEndian, num)
	return num
}

func Int64ToRaw(num int64, len int) []byte {
	buf := make([]byte, len)
	PutInt64(buf, num)
	return buf
}
