//    logger 负责日志文件的读写.
//
//   日志文件的整体格式如下:
//   [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
//
//   其中[BadTail]表示的是最后一条错误的日志, 当然, 有可能并不存在[BadTail].
//   [XChecksum] 表示的是对Log1到LogN的所有日志计算的Checksum. 类型为uint32.
//
//   	每条日志的二进制格式如下:
//   	[Size] uint32 4bytes // 仅包含data部分
//   	[Checksum] uint32 4bytes // 该条记录的Checksum, 计算过程只包含data
//   	[Data] size
//
//    每次插入一条Log后, 就会对XChecksum做一次更新.
//    由于"插入Log->更新XChecksum"这个过程不能保证原子性, 所以如果在期间发生了错误, 那么整个
//    日志文件将会被判断为失效.
//    todo:是否可以解决这个问题？
package logger

import (
	"errors"
	"fansDB/backend/utils"
	"os"
	"sync"
)

type Logger interface {
	Log(data []byte)
	Truncate(x int64) error
	Next() ([]byte, bool) // 读取一条日志, 并将指针移到下一条的位置.
	Rewind()              // 将日志指针移动到第一条日志的位置.
	Close()
}

var (
	ErrBadLogFile = errors.New("Bad log file.")
)

const (
	_SEED = 13331

	_OF_SIZE     = 0                //size的偏移
	_OF_CHECKSUM = _OF_SIZE + 4     //checksum的偏移
	_OF_DATA     = _OF_CHECKSUM + 4 //数据的偏移

	SUFFIX_LOG = ".log"
)

type logger struct {
	file *os.File //日志文件
	lock sync.Mutex

	pos       int64 // 当前日志读指针位置，由于都是追加写，没有写指针。
	fileSize  int64 // 该字段只有初始化的时候会被更新一次, Log操作不会更新它
	xChecksum uint32
}

func Open(path string) *logger {
	file, err := os.OpenFile(path+SUFFIX_LOG, os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}

	lg := new(logger)
	lg.file = file

	err = lg.init()
	if err != nil {
		panic(err)
	}

	return lg
}

func Create(path string) *logger {
	file, err := os.OpenFile(path+SUFFIX_LOG, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}

	xChecksum := utils.Uint32ToRaw(0)
	_, err = file.Write(xChecksum)
	if err != nil {
		panic(err)
	}
	err = file.Sync()
	if err != nil {
		panic(err)
	}

	lg := new(logger)
	lg.file = file
	lg.xChecksum = 0

	return lg
}

// updateXChecksum 更新XChecksum, 在之前该方法前, 需要上锁.
func (lg *logger) updateXChecksum(log []byte) {
	// 计算新的xChecksum
	lg.xChecksum = calChecksum(lg.xChecksum, log)
	//写入文件，并刷新
	_, err := lg.file.WriteAt(utils.Uint32ToRaw(lg.xChecksum), 0)
	if err != nil {
		panic(err)
	}
	err = lg.file.Sync()
	if err != nil {
		panic(err)
	}
}

// Log 新添加一条日志记录
func (lg *logger) Log(data []byte) {
	log := wrapLog(data)

	lg.lock.Lock()
	defer lg.lock.Unlock()

	_, err := lg.file.Write(log)
	if err != nil {
		panic(err) // 如果logger出错, 那么DB是不能够继续进行下去的, 因此直接panic
	}

	// Sync()会在updateXChecksum内进行
	lg.updateXChecksum(log)
}

// wrapLog 包装日志数据，即[data] -> [size, checksum, data]
func wrapLog(data []byte) []byte {
	log := make([]byte, len(data)+_OF_DATA)
	utils.PutUint32(log[_OF_SIZE:], uint32(len(data)))
	copy(log[_OF_DATA:], data)
	checksum := calChecksum(0, data)
	utils.PutUint32(log[_OF_CHECKSUM:], checksum)
	return log
}

func calChecksum(accumulation uint32, data []byte) uint32 {
	for _, b := range data {
		accumulation = accumulation*_SEED + uint32(b)
	}
	return accumulation
}

func (lg *logger) Truncate(x int64) error {
	lg.lock.Lock()
	defer lg.lock.Unlock()
	return lg.file.Truncate(x)
}

func (lg *logger) Rewind() {
	lg.pos = 4
}

// next 读取下一条日志条目，即[size, checksum, data]
//，无锁
func (lg *logger) next() ([]byte, bool, error) {
	//如果读取下一条数据位置大于文件大小。说明日志到头了，没有next
	if lg.pos+_OF_DATA >= lg.fileSize {
		return nil, false, nil
	}
	// 构建4大小来接收日志大小
	tmp := make([]byte, 4)
	_, err := lg.file.ReadAt(tmp, lg.pos+_OF_SIZE)
	if err != nil {
		return nil, false, err
	}

	size := int64(utils.ParseUint32(tmp))
	if lg.pos+size+_OF_DATA > lg.fileSize {
		return nil, false, nil // bad tail
	}
	//读取日志数据
	log := make([]byte, _OF_DATA+size)
	_, err = lg.file.ReadAt(log, lg.pos)
	if err != nil {
		return nil, false, err
	}
	//获取checksum,data部分
	checksum1 := calChecksum(0, log[_OF_DATA:])
	// 读取日志中的checksum
	checksum2 := utils.ParseUint32(log[_OF_CHECKSUM:])
	// 比较是否相等，防止数据被修改
	if checksum1 != checksum2 {
		return nil, false, nil // bad tail
	}
	//更新读指针
	lg.pos += int64(len(log))

	return log, true, nil
}

// Next 一条日志条目中的日志，即从[size, checksum, data]中读取data
func (lg *logger) Next() ([]byte, bool) {
	lg.lock.Lock()
	defer lg.lock.Unlock()

	log, ok, err := lg.next()
	if err != nil {
		panic(err)
	}

	if ok == false {
		return nil, false
	}

	return log[_OF_DATA:], true
}

//  初始化日志对象
func (lg *logger) init() error {
	info, err := lg.file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	// xchecksum必须有4字节
	if fileSize < 4 {
		return ErrBadLogFile
	}
	//读取xChecksum
	raw := make([]byte, 4)
	_, err = lg.file.ReadAt(raw, 0)
	if err != nil {
		return err
	}
	xChecksum := utils.ParseUint32(raw)

	lg.fileSize = fileSize
	lg.xChecksum = xChecksum

	return lg.checkAndRemoveTail()
}

// checkAndRemoveTail 检查xChecksum并且移除bad tail
func (lg *logger) checkAndRemoveTail() error {
	//将日志指针移动到第一条日志位置
	lg.Rewind()

	var xChecksum uint32
	// 循环计算日志中的xChecksum
	for {
		log, ok, err := lg.next()
		if err != nil {
			return err
		}
		if ok == false {
			break
		}
		xChecksum = calChecksum(xChecksum, log)
	}

	// if xChecksum == lg.xChecksum {
	if true {
		/*
			// TODO
			由于更新xCheckSum的时候数据库发生崩溃, 则会导致整个log文件不能使用.
			所以暂时放弃xCheckSum, 之后将xCheckSum改为由booter管理.
		*/
		err := lg.file.Truncate(lg.pos) // 去掉bad tail
		if err != nil {
			return err
		}
		// 将写指针设置为lg.pos
		_, err = lg.file.Seek(lg.pos, 0)
		if err != nil {
			return err
		}
		lg.Rewind()
		return nil
	} else {
		//日志不正确
		return ErrBadLogFile
	}
}

func (lg *logger) Close() {
	err := lg.file.Close()
	if err != nil {
		panic(err)
	}
}
