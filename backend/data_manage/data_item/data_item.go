package data_item

import "briefDb/backend/utils"

/**
  dataEngine为上层模块提供的数据抽象

	数据共享:
		利用d.Data()得到的数据, 是内存共享的.

  	数据项修改协议:
   		上层模块在对数据项进行任何修改之前, 都必须调用d.Before(), 如果想撤销修改, 则再调用
		d.UnBefore(). 修改完成后, 还必须调用d.After(xid).
		DM会保证对Dataitem的修改是原子性的.

	数据项释放协议:
		上层模块不用数据项时, 必须调用d.Release()来将其释放
*/

type DataItem interface {
	Data() []byte     //Data以共享新式返回该dataItem的数据内容
	UUID() utils.UUID //Handle 返回该dataItem的handle

	Before()
	UnBefore()
	After()
	Release()

	//下面为Dm为上一层提供的对DataItem的锁操作
	Lock()
	Unlock()
	RLock()
	RUnLock()
}

/**
  对DataItem的实际实现， 其结构如下：
   [Valid Flag]        [Data Size]          [Data]
   1 byte bool		   2 bytes uint16       *
*/
