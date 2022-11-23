package page

import "sync"

/**
 * page.go实现page的逻辑和接口
     Page更新协议:
		在对Page做任何的更新之前, 一定需要吸纳调用Dirty().

	Page释放协议:
		在对Page操作完之后, 一定要调用Release()释放掉该页.
*/
type Page interface {
	Lock()
	Unlock()

	Release()     //释放该页
	SetDirty()    //设置为脏页
	Pgno() Pgno   //获取页号
	Data() []byte //获取数据

}

type page struct {
	pgno  Pgno
	data  []byte
	dirty bool //标识是否是脏页
	lock  sync.Mutex

	pc *pcacher
}

func NewPage(pgno Pgno, data []byte, pc *pcacher) *page {
	return &page{
		pgno: pgno,
		data: data,
		pc:   pc,
	}
}

func (p *page) Unlock() {
	p.lock.Unlock()
}

func (p *page) Lock() {
	p.lock.Lock()
}

func (p *page) Release() {

}

func (p *page) SetDirty() {
	p.dirty = true
}

func (p *page) Pgno() Pgno {
	return p.pgno
}

func (p *page) Data() []byte {
	return p.data
}
