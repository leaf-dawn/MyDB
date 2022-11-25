/*
	data_manager.go 实现了DM, 它实现了对磁盘文件的管理.
	它在磁盘文件的基础上抽象出了"数据项"的概念, 并保证了数据库的可恢复性.
*/
package data_manage

import (
	"briefDb/backend/data_manage/logger"
	"briefDb/backend/data_manage/page_cacher"
	"briefDb/backend/data_manage/page_free_manage"
	tm "briefDb/backend/transaction_manage"
	"briefDb/backend/utils"
	"briefDb/backend/utils/cacher"
	"errors"
)

var (
	ErrBusy         = errors.New("Database is busy.")
	ErrDataTooLarge = errors.New("Data is too large.")
)

type DataManager interface {
	Read(uid utils.UUID) (DataItem, bool, error)
	Insert(xid tm.TransactionID, data []byte) (utils.UUID, error)

	Close()
}

type dataManager struct {
	tm tm.TransactionManager  // tm主要用于恢复时使用
	pc page_cacher.PageCacher //页缓存
	lg logger.Logger          //日志记录

	pidx page_free_manage.PageFreeManager
	dic  cacher.Cacher // dataitem的cache

	page1 page_cacher.Page
}

func newDataManager(pc page_cacher.PageCacher, lg logger.Logger, tm tm.TransactionManager) *dataManager {
	pidx := page_free_manage.NewPageFreeManager()

	dm := &dataManager{
		tm:   tm,
		pc:   pc,
		lg:   lg,
		pidx: pidx,
	}

	options := new(cacher.Options)
	options.MaxHandles = 0 // 实际的内存限制实际上是在pcacher中, 所以这里应该设置为0, 表示无限
	options.Get = dm.getForCacher
	options.Release = dm.releaseForCacher
	dm.dic = cacher.NewCacher(options)

	return dm
}

func Open(path string, mem int64, tm tm.TransactionManager) *dataManager {
	pc := page_cacher.Open(path, mem)
	lg := logger.Open(path)

	dm := newDataManager(pc, lg, tm)
	if dm.loadAndCheckPage1() == false {
		Recover(dm.tm, dm.lg, dm.pc)
	}

	dm.fillPageFreeManager()

	P1SetVCOpen(dm.page1)
	dm.pc.FlushPage(dm.page1)

	return dm
}

func Create(path string, mem int64, tm tm.TransactionManager) *dataManager {
	pc := page_cacher.Create(path, mem)
	lg := logger.Create(path)

	dm := newDataManager(pc, lg, tm)
	dm.initPage1()

	return dm
}

// fillPageFreeManager 构建pindex
func (dm *dataManager) fillPageFreeManager() {
	noPages := dm.pc.NoPages()
	for i := 2; i <= noPages; i++ {
		pg, err := dm.pc.GetPage(page_cacher.PageNum(i))
		if err != nil {
			panic(err)
		}
		dm.pidx.Add(pg.PageNum(), PageXFreeSpace(pg))
		pg.Release()
	}
}

// loadAndCheckPage1 在OpenDB的时候读入page1, 并检验其正确性.
func (dm *dataManager) loadAndCheckPage1() bool {
	var err error
	dm.page1, err = dm.pc.GetPage(1)
	if err != nil {
		panic(err)
	}
	return P1CheckVC(dm.page1)
}

// initPage1 在CreateDB的时候用于初始化page1.
func (dm *dataManager) initPage1() {
	pgno := dm.pc.NewPage(P1InitRaw())
	utils.Assert(pgno == 1)
	var err error
	dm.page1, err = dm.pc.GetPage(pgno)
	if err != nil {
		panic(err)
	}

	dm.pc.FlushPage(dm.page1)
}

func (dm *dataManager) Close() {
	//	TODO: 如果还有事务正在进行, 直接Close或许会出错.
	dm.dic.Close()
	dm.lg.Close()

	// 关于page1的操作一定要在Close中被最后执行.
	P1SetVCClose(dm.page1)
	dm.page1.Release()
	dm.pc.Close()
}

func (dm *dataManager) Insert(xid tm.TransactionID, data []byte) (utils.UUID, error) {
	/*
		第一步: 将data包裹成dataitem raw.
				并检测raw长度是不是过长.
	*/
	raw := WrapDataitemRaw(data)
	if len(raw) > PageXMaxFreeSpace() {
		return 0, ErrDataTooLarge
	}

	/*
		第二步: 选出用来插入raw的pgno.
		因为有可能选择不成功, 则创建新页, 然后再次尝试选择.
		由于多线程, 有可能在该次创建新页后, 到下次它选择之前, 该新页已经被其他线程选走.
		所以需要多次尝试, 如果多次尝试仍然失败, 则返回一个ErrBusy错误.
	*/
	var pgno page_cacher.PageNum
	var freeSpace int
	var pg page_cacher.Page
	var err error
	for try := 0; try < 5; try++ {
		var ok bool
		pgno, freeSpace, ok = dm.pidx.Select(len(raw))
		if ok == true {
			break
		} else {
			// 创建新页, 并将新页加入到pindex, 以待下次选择.
			newPgno := dm.pc.NewPage(PageXInitRaw())
			dm.pidx.Add(newPgno, PageXMaxFreeSpace())
		}
	}
	if pgno == 0 { // 选择失败, 返回ErrBusy
		return 0, ErrBusy
	}
	defer func() { // 该函数用于将pgno重新插回pidx
		if pg != nil {
			dm.pidx.Add(pgno, PageXFreeSpace(pg))
		} else {
			dm.pidx.Add(pgno, freeSpace)
		}
	}()

	/*
		第三步: 获得该页的Page实例
	*/
	pg, err = dm.pc.GetPage(pgno)
	if err != nil {
		return 0, err
	}

	/*
		第四步: 做日志.
	*/
	log := InsertLog(xid, pg, raw)
	dm.lg.Log(log)

	/*
		第五步: 将内容插入到该页内, 并返回插入的位移.
	*/
	offset := PageXInsert(pg, raw)

	/*
		第六步: 释放掉该页, 并返回UUID
	*/
	pg.Release()
	return Address2UUID(pgno, offset), nil
}

func (dm *dataManager) Read(uid utils.UUID) (DataItem, bool, error) {
	h, err := dm.dic.Get(uid)
	if err != nil {
		return nil, false, err
	}
	di := h.(*dataItem)
	if di.IsValid() == false { // 如果dataitem为非法, 则进行拦截, 返回空值
		di.Release()
		return nil, false, nil
	}

	return di, true, nil
}

func (dm *dataManager) getForCacher(uid utils.UUID) (interface{}, error) {
	pgno, offset := UUID2Address(uid)
	pg, err := dm.pc.GetPage(pgno)
	if err != nil {
		return nil, err
	}
	return ParseDataItem(pg, offset, dm), nil
}

func (dm *dataManager) releaseForCacher(h interface{}) {
	di := h.(*dataItem)
	di.pg.Release()
}

// logDataitem 为di生成Update日志.
func (dm *dataManager) logDataitem(xid tm.TransactionID, di *dataItem) {
	log := UpdateLog(xid, di)
	dm.lg.Log(log)
}

func (dm *dataManager) ReleaseDataitem(di *dataItem) {
	dm.dic.Release(di.uid)
}
