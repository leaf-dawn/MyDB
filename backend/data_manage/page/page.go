package page

type Page interface {
	Lock()
	Unlock()

	Release()    //释放该页
	SetDirty()   //设置为脏页
	IsDirty()    //判断是否是脏页
	PageNumber() //获取页号
	Data()       //获取数据

}
