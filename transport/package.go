package transport

//在AnyDB中传递的对象
type Package interface {
	Data() []byte
	Error() error
}

//简单数据对象
type SimplePackage struct {
	data []byte
	err  error
}

func NewPackage(data []byte, err error) *SimplePackage {
	return &SimplePackage{
		data: data,
		err:  err,
	}
}

func (p *SimplePackage) Data() []byte {
	return p.data
}

func (p *SimplePackage) Error() error {
	return p.err
}
