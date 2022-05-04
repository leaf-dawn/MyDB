package transport

import (
	"errors"
)

//用于把package转换为二进制，并从二进制转换为package
type Packager interface {
	Encode(pkg Package) []byte
	Decode(data []byte) (Package, error)
}

type SimplePackager struct {
}

func NewSimplePackager() *SimplePackager {
	return &SimplePackager{}
}

//编码的规则是：第一个byte为如果是1，说明是报错，如果是0说明是数据，报文格式
func (p *SimplePackager) Encode(pkg Package) []byte {
	if pkg.Error() != nil {
		tmp := make([]byte, len([]byte(pkg.Error().Error()))+1)
		tmp[0] = byte(1)
		copy(tmp[1:], pkg.Error().Error())
		return tmp
	} else {
		tmp := make([]byte, len(pkg.Data())+1)
		tmp[0] = byte(0)
		copy(tmp[1:], pkg.Data())
		return tmp
	}
}

func (p *SimplePackager) Decode(data []byte) (Package, error) {
	if len(data) == 0 {
		return nil, errors.New("数据包有问题")
	}
	if data[0] == byte(1) {
		//数据为报错
		err := errors.New(string(data[1:]))
		return NewPackage(nil, err), nil
	} else if data[0] == byte(0) {
		//为数据
		return NewPackage(data[1:], nil), nil
	} else {
		//数据出错
		return nil, errors.New("数据包有问题")
	}
}
