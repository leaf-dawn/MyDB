package main

import (
	"briefDb/transport"
)

//模拟发送数据的，把[]byte发送到服务端，并接收数据返回
type Client interface {
	Execute(sendData []byte) ([]byte, error)
	Close() error
}

type client struct {
	transport transport.Transporter
}

func NewClient(transporter transport.Transporter) *client {
	return &client{
		transport: transporter,
	}
}

func (c *client) Execute(sendData []byte) ([]byte, error) {
	pkg := transport.NewPackage(sendData, nil)
	//调用transporter发送数据
	err := c.transport.Send(pkg)
	if err != nil {
		return nil, err
	}
	//接收返回数据
	recv, err := c.transport.Receive()
	if err != nil {
		return nil, err
	}
	if recv.Error() != nil {
		return nil, recv.Error()
	}
	return recv.Data(), nil
}

func (c *client) Close() error {
	return c.transport.Close()
}
