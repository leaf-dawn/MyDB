//
//client是提供给用户的一个api，可以直接使用。
//也可以通过main启动，通过命令行进行调用。
//shell -> client -> roundTripper(内部需要使用到transporter包进行发送接收)
//

package client

import "fansDB/transporter"

type Client interface {
	Execute(stat []byte) ([]byte, error)
	Close()
}

type client struct {
	roundTripper RoundTripper
}

func NewClient(packager transporter.Packager) *client {
	return &client{
		roundTripper: NewRoundTripper(packager),
	}
}

func (c *client) Close() {
	c.roundTripper.Close()
}

func (c *client) Execute(stat []byte) ([]byte, error) {
	statPkg := transporter.NewPackage(stat, nil)
	pkg, err := c.roundTripper.RoundTrip(statPkg)
	if err != nil {
		return nil, err
	}
	if pkg.Err() != nil {
		return nil, pkg.Err()
	}
	return pkg.Data(), nil
}
