package transport

import (
	"bufio"
	"net"
)

//用于发送数据包的,对conn进行封装
type Transporter interface {
	Receive() (Package, error)
	Send(Package) error
	Close() error
}

type SimpleTransporter struct {
	packager Packager
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
}

func NewTransporter(conn net.Conn, packager Packager) *SimpleTransporter {
	return &SimpleTransporter{
		packager: packager,
		conn:     conn,
		reader:   bufio.NewReader(conn),
		writer:   bufio.NewWriter(conn),
	}
}

//发送数据
func (st *SimpleTransporter) Send(pkg Package) error {
	data := st.packager.Encode(pkg)
	_, err := st.writer.Write(data)
	if err != nil {
		return err
	}
	err = st.writer.Flush()
	return err
}

//接收数据
func (st *SimpleTransporter) Receive() (Package, error) {
	data := []byte{}
	n, err := st.reader.Read(data)
	if err != nil {
		return nil, err
	}
	pkg, err := st.packager.Decode(data[:n])
	return pkg, err
}

func (st *SimpleTransporter) Close() error {
	return st.conn.Close()
}
