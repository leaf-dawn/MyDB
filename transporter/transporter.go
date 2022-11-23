/*
	Transporter 负责了二进制数据的传送和接受.
*/
package transporter

import (
	"bufio"
	"encoding/hex"
	"net"
)

//一个接口
type Transporter interface {
	Receive() ([]byte, error)
	Send(data []byte) error
	Close() error
}

//一个实现类hex
//hexTransporter有自己的二进制数据传输协议, 协议内容为:
//首先将二进制数据按照高4位和低4位拆分, 目的是为了干掉特殊字符, 如换行符.
//接着, 再拆分后的二进制数据后, 补上一个换行符\n, 并发送.
//那么, 另一端的Transporter就可以以readLine的形式, 读取出这一段传送的数据.
//接受到数据后, 去掉最后的换行符, 再将二进制数据的按照之前拆分的逆方法, 进行组装.
//最后得到完整的二进制数据.
type hexTransporter struct {
	conn     net.Conn
	receiver *bufio.Reader
	sender   *bufio.Writer
}

func NewHexTransporter(conn net.Conn) *hexTransporter {
	receiver := bufio.NewReader(conn)
	sender := bufio.NewWriter(conn)
	return &hexTransporter{
		conn:     conn,
		receiver: receiver,
		sender:   sender,
	}
}

//把一个byte发生出去，包装了conn.Write，
func (t *hexTransporter) Send(data []byte) error {
	data = hexEncode(data)

	_, err := t.sender.Write(data)
	if err != nil {
		return err
	}
	return t.sender.Flush()
}

//接收二进制
func (t *hexTransporter) Receive() ([]byte, error) {
	// 读取二进制，知道读取到\n作为一个结束
	data, err := t.receiver.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	data = hexDecode(data)
	return data, nil
}

//关闭
func (t *hexTransporter) Close() error {
	return t.conn.Close()
}

func hexDecode(buf []byte) []byte {
	n := len(buf) / 2
	ret := make([]byte, n)
	_, err := hex.Decode(ret, buf[:n*2])
	if err != nil {
		panic(err)
	}
	return ret
}

//进行了处理，
func hexEncode(buf []byte) []byte {
	n := len(buf)
	ret := make([]byte, n*2+1)
	hex.Encode(ret, buf)
	ret[n*2] = '\n'
	return ret
}
