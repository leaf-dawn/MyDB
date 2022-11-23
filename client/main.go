package main

import (
	"briefDb/client/client"
	"briefDb/transporter"
	"fmt"
	"net"
	"os"
)

const (
	_NET     = "tcp"
	_ADDRESS = ":8080"
)

//客户端启动
func main() {
	conn, err := net.Dial(_NET, _ADDRESS)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	//装配传输对象进行传输数据的
	pro := transporter.NewProtocoler()
	trs := transporter.NewHexTransporter(conn)
	pkger := transporter.NewPackager(trs, pro)
	//创建客户端并启动shell
	clt := client.NewClient(pkger)
	shell := client.NewShell(clt)
	shell.Run()
}
