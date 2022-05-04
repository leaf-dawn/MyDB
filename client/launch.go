package main

import (
	"briefDb/transport"
	"fmt"
	"net"
)

const (
	_NET     = "tcp"
	_ADDRESS = ":8080"
)

func main() {
	//获取连接
	conn, err := net.Dial(_NET, _ADDRESS)
	if err != nil {
		fmt.Println(err)
	}
	//初始化客户端
	pkger := transport.NewSimplePackager()
	st := transport.NewTransporter(conn, pkger)
	client := NewClient(st)
	//初始化shell
	shell := NewShell(client)
	shell.Run()
}
