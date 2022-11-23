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

func main() {
	conn, err := net.Dial(_NET, _ADDRESS)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	pro := transporter.NewProtocoler()
	trs := transporter.NewHexTransporter(conn)
	pkger := transporter.NewPackager(trs, pro)

	clt := client.NewClient(pkger)
	shell := client.NewShell(clt)
	shell.Run()
}
