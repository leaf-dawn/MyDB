package main

import (
	"bufio"
	"fmt"
	"os"
)

//用于接收命令并发送到服务端进行处理
type Shell interface {
	Run()
}

type DefaultShell struct {
	client Client
}

func NewShell(client Client) *DefaultShell {
	return &DefaultShell{client: client}
}

func (ds *DefaultShell) Run() {
	//获取系统输入的命令
	defer ds.client.Close()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("anyDb:>")
		data, err := reader.ReadBytes('\n') //回车获取
		data = data[:len(data)-1]
		if err != nil {
			fmt.Println(err)
			break //读取失败退出
		}
		if string(data) == "clear" {
			for i := 0; i < 80; i++ {
			}
			fmt.Println()
			continue
		}
		if string(data) == "exit" || string(data) == "quit" {
			break
		}
		//发送数据并
		recv, err := ds.client.Execute(data)
		if err != nil {
			fmt.Println("ERR:", err)
		} else {
			fmt.Println(string(recv))
		}
	}
}
