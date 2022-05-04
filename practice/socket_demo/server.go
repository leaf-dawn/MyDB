package main

import (
	"bufio"
	"fmt"
	"net"
)

func process(conn net.Conn) {
	defer conn.Close() // 最后关闭连接
	for {
		reader := bufio.NewReader(conn)
		buf := make([]byte, 128)
		n, err := reader.Read(buf) //读取数据
		if err != nil {
			fmt.Println("读取数据失败", err)
			break
		}
		recvStr := string(buf[:n])
		fmt.Println("收到数据为：", recvStr)
		_, _ = conn.Write([]byte(recvStr)) //发送数据
	}
}

func main() {
	listen, err := net.Listen("tcp", ":8080") //监听8080端口
	if err != nil {
		fmt.Println("监听失败", err)
		return
	}
	for {
		conn, err := listen.Accept() //接收获取连接
		if err != nil {
			fmt.Println("接收失败", err)
			continue
		}
		go process(conn) //启动一个线程处理连接
	}
}
