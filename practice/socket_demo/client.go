package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

//客户端
func main() {
	conn, err := net.Dial("tcp", "192.168.43.106:8080")
	if err != nil {
		fmt.Println("err:", err)
		return
	}
	defer conn.Close() // 关闭连接
	inputReader := bufio.NewReader(os.Stdin)
	for {
		input, _ := inputReader.ReadString('\n') //读取输入
		inputInfo := strings.Trim(input, "\r\n")
		if strings.ToUpper(inputInfo) == "Q" { //发送q的话就退出
			return
		}
		_, err = conn.Write([]byte(inputInfo)) //发送数据
		if err != nil {
			return
		}
		buf := make([]byte, 512)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("接收数据失败", err)
			return
		}
		fmt.Println(string(buf[:n]))
	}
}
