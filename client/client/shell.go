package client

import (
	"bufio"
	"fmt"
	"os"
)

//
//shell命令行，用户可以通过shell来使用命令行发送命令
//
type Shell interface {
	Run()
}

type shell struct {
	client Client
}

func NewShell(client Client) *shell {
	return &shell{
		client: client,
	}
}

func (s *shell) Run() {
	defer s.client.Close()
	// 含有缓冲区的io读取的包。提高效率
	termReader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(":> ")
		//读取到换行\n
		line, err := termReader.ReadBytes('\n')
		stat := line[:len(line)-1]
		if err != nil {
			fmt.Println(err)
			break
		}
		if string(stat) == "clear" {
			for i := 0; i < 80; i++ {
				fmt.Println()
			}
			continue
		}
		if string(stat) == "exit" || string(stat) == "quit" {
			break
		}
		//通过client执行用户指令
		result, err := s.client.Execute(stat)
		if err != nil {
			fmt.Println("Err: ", err)
		} else {
			fmt.Println(string(result))
		}
	}
}
