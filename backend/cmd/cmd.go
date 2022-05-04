package cmd

import (
	"github.com/spf13/cobra"
)

//根命令行，用于初始化配置文件等等，虽然可能现在都是写死的
var (
	rootCmd = &cobra.Command{}
)

func Execute() error {
	rootCmd.RunE = func(cmd *cobra.Command, args []string) error {
		//进行初始化操作
	}
}
