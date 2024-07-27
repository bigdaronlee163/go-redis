package main

import (
	"fmt"
	"os"
)

func main() {
	// os.Args 是一个字符串切片，包含了所有命令行参数
	args := os.Args

	// 打印所有的命令行参数
	for i, arg := range args {
		fmt.Printf("Argument %d: %s\n", i, arg)
	}
}

// 执行下面的命令运行。或者go build 然后运行。
// go run args_demo.go arg1 arg2 arg3
