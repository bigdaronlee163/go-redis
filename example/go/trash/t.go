package main

import "fmt"

func init() {
	fmt.Println("init invoked")
}

func main() {
	// ./t.go:10:2: undefined: init
	// init()
}
