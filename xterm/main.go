package main

import (
	"syscall/js"
)

func write(args ...interface{}) {
	js.Global().Get("term").Call("write", args...)
}

func main() {
	//write("this is a test message")
}