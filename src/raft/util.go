package raft

import "fmt"

// Debugging
const Debug = 0

func DPrintf(logHead string, format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf("%v", logHead)
		fmt.Printf(format, a...)
	}
	return
}

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Println(a...)
	}
	return
}

func DPrint(a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Print(a...)
	}
	return
}
