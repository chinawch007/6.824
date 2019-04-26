package main

import (
	"fmt"
	"reflect"
)

type interTest interface {
	out()
}

func fuck(in interTest) {
	in.out()
}

type classTest struct {
	data int
}

//类的函数就这么实现吗?以类为参数的函数?
func (c classTest) out() {
	fmt.Println(c.data)
}

func main() {
	var a int = 10

	fmt.Println("hello world")
	fmt.Println(reflect.ValueOf(a))
	fmt.Println(reflect.TypeOf(a))

	//这个var必须在什么情形下加
	var c classTest
	fuck(c)
}
