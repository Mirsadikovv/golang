package main

import (
	"fmt"
	"time"
)

func doSomething() {
	fmt.Println("I did something")
}

func main() {

	go doSomething()
	time.Sleep(time.Second)
}
