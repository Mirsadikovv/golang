package main

import (
	"fmt"
	"sync"
)

func doSomething(wg *sync.WaitGroup, count int16) {

	defer wg.Done()
	fmt.Println("WaitGroup", count)
}

func main() {

	wg := &sync.WaitGroup{}

	wg.Add(4)
	go doSomething(wg, 1)
	go doSomething(wg, 2)
	go doSomething(wg, 3)
	go doSomething(wg, 4)
	wg.Wait()
}
