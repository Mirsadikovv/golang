package main

import (
	"fmt"
	"sync"
)

func main() {
	var mu1, mu2 sync.Mutex

	go func() {
		fmt.Println("1.1")
		mu1.Lock()
		defer mu1.Unlock()

		fmt.Println("1.2")
		mu2.Lock()
		defer mu2.Unlock()
	}()

	go func() {
		fmt.Println("2.1")
		mu2.Lock()
		defer mu2.Unlock()

		fmt.Println("2.2")
		mu1.Lock()
		defer mu1.Unlock()
	}()
}
