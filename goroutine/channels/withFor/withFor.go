package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan int)

	go func() {
		ch <- 1
		time.Sleep(time.Second)
		ch <- 2
		time.Sleep(time.Second)
		ch <- 3
		close(ch) // закрываем, чтобы for range остановился (если не закрыть будет deadlock)
	}()

	for v := range ch { // читаем до закрытия
		fmt.Println(v)
	}
}
