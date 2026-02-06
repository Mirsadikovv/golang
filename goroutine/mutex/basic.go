package main

import (
	"fmt"
	"sync"
)

func main() {
	var (
		counter int
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	for i := 0; i < 5; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			mu.Lock() // 🔒 захватываем mutex(блокируем запись в counter)
			counter++
			mu.Unlock() // 🔓 освобождаем mutex(освобождаем доступ к counter)
		}()
	}

	wg.Wait()
	fmt.Println("Counter:", counter)
}

/*
КОММЕНТАРИИ самые важные(коротко и ясно):
1. Mutex используется для синхронизации доступа к общему ресурсу (в данном случае переменной counter).
2. Без mutex возможна гонка данных (race condition), что может привести к некорректному результату.
*/
