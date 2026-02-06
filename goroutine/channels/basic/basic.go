package main

import "fmt"

func worker(done chan string, id int) {

	done <- fmt.Sprintf("Worker %d is done", id)

	// fmt.Println(done) // This would print address of the channel
}

func main() {
	done := make(chan string)
	defer close(done)
	go worker(done, 2)
	go worker(done, 1)

	fmt.Println(<-done)
	fmt.Println(<-done)

	fmt.Println("All workers are done")
}

/*
КОММЕНТАРИИ (только основные моменты):

1. Создание канала: done := make(chan string)
   - Каналы используются для передачи данных между горутинами.

2. Отправка в канал: done <- fmt.Sprintf("Worker %d is done", id)
   - Горутинa отправляет сообщение в канал, сигнализируя о завершении работы.

3. Получение из канала: fmt.Println(<-done)
   - Главная горутина получает сообщение из канала, блокируясь до тех пор, пока сообщение не будет доступно.

4. Порядок выполнения: Поскольку горутины выполняются параллельно, порядок получения сообщений из канала может отличаться от порядка их отправки.

*/
