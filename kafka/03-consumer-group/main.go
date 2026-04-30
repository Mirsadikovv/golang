package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	kafka "github.com/segmentio/kafka-go"
)

// Consumer Group — горизонтальное масштабирование чтения.
//
// Идея: topic имеет N партиций. Если запустить N consumer'ов в одной группе,
// каждый получит свою партицию и будет обрабатывать её независимо.
// Пропускная способность растёт линейно с числом партиций.
//
// Правило: consumers в группе <= кол-во партиций.
// Лишние consumers будут простаивать.
//
// В этом примере мы эмулируем 3 consumers в одной группе в одном процессе.
// В реальном продакшне — это 3 отдельных инстанса сервиса.

const (
	brokerAddr  = "localhost:9092"
	topic       = "hello-kafka"
	groupID     = "scaled-consumer-group"
	numWorkers  = 3
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("Запускаем %d consumers в группе '%s'\n", numWorkers, groupID)
	fmt.Println("Сначала запусти producer, чтобы создать сообщения.")
	fmt.Println("─────────────────────────────────────────────────────")

	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go runConsumer(ctx, &wg, i)
	}

	// Ждём Ctrl+C
	<-ctx.Done()
	fmt.Println("\nОстановка всех consumers...")
	wg.Wait()
	fmt.Println("Готово.")
}

func runConsumer(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()

	// Все 3 reader используют одинаковый GroupID.
	// Kafka сама распределит партиции между ними (это называется rebalance).
	// Когда один из consumers падает — Kafka перераспределяет его партиции остальным.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddr},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			// ctx.Done() — нормальная остановка
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Consumer %d] Ошибка: %v", id, err)
			return
		}

		fmt.Printf("[Consumer %d] Partition=%d Offset=%d Key=%s Value=%s\n",
			id, msg.Partition, msg.Offset, msg.Key, msg.Value)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Consumer %d] Ошибка commit: %v", id, err)
		}
	}
}
