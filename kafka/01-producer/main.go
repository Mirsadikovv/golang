package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Producer отправляет сообщения в topic.
//
// Как это работает:
// 1. Создаём Writer — он знает адрес брокера и название topic
// 2. Вызываем WriteMessages — Kafka принимает сообщения и сохраняет их в партиции
// 3. Consumer сможет прочитать эти сообщения позже (даже через часы/дни)

func main() {
	// Writer — это наш producer.
	// Balancer: LeastBytes распределяет сообщения по партициям равномерно по объёму.
	// Если не указан ключ — Kafka сама выбирает партицию.
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "hello-kafka",
		Balancer: &kafka.LeastBytes{},

		// Ждём подтверждения от брокера перед возвратом из WriteMessages.
		// RequiredAcks: -1 (All) — максимальная надёжность (все реплики подтвердили).
		RequiredAcks: 1, // достаточно одного брокера для примера
	})
	defer writer.Close()

	fmt.Println("Producer запущен. Отправляем 5 сообщений...")

	for i := 1; i <= 5; i++ {
		msg := kafka.Message{
			// Key используется для выбора партиции.
			// Сообщения с одинаковым Key всегда попадают в одну партицию — порядок сохраняется.
			// Без Key — балансировщик выбирает партицию сам.
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("Сообщение номер %d, время: %s", i, time.Now().Format("15:04:05"))),
		}

		err := writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Fatalf("Ошибка отправки сообщения: %v", err)
		}

		fmt.Printf("✓ Отправлено: %s\n", msg.Value)
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("\nВсе сообщения отправлены!")
	fmt.Println("Запусти 02-consumer, чтобы прочитать их.")

	// Статистика writer-а
	stats := writer.Stats()
	fmt.Printf("\nСтатистика:\n  Отправлено: %d сообщений\n  Байт:       %d\n", stats.Messages, stats.Bytes)
}
