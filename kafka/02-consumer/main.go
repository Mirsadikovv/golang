package main

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

// Consumer читает сообщения из topic и сохраняет offset.
//
// Offset — порядковый номер сообщения внутри партиции.
// Kafka не удаляет сообщение после чтения (в отличие от очередей).
// Consumer сам управляет тем, какой offset он уже обработал (commit).
//
// Благодаря этому:
// - Можно перечитать старые сообщения (сменив GroupID или сбросив offset)
// - При падении consumer продолжит с последнего committed offset

func main() {
	// Reader — это наш consumer.
	//
	// GroupID определяет consumer group.
	// Kafka запомнит для этой группы, какой offset уже прочитан.
	// При следующем запуске чтение продолжится с места остановки.
	//
	// StartOffset: kafka.FirstOffset — читать с самого начала topic,
	// если для этой GroupID ещё нет сохранённого offset.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       "hello-kafka",
		GroupID:     "my-consumer-group",
		StartOffset: kafka.FirstOffset,

		// MinBytes/MaxBytes управляют размером fetch-запросов.
		// Kafka ждёт, пока накопится MinBytes или пройдёт MaxWait.
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
	})
	defer reader.Close()

	fmt.Println("Consumer запущен. Ожидаем сообщения (Ctrl+C для выхода)...")
	fmt.Println("─────────────────────────────────────────────────────")

	for {
		// FetchMessage — получить следующее сообщение.
		// Блокирует выполнение до появления нового сообщения.
		msg, err := reader.FetchMessage(context.Background())
		if err != nil {
			log.Fatalf("Ошибка чтения: %v", err)
		}

		fmt.Printf("Partition: %d | Offset: %d\n", msg.Partition, msg.Offset)
		fmt.Printf("Key:       %s\n", msg.Key)
		fmt.Printf("Value:     %s\n", msg.Value)
		fmt.Println("─────────────────────────────────────────────────────")

		// CommitMessages — сообщаем Kafka, что это сообщение успешно обработано.
		// Kafka запомнит offset. При следующем запуске consumer начнёт ПОСЛЕ этого offset.
		//
		// Важно: commit делаем ПОСЛЕ успешной обработки, не до.
		// Если сделать commit до обработки и процесс упадёт — сообщение будет потеряно.
		if err := reader.CommitMessages(context.Background(), msg); err != nil {
			log.Fatalf("Ошибка commit: %v", err)
		}
	}
}
