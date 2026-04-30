package main

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Партиции и ключи сообщений.
//
// Ключевая идея: если сообщения относятся к одной сущности (например, один заказ),
// они должны попасть в одну партицию — иначе порядок не гарантирован.
//
// Kafka вычисляет партицию как: hash(key) % numPartitions
// Одинаковый key → одна партиция → гарантированный порядок.
//
// Пример: события одного пользователя должны обрабатываться по порядку.
// Key = userID гарантирует, что все события user-123 попадут в partition X.

func main() {
	// Создаём topic с 3 партициями вручную (для демонстрации).
	// В продакшне это делается через terraform/helm/admin API.
	createTopic("user-events", 3)

	// Producer с Hash балансировщиком — ключ определяет партицию
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "user-events",

		// Hash: hash(key) % numPartitions
		// Одинаковый key → всегда одна партиция
		Balancer: &kafka.Hash{},
	})
	defer writer.Close()

	// Имитируем события разных пользователей
	events := []struct {
		userID string
		action string
	}{
		{"user-1", "login"},
		{"user-2", "login"},
		{"user-1", "view_product"},  // user-1, партиция та же что и первый login
		{"user-3", "login"},
		{"user-2", "add_to_cart"},   // user-2, та же партиция
		{"user-1", "checkout"},      // user-1, та же партиция → порядок: login → view → checkout
		{"user-2", "payment"},       // user-2, та же партиция → порядок: login → cart → payment
	}

	fmt.Println("Отправляем события пользователей (key = userID):")
	fmt.Println("─────────────────────────────────────────────────────")

	for _, e := range events {
		msg := kafka.Message{
			Key:   []byte(e.userID),
			Value: []byte(fmt.Sprintf(`{"user":"%s","action":"%s","ts":"%s"}`, e.userID, e.action, time.Now().Format(time.RFC3339))),
		}

		if err := writer.WriteMessages(context.Background(), msg); err != nil {
			log.Fatalf("Ошибка: %v", err)
		}

		// Посмотрим в какую партицию попало сообщение
		partition := hashPartition(e.userID, 3)
		fmt.Printf("Key=%-8s Action=%-15s → Partition %d\n", e.userID, e.action, partition)
	}

	fmt.Println("\nВывод: все события одного user попали в одну партицию.")
	fmt.Println("Consumer читая партицию увидит события в правильном порядке.")
}

func hashPartition(key string, numPartitions int) int {
	// Упрощённая версия того, что делает kafka.Hash{}
	h := 0
	for _, c := range key {
		h = 31*h + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h % numPartitions
}

func createTopic(topic string, partitions int) {
	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Printf("Не могу подключиться к Kafka (возможно уже запущена): %v", err)
		return
	}
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		// Игнорируем "topic already exists"
		fmt.Printf("Topic '%s': %v\n", topic, err)
		return
	}
	fmt.Printf("Topic '%s' создан с %d партициями\n\n", topic, partitions)
}
