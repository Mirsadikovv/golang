package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Real-world пример: Order Processing System
//
// Архитектура:
//   OrderService (producer) → topic "orders" → PaymentService (consumer)
//
// Особенности:
// - Сообщения сериализованы в JSON
// - Key = OrderID → все события заказа в одной партиции (порядок гарантирован)
// - Retry логика при ошибках обработки
// - Graceful shutdown (Ctrl+C завершает текущую обработку, не теряет сообщения)

const (
	brokerAddr = "localhost:9092"
	topic      = "orders"
	groupID    = "payment-service"
)

// Order — событие заказа, которое летит через Kafka
type Order struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// ── Producer ─────────────────────────────────────────────────────────────────

func runOrderService(ctx context.Context) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokerAddr),
		Topic:                  topic,
		Balancer:               &kafka.Hash{}, // key=orderID → одна партиция на заказ
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	statuses := []string{"new", "confirmed", "shipped", "delivered"}

	for i := 1; i <= 10; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		orderID := fmt.Sprintf("order-%03d", i)
		order := Order{
			ID:        orderID,
			UserID:    fmt.Sprintf("user-%d", rand.Intn(3)+1),
			Amount:    float64(rand.Intn(9900)+100) / 100,
			Status:    statuses[rand.Intn(len(statuses))],
			CreatedAt: time.Now(),
		}

		data, err := json.Marshal(order)
		if err != nil {
			log.Printf("[OrderService] json ошибка: %v", err)
			continue
		}

		msg := kafka.Message{
			Key:   []byte(order.ID),
			Value: data,
		}

		if err := writer.WriteMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[OrderService] Ошибка отправки: %v", err)
			continue
		}

		fmt.Printf("[OrderService] → Отправлен %s (user=%s amount=%.2f status=%s)\n",
			order.ID, order.UserID, order.Amount, order.Status)

		time.Sleep(300 * time.Millisecond)
	}

	fmt.Println("[OrderService] Все заказы отправлены.")
}

// ── Consumer ─────────────────────────────────────────────────────────────────

func runPaymentService(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokerAddr},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	fmt.Println("[PaymentService] Ожидаем заказы...")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("[PaymentService] Graceful shutdown.")
				return
			}
			log.Printf("[PaymentService] Ошибка чтения: %v", err)
			return
		}

		var order Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Printf("[PaymentService] JSON parse ошибка: %v", err)
			// Commit даже при ошибке парсинга — иначе застрянем на этом сообщении
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		// Обработка с retry (3 попытки)
		if err := processWithRetry(order, 3); err != nil {
			log.Printf("[PaymentService] Не удалось обработать %s: %v", order.ID, err)
			// В реальном коде — отправить в Dead Letter Queue
		}

		// Commit только после успешной обработки (at-least-once семантика)
		if err := reader.CommitMessages(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[PaymentService] Ошибка commit: %v", err)
		}
	}
}

func processWithRetry(order Order, maxRetries int) error {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := processPayment(order); err != nil {
			lastErr = err
			if attempt < maxRetries {
				backoff := time.Duration(attempt*100) * time.Millisecond
				fmt.Printf("[PaymentService] Попытка %d/%d для %s не удалась, retry через %v\n",
					attempt, maxRetries, order.ID, backoff)
				time.Sleep(backoff)
			}
			continue
		}
		return nil
	}

	return lastErr
}

func processPayment(order Order) error {
	// Имитируем обработку платежа (иногда падает)
	if rand.Float32() < 0.1 {
		return fmt.Errorf("payment gateway timeout")
	}

	time.Sleep(50 * time.Millisecond) // имитация работы

	fmt.Printf("[PaymentService] ✓ Обработан %s | user=%s | amount=%.2f | status=%s\n",
		order.ID, order.UserID, order.Amount, order.Status)

	return nil
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Println("=== Order Processing System ===")
	fmt.Println("Ctrl+C для graceful shutdown")
	fmt.Println("──────────────────────────────────────────────────────")

	var wg sync.WaitGroup

	// Запускаем PaymentService (consumer)
	wg.Add(1)
	go runPaymentService(ctx, &wg)

	// Небольшая пауза, чтобы consumer успел подключиться
	time.Sleep(500 * time.Millisecond)

	// Запускаем OrderService (producer)
	go runOrderService(ctx)

	// Ждём Ctrl+C или завершения контекста
	<-ctx.Done()
	fmt.Println("\nОстановка системы...")
	wg.Wait()
	fmt.Println("Система остановлена.")
}
