# Apache Kafka — полное руководство для Go разработчика

## Что такое Kafka?

Apache Kafka — распределённая платформа для потоковой передачи событий (event streaming).
Используется для:
- Асинхронного общения между микросервисами
- Обработки событий в реальном времени
- Логирования и аудита
- ETL пайплайнов

---

## Архитектура

```
┌─────────────┐     publish      ┌───────────────────────────────────┐
│  Producer   │ ───────────────► │             Kafka Broker           │
└─────────────┘                  │                                   │
                                 │  Topic: "orders"                  │
┌─────────────┐     subscribe    │  ┌──────────┬──────────┬────────┐ │
│  Consumer   │ ◄─────────────── │  │Partition0│Partition1│Part..2 │ │
└─────────────┘                  │  └──────────┴──────────┴────────┘ │
                                 └───────────────────────────────────┘
```

---

## Ключевые концепции

### Broker
Сервер Kafka. Хранит данные, обслуживает producers и consumers.
В продакшне обычно кластер из 3+ брокеров.

### Topic
Именованный поток событий (аналог таблицы в БД или очереди).
Пример: `orders`, `payments`, `user-events`.

### Partition
Topic делится на партиции — упорядоченный, неизменяемый лог сообщений.
- Каждое сообщение внутри партиции имеет **offset** (порядковый номер)
- Партиции позволяют **горизонтально масштабировать** чтение
- Порядок гарантируется **только внутри одной партиции**

```
Topic "orders" с 3 партициями:

Partition 0: [msg0] [msg1] [msg4] [msg7] ...
Partition 1: [msg2] [msg5] [msg8] ...
Partition 2: [msg3] [msg6] [msg9] ...
              ↑
           offset=0
```

### Offset
Уникальный номер сообщения внутри партиции. Consumer запоминает (commit),
какой offset он уже прочитал — это позволяет продолжить с нужного места после перезапуска.

### Producer
Публикует сообщения в topic. Может указать ключ — тогда Kafka гарантирует,
что все сообщения с одним ключом попадут в **одну партицию** (порядок сохранится).

### Consumer
Читает сообщения из topic. Запоминает offset последнего прочитанного сообщения.

### Consumer Group
Группа consumers, которые вместе читают один topic.
- Каждая партиция назначается **только одному** consumer в группе
- Позволяет масштабировать обработку горизонтально

```
Topic: 3 партиции, Consumer Group из 3 consumers:

Partition 0 ──► Consumer A
Partition 1 ──► Consumer B
Partition 2 ──► Consumer C

Если добавить 4-й consumer — он будет простаивать (партиций меньше).
Если убрать Consumer B — Partition 1 перейдёт к A или C (rebalance).
```

---

## Гарантии доставки

| Режим           | Описание                                          | Риск                  |
|-----------------|---------------------------------------------------|-----------------------|
| At most once    | Сообщение доставляется 0 или 1 раз                | Потеря данных         |
| At least once   | Сообщение доставляется 1+ раз (по умолчанию)      | Дубликаты             |
| Exactly once    | Ровно 1 раз (idempotent producer + транзакции)    | Сложная настройка     |

---

## Библиотека: segmentio/kafka-go

Чистый Go, без CGo зависимостей. Простой и понятный API.

```bash
go get github.com/segmentio/kafka-go
```

---

## Примеры в этом разделе

| Файл                        | Что изучаем                              |
|-----------------------------|------------------------------------------|
| `01-producer/main.go`       | Отправка сообщений в topic               |
| `02-consumer/main.go`       | Чтение сообщений, commit offset          |
| `03-consumer-group/main.go` | Масштабирование через consumer groups    |
| `04-partitions/main.go`     | Ключи сообщений и порядок в партициях    |
| `05-real-world/main.go`     | Order processing: JSON события + retry   |

---

## Запуск локальной среды

```bash
# Запустить Kafka + Zookeeper
docker-compose up -d

# Создать topic вручную (опционально — kafka-go создаёт автоматически)
docker exec -it kafka kafka-topics.sh \
  --create --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Посмотреть список topics
docker exec -it kafka kafka-topics.sh \
  --list --bootstrap-server localhost:9092

# Читать сообщения из консоли
docker exec -it kafka kafka-console-consumer.sh \
  --topic orders \
  --from-beginning \
  --bootstrap-server localhost:9092
```

---

## Частые ошибки

**dial tcp: connection refused** — Kafka не запущена, проверь `docker-compose up`.

**Leader Not Available** — topic только создаётся, подождать 1-2 секунды.

**Consumer не получает сообщения** — проверь `GroupID` и `StartOffset`.
Если group уже читала topic, она начнёт с последнего committed offset, а не с начала.
Смени `GroupID` или используй `FirstOffset` при создании reader.
