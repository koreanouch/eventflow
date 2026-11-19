package main

import (
	"context"
	"fmt"
	"microservice/cmd/config"
	"microservice/internal/order/infrastructure/processor"
	"microservice/internal/order/infrastructure/queue"
	"microservice/pkg/amqp"
	"microservice/pkg/database"
	"microservice/pkg/logger"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

var processingCount int32

func main() {
	env := os.Getenv("ENVIRONMENT")
	if env == "" {
		env = "dev"
	}

	config.LoadEnv(env)
	logger.Init(env)
	log := logger.L()
	defer log.Sync()

	log.Info("==== Order Worker Start ====")

	cfg, err := config.LoadVars()
	if err != nil {
		log.Fatal("Failed to load configuration", zap.Error(err))
	}

	log.Info("Configuration loaded successfully")

	// Connect to database
	db, err := database.NewPostgresDatabase(context.Background(), database.PostgresConfig{
		Host:     cfg.DBHost,
		Port:     cfg.DBPort,
		User:     cfg.DBUser,
		DBName:   cfg.DBName,
		Password: cfg.DBPassword,
		SSLMode:  cfg.DBSSLMode,
	})

	if err != nil {
		log.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer db.Close()

	// Connect to RabbitMQ
	amqpClient, err := amqp.NewClient(cfg.RabbitMQHost, cfg.RabbitMQPort, cfg.RabbitMQUser, cfg.RabbitMQPassword, log)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ", zap.Error(err))
	}
	defer amqpClient.Close()

	// Enable publisher confirms for retry/DLQ publishing
	if err := amqpClient.EnablePublisherConfirms(); err != nil {
		log.Fatal("Failed to enable publisher confirms", zap.Error(err))
	}

	// Setup QoS
	if err := amqpClient.Qos(1, 0); err != nil {
		log.Fatal("Failed to set QoS", zap.Error(err))
	}

	// Setup queues before consuming
	queueMgr := queue.NewQueueManager(amqpClient.Channel(), cfg, log)
	if err := queueMgr.SetupOrderQueues(context.Background()); err != nil {
		log.Fatal("Failed to setup queues", zap.Error(err))
	}

	// Create event processor
	eventProcessor := processor.NewEventProcessor(db, log)

	// Start consuming messages
	deliveries, err := amqpClient.Consume(
		cfg.RabbitMQQueueOrders,
		"order-worker",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatal("Failed to start consuming messages", zap.Error(err))
	}

	log.Info("Worker started, listening for messages", zap.String("queue", cfg.RabbitMQQueueOrders))

	// Handle graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Channel to signal processing goroutine to stop
	stopProcessing := make(chan struct{})

	// Root context for in-flight work. It is cancelled only after the graceful
	// drain window, so a deadline-exceeded request cannot orphan retry/DLQ
	// bookkeeping (those use a context independent of the message deadline).
	processingCtx, cancelProcessing := context.WithCancel(context.Background())
	defer cancelProcessing()

	// Message processing loop
	go func() {
		for {
			select {
			case delivery, ok := <-deliveries:
				if !ok {
					log.Info("Deliveries channel closed")
					return
				}
				atomic.AddInt32(&processingCount, 1)
				msgCtx, cancelMsg := context.WithTimeout(
					processingCtx,
					time.Duration(cfg.WorkerProcessingTimeoutSeconds)*time.Second,
				)
				handleMessage(msgCtx, delivery, eventProcessor, amqpClient, cfg, log)
				cancelMsg()
				atomic.AddInt32(&processingCount, -1)
			case <-stopProcessing:
				log.Info("Stopped processing new messages")
				return
			}
		}
	}()

	// Wait for shutdown signal
	sig := <-quit
	log.Info("Shutdown signal received", zap.String("signal", sig.String()))

	// Stop consuming new messages
	ch := amqpClient.Channel()
	if err := ch.Cancel("order-worker", false); err != nil {
		log.Error("Failed to cancel consumer", zap.Error(err))
	}

	// Signal processing goroutine to stop
	close(stopProcessing)

	// Wait for current messages to finish processing
	deadline := time.Now().Add(time.Duration(cfg.ShutDownTimeOut) * time.Second)
	for {
		if atomic.LoadInt32(&processingCount) == 0 {
			log.Info("All messages processed")
			break
		}
		if time.Now().After(deadline) {
			log.Warn("Shutdown timeout exceeded, aborting in-flight work",
				zap.Int32("processing_count", atomic.LoadInt32(&processingCount)),
			)
			cancelProcessing()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancelProcessing()

	log.Info("Worker shutdown completed")
}

func handleMessage(ctx context.Context, delivery amqp091.Delivery, processor *processor.EventProcessor, amqpClient *amqp.Client, cfg *config.Configuration, log *zap.Logger) {
	// Retry/DLQ publishing must not inherit the processing deadline: if the
	// event timed out, the original context is already expired.
	publishCtx, cancelPublish := context.WithTimeout(
		context.WithoutCancel(ctx),
		time.Duration(cfg.RequestTimeoutSeconds)*time.Second,
	)
	defer cancelPublish()

	// Parse message
	evt, err := processor.ParseOrderCreatedEvent(delivery.Body)
	if err != nil {
		log.Error("Failed to parse message, sending to DLQ",
			zap.String("message_id", delivery.MessageId),
			zap.Error(err),
		)
		// Send to DLQ immediately for unparseable messages
		if err := sendToDLQ(publishCtx, amqpClient, delivery.Body, delivery.MessageId, delivery.Headers, 0, cfg, log); err != nil {
			log.Error("Failed to send malformed message to DLQ, NACKing",
				zap.String("message_id", delivery.MessageId),
				zap.Error(err),
			)
			// NACK without requeue to prevent infinite loop on DLQ publish failures
			if err := delivery.Nack(false, false); err != nil {
				log.Error("Failed to NACK message", zap.String("message_id", delivery.MessageId), zap.Error(err))
			}
			return
		}
		// ACK original message after successful DLQ publish
		if err := delivery.Ack(false); err != nil {
			log.Error("Failed to ACK message after sending to DLQ", zap.String("message_id", delivery.MessageId), zap.Error(err))
		}
		return
	}

	// Get retry count from headers
	retryCount := retryCountFromHeaders(delivery.Headers)

	// Process event
	err = processor.ProcessOrderCreated(ctx, evt, "order-worker")
	if err != nil {
		log.Error("Failed to process event",
			zap.String("event_id", evt.EventID),
			zap.String("message_id", delivery.MessageId),
			zap.Int32("retry_count", retryCount),
			zap.Error(err),
		)

		// Check if we've exceeded max retries
		if retryCount >= int32(cfg.RabbitMQMaxRetries) {
			log.Error("Max retries exceeded, sending to DLQ",
				zap.String("event_id", evt.EventID),
				zap.String("message_id", delivery.MessageId),
				zap.Int32("retry_count", retryCount),
			)
			// Send to DLQ
			if err := sendToDLQ(publishCtx, amqpClient, delivery.Body, delivery.MessageId, delivery.Headers, retryCount, cfg, log); err != nil {
				log.Error("Failed to send to DLQ after max retries",
					zap.String("event_id", evt.EventID),
					zap.Error(err),
				)
				// NACK with requeue to retry later
				if err := delivery.Nack(false, true); err != nil {
					log.Error("Failed to NACK message", zap.String("message_id", delivery.MessageId), zap.Error(err))
				}
				return
			}
			// ACK original message after successful DLQ publish
			if err := delivery.Ack(false); err != nil {
				log.Error("Failed to ACK message after sending to DLQ", zap.String("message_id", delivery.MessageId), zap.Error(err))
			}
			return
		}

		// Increment retry count and send to retry queue
		nextRetryCount := retryCount + 1
		if err := sendToRetry(publishCtx, amqpClient, delivery.Body, delivery.MessageId, delivery.Headers, nextRetryCount, cfg, log); err != nil {
			log.Error("Failed to publish to retry queue",
				zap.String("event_id", evt.EventID),
				zap.Int32("retry_count", nextRetryCount),
				zap.Error(err),
			)
			// NACK with requeue - preserve original message for later retry
			if err := delivery.Nack(false, true); err != nil {
				log.Error("Failed to NACK message", zap.String("message_id", delivery.MessageId), zap.Error(err))
			}
			return
		}

		// ACK original message after successful retry queue publish
		if err := delivery.Ack(false); err != nil {
			log.Error("Failed to ACK message after sending to retry", zap.String("message_id", delivery.MessageId), zap.Error(err))
		}
		return
	}

	// Success - ACK the message
	if err := delivery.Ack(false); err != nil {
		log.Error("Failed to ACK message on success", zap.String("message_id", delivery.MessageId), zap.Error(err))
	} else {
		log.Info("Message processed successfully",
			zap.String("event_id", evt.EventID),
			zap.String("message_id", delivery.MessageId),
		)
	}
}

// retryCountFromHeaders tolera los tipos que RabbitMQ puede usar al decodificar
// un entero de cabecera (int32, int64, float64 o int).
func retryCountFromHeaders(headers amqp091.Table) int32 {
	value, ok := headers["x-retry-count"]
	if !ok {
		return 0
	}

	var count int64
	switch v := value.(type) {
	case int32:
		count = int64(v)
	case int64:
		count = v
	case int:
		count = int64(v)
	case float64:
		count = int64(v)
	default:
		return 0
	}
	if count < 0 {
		return 0
	}
	return int32(count)
}

func sendToRetry(ctx context.Context, amqpClient *amqp.Client, body []byte, messageID string, headers amqp091.Table, retryCount int32, cfg *config.Configuration, log *zap.Logger) error {
	headers = copyHeaders(headers)
	headers["x-retry-count"] = retryCount

	retryQueue := queue.RetryQueueName(cfg.RabbitMQQueueRetry, int(retryCount))
	delayMs := queue.RetryDelayMs(cfg.RabbitMQRetryDelayMs, cfg.RabbitMQMaxRetryDelayMs, int(retryCount))

	msg := amqp091.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp091.Persistent,
		MessageId:    messageID,
		Headers:      headers,
	}

	err := amqpClient.PublishWithConfirm(ctx, cfg.RabbitMQExchange+".retry", retryQueue, true, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to retry queue: %w", err)
	}

	log.Info("Message sent to retry queue",
		zap.String("message_id", messageID),
		zap.Int32("retry_count", retryCount),
		zap.String("queue", retryQueue),
		zap.Int("delay_ms", delayMs),
	)

	return nil
}

func sendToDLQ(ctx context.Context, amqpClient *amqp.Client, body []byte, messageID string, headers amqp091.Table, retryCount int32, cfg *config.Configuration, log *zap.Logger) error {
	headers = copyHeaders(headers)
	headers["x-retry-count"] = retryCount
	headers["x-dead-letter-reason"] = "max_retries_exceeded"

	msg := amqp091.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp091.Persistent,
		MessageId:    messageID,
		Timestamp:    time.Now().UTC(),
		Headers:      headers,
	}

	err := amqpClient.PublishWithConfirm(ctx, cfg.RabbitMQExchange, "dead-letter.retry", true, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to DLQ: %w", err)
	}

	log.Info("Message sent to DLQ",
		zap.String("message_id", messageID),
		zap.Int32("retry_count", retryCount),
		zap.String("queue", cfg.RabbitMQQueueDLQ),
	)

	return nil
}

func copyHeaders(headers amqp091.Table) amqp091.Table {
	out := make(amqp091.Table, len(headers)+2)
	for key, value := range headers {
		out[key] = value
	}
	return out
}
