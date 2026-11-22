package main

import (
	"context"
	"microservice/cmd/config"
	"microservice/internal/order/infrastructure/consumer"
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

	// Create event processor and consumer handler
	eventProcessor := processor.NewEventProcessor(db, log)
	consumerHandler := consumer.NewHandler(eventProcessor, amqpClient, consumer.Config{
		ConsumerName:      "order-worker",
		RetryExchange:     cfg.RabbitMQExchange + ".retry",
		RetryQueueBase:    cfg.RabbitMQQueueRetry,
		DLQExchange:       cfg.RabbitMQExchange,
		DLQRoutingKey:     "dead-letter.retry",
		MaxRetries:        cfg.RabbitMQMaxRetries,
		RetryDelayMs:      cfg.RabbitMQRetryDelayMs,
		MaxRetryDelayMs:   cfg.RabbitMQMaxRetryDelayMs,
		ProcessingTimeout: time.Duration(cfg.WorkerProcessingTimeoutSeconds) * time.Second,
		PublishTimeout:    time.Duration(cfg.RequestTimeoutSeconds) * time.Second,
	}, log)

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
				consumerHandler.Handle(processingCtx, consumer.Message{
					Body:      delivery.Body,
					MessageID: delivery.MessageId,
					Headers:   delivery.Headers,
					Ack:       delivery.Ack,
					Nack:      delivery.Nack,
				})
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
