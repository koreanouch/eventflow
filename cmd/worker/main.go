package main

import (
	"context"
	"errors"
	"fmt"
	"microservice/cmd/config"
	"microservice/internal/order/infrastructure/consumer"
	"microservice/internal/order/infrastructure/processor"
	"microservice/internal/order/infrastructure/queue"
	"microservice/pkg/amqp"
	"microservice/pkg/database"
	"microservice/pkg/logger"
	"microservice/pkg/middleware"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
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

	// Las métricas del worker se exponen en un servidor HTTP independiente para
	// poder consultar reintentos y DLQ sin un consumidor adicional.
	metricsServer := startMetricsServer(cfg.WorkerMetricsPort, log)
	defer shutdownMetricsServer(metricsServer, log)

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

	// Connect to RabbitMQ. El cliente se reconecta solo y los hooks restauran el
	// QoS y la topología (exchanges, colas, reintentos y DLQ) en cada reconexión.
	amqpClient := amqp.NewLazyClient(cfg.RabbitMQHost, cfg.RabbitMQPort, cfg.RabbitMQUser, cfg.RabbitMQPassword, log)
	defer amqpClient.Close()

	// Enable publisher confirms for retry/DLQ publishing
	if err := amqpClient.EnablePublisherConfirms(); err != nil {
		log.Fatal("Failed to enable publisher confirms", zap.Error(err))
	}

	amqpClient.OnReconnect(func(ch *amqplib.Channel) error {
		if err := ch.Qos(1, 0, false); err != nil {
			return fmt.Errorf("failed to set QoS: %w", err)
		}
		return queue.SetupOrderQueues(ch, cfg, log)
	})

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), 30*time.Second)
	err = amqpClient.EnsureConnected(connectCtx)
	cancelConnect()
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ", zap.Error(err))
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

	// Message processing loop. Si la conexión con RabbitMQ se cae, el canal de
	// deliveries se cierra: se vuelve a conectar y se reanuda el consumo en lugar
	// de terminar el worker en silencio.
	go consumeLoop(amqpClient, consumerHandler, cfg, log, processingCtx, stopProcessing)

	// Wait for shutdown signal
	sig := <-quit
	log.Info("Shutdown signal received", zap.String("signal", sig.String()))

	// Stop consuming new messages
	if ch := amqpClient.Channel(); ch != nil {
		if err := ch.Cancel("order-worker", false); err != nil {
			log.Error("Failed to cancel consumer", zap.Error(err))
		}
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

// consumeLoop mantiene vivo el consumo: si el canal de deliveries se cierra
// (caída o reinicio del broker) reintenta la conexión y vuelve a consumir.
func consumeLoop(
	amqpClient *amqp.Client,
	handler *consumer.Handler,
	cfg *config.Configuration,
	log *zap.Logger,
	processingCtx context.Context,
	stop <-chan struct{},
) {
	const retryDelay = 2 * time.Second

	for {
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
			log.Error("Failed to start consuming messages; retrying", zap.Error(err))
			if !waitOrStop(stop, retryDelay) {
				return
			}
			continue
		}

		log.Info("Worker started, listening for messages", zap.String("queue", cfg.RabbitMQQueueOrders))

	consume:
		for {
			select {
			case delivery, ok := <-deliveries:
				if !ok {
					break consume
				}
				atomic.AddInt32(&processingCount, 1)
				handler.Handle(processingCtx, consumer.Message{
					Body:      delivery.Body,
					MessageID: delivery.MessageId,
					Headers:   delivery.Headers,
					Ack:       delivery.Ack,
					Nack:      delivery.Nack,
				})
				atomic.AddInt32(&processingCount, -1)
			case <-stop:
				log.Info("Stopped processing new messages")
				return
			}
		}

		log.Warn("RabbitMQ deliveries channel closed; reconnecting")
		reconnectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := amqpClient.EnsureConnected(reconnectCtx); err != nil {
			log.Error("Failed to reconnect to RabbitMQ; retrying", zap.Error(err))
		}
		cancel()
		if !waitOrStop(stop, retryDelay) {
			return
		}
	}
}

func waitOrStop(stop <-chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-stop:
		return false
	}
}

// startMetricsServer expone /metrics y /healthz del worker en un puerto
// separado. No interfiere con el consumo de mensajes.
func startMetricsServer(port int, log *zap.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", middleware.MetricsHTTPHandler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("Worker metrics server started", zap.Int("port", port))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Worker metrics server stopped unexpectedly", zap.Error(err))
		}
	}()

	return server
}

func shutdownMetricsServer(server *http.Server, log *zap.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Error("Failed to shutdown metrics server", zap.Error(err))
	}
}
