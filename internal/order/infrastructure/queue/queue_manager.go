package queue

import (
	"context"
	"fmt"
	"microservice/cmd/config"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type QueueManager struct {
	channel *amqp.Channel
	cfg     *config.Configuration
	log     *zap.Logger
}

func NewQueueManager(channel *amqp.Channel, cfg *config.Configuration, log *zap.Logger) *QueueManager {
	return &QueueManager{
		channel: channel,
		cfg:     cfg,
		log:     log,
	}
}

func (qm *QueueManager) SetupOrderQueues(ctx context.Context) error {
	return SetupOrderQueues(qm.channel, qm.cfg, qm.log)
}

// SetupOrderQueues declara exchanges, colas, reintentos y DLQ sobre un canal
// concreto. Al recibir el canal como parámetro puede reutilizarse como hook de
// reconexión del cliente AMQP sin depender de un canal concreto.
func SetupOrderQueues(channel *amqp.Channel, cfg *config.Configuration, log *zap.Logger) error {
	// Declare main exchange (topic to support routing)
	if err := channel.ExchangeDeclare(
		cfg.RabbitMQExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare main exchange: %w", err)
	}

	// Declare main queue (no DLX, no TTL)
	if _, err := channel.QueueDeclare(
		cfg.RabbitMQQueueOrders,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare main queue: %w", err)
	}

	// Bind main queue
	if err := channel.QueueBind(
		cfg.RabbitMQQueueOrders,
		cfg.RabbitMQRoutingKey,
		cfg.RabbitMQExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to bind main queue: %w", err)
	}

	// Declare DLX (dead-letter exchange) - points to main exchange so messages can be reprocessed
	if err := channel.ExchangeDeclare(
		cfg.RabbitMQExchange+".dlx",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare DLX: %w", err)
	}

	// Declare retry exchange
	if err := channel.ExchangeDeclare(
		cfg.RabbitMQExchange+".retry",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare retry exchange: %w", err)
	}

	// Declare one retry queue per attempt. Each queue has its own TTL, so the
	// wait grows exponentially instead of staying fixed at a single delay.
	for attempt := 1; attempt <= cfg.RabbitMQMaxRetries; attempt++ {
		retryQueue := RetryQueueName(cfg.RabbitMQQueueRetry, attempt)
		delayMs := RetryDelayMs(cfg.RabbitMQRetryDelayMs, cfg.RabbitMQMaxRetryDelayMs, attempt)

		retryArgs := amqp.Table{
			"x-dead-letter-exchange":    cfg.RabbitMQExchange + ".dlx",
			"x-dead-letter-routing-key": cfg.RabbitMQRoutingKey,
			"x-message-ttl":             int64(delayMs),
		}
		if _, err := channel.QueueDeclare(
			retryQueue,
			true,
			false,
			false,
			false,
			retryArgs,
		); err != nil {
			return fmt.Errorf("failed to declare retry queue %s: %w", retryQueue, err)
		}

		// Bind retry queue to retry exchange using its own name as routing key
		if err := channel.QueueBind(
			retryQueue,
			retryQueue,
			cfg.RabbitMQExchange+".retry",
			false,
			nil,
		); err != nil {
			return fmt.Errorf("failed to bind retry queue %s: %w", retryQueue, err)
		}
	}

	// Bind DLX to main queue
	if err := channel.QueueBind(
		cfg.RabbitMQQueueOrders,
		cfg.RabbitMQRoutingKey,
		cfg.RabbitMQExchange+".dlx",
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to bind main queue to DLX: %w", err)
	}

	// Declare DLQ for messages that exhausted retries
	if _, err := channel.QueueDeclare(
		cfg.RabbitMQQueueDLQ,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare DLQ: %w", err)
	}

	// Bind DLQ to main exchange for manual inspection
	if err := channel.QueueBind(
		cfg.RabbitMQQueueDLQ,
		"dead-letter.*",
		cfg.RabbitMQExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to bind DLQ to exchange: %w", err)
	}

	log.Info("Order queues setup completed",
		zap.String("main_queue", cfg.RabbitMQQueueOrders),
		zap.String("retry_queue_base", cfg.RabbitMQQueueRetry),
		zap.String("dlq", cfg.RabbitMQQueueDLQ),
		zap.Int("retry_queues", cfg.RabbitMQMaxRetries),
		zap.Int("retry_delay_ms", cfg.RabbitMQRetryDelayMs),
		zap.Int("retry_max_delay_ms", cfg.RabbitMQMaxRetryDelayMs),
	)

	return nil
}
