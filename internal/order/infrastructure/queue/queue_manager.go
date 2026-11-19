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
	// Declare main exchange (topic to support routing)
	if err := qm.channel.ExchangeDeclare(
		qm.cfg.RabbitMQExchange,
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
	if _, err := qm.channel.QueueDeclare(
		qm.cfg.RabbitMQQueueOrders,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare main queue: %w", err)
	}

	// Bind main queue
	if err := qm.channel.QueueBind(
		qm.cfg.RabbitMQQueueOrders,
		qm.cfg.RabbitMQRoutingKey,
		qm.cfg.RabbitMQExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to bind main queue: %w", err)
	}

	// Declare DLX (dead-letter exchange) - points to main exchange so messages can be reprocessed
	if err := qm.channel.ExchangeDeclare(
		qm.cfg.RabbitMQExchange+".dlx",
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
	if err := qm.channel.ExchangeDeclare(
		qm.cfg.RabbitMQExchange+".retry",
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
	for attempt := 1; attempt <= qm.cfg.RabbitMQMaxRetries; attempt++ {
		retryQueue := RetryQueueName(qm.cfg.RabbitMQQueueRetry, attempt)
		delayMs := RetryDelayMs(qm.cfg.RabbitMQRetryDelayMs, qm.cfg.RabbitMQMaxRetryDelayMs, attempt)

		retryArgs := amqp.Table{
			"x-dead-letter-exchange":    qm.cfg.RabbitMQExchange + ".dlx",
			"x-dead-letter-routing-key": qm.cfg.RabbitMQRoutingKey,
			"x-message-ttl":             int64(delayMs),
		}
		if _, err := qm.channel.QueueDeclare(
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
		if err := qm.channel.QueueBind(
			retryQueue,
			retryQueue,
			qm.cfg.RabbitMQExchange+".retry",
			false,
			nil,
		); err != nil {
			return fmt.Errorf("failed to bind retry queue %s: %w", retryQueue, err)
		}
	}

	// Bind DLX to main queue
	if err := qm.channel.QueueBind(
		qm.cfg.RabbitMQQueueOrders,
		qm.cfg.RabbitMQRoutingKey,
		qm.cfg.RabbitMQExchange+".dlx",
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to bind main queue to DLX: %w", err)
	}

	// Declare DLQ for messages that exhausted retries
	if _, err := qm.channel.QueueDeclare(
		qm.cfg.RabbitMQQueueDLQ,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to declare DLQ: %w", err)
	}

	// Bind DLQ to main exchange for manual inspection
	if err := qm.channel.QueueBind(
		qm.cfg.RabbitMQQueueDLQ,
		"dead-letter.*",
		qm.cfg.RabbitMQExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("failed to bind DLQ to exchange: %w", err)
	}

	qm.log.Info("Order queues setup completed",
		zap.String("main_queue", qm.cfg.RabbitMQQueueOrders),
		zap.String("retry_queue_base", qm.cfg.RabbitMQQueueRetry),
		zap.String("dlq", qm.cfg.RabbitMQQueueDLQ),
		zap.Int("retry_queues", qm.cfg.RabbitMQMaxRetries),
		zap.Int("retry_delay_ms", qm.cfg.RabbitMQRetryDelayMs),
		zap.Int("retry_max_delay_ms", qm.cfg.RabbitMQMaxRetryDelayMs),
	)

	return nil
}
