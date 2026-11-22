package eventpublisher

import (
	"context"
	"encoding/json"
	"fmt"
	"microservice/cmd/config"
	"microservice/internal/order/application/port"
	"microservice/internal/order/domain/event"
	"microservice/pkg/amqp"
	"microservice/pkg/metrics"

	amqplib "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

var _ port.OrderCreatedPublisher = (*OrderEventPublisher)(nil)

type OrderEventPublisher struct {
	client *amqp.Client
	cfg    *config.Configuration
	log    *zap.Logger
}

func NewOrderEventPublisher(client *amqp.Client, cfg *config.Configuration, log *zap.Logger) *OrderEventPublisher {
	return &OrderEventPublisher{
		client: client,
		cfg:    cfg,
		log:    log,
	}
}

func (p *OrderEventPublisher) PublishOrderCreated(ctx context.Context, evt *event.OrderCreated) error {
	if evt == nil {
		return fmt.Errorf("event cannot be nil")
	}

	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := amqplib.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqplib.Persistent,
		MessageId:    evt.EventID,
		Timestamp:    evt.Timestamp,
		Headers: amqplib.Table{
			"x-event-type": evt.EventType,
			"x-request-id": evt.RequestID,
		},
	}

	err = p.client.PublishWithConfirm(ctx, p.cfg.RabbitMQExchange, p.cfg.RabbitMQRoutingKey, true, msg)
	if err != nil {
		metrics.Inc(metrics.OrderEventPublishFailuresTotal, nil)
		return fmt.Errorf("failed to publish order created event: %w", err)
	}

	metrics.Inc(metrics.OrderEventsPublishedTotal, nil)

	p.log.Info("Order created event published",
		zap.String("event_id", evt.EventID),
		zap.String("order_id", evt.OrderID),
		zap.String("customer_id", evt.CustomerID),
		zap.String("request_id", evt.RequestID),
	)

	return nil
}
