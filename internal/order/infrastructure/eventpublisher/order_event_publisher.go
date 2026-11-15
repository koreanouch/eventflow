package eventpublisher

import (
	"context"
	"encoding/json"
	"fmt"
	"microservice/cmd/config"
	"microservice/internal/order/domain/event"
	"microservice/pkg/amqp"

	amqplib "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Publisher interface {
	PublishOrderCreated(ctx context.Context, evt *event.OrderCreated) error
}

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
		},
	}

	err = p.client.PublishWithConfirm(ctx, p.cfg.RabbitMQExchange, p.cfg.RabbitMQRoutingKey, true, msg)
	if err != nil {
		return fmt.Errorf("failed to publish order created event: %w", err)
	}

	p.log.Info("Order created event published",
		zap.String("event_id", evt.EventID),
		zap.String("order_id", evt.OrderID),
		zap.String("customer_id", evt.CustomerID),
	)

	return nil
}
