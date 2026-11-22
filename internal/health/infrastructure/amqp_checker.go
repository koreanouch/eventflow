package infrastructure

import (
	"context"
	"fmt"

	"microservice/pkg/amqp"
)

// AMQPHealthChecker adapta el cliente AMQP al contrato de salud.
type AMQPHealthChecker struct {
	client *amqp.Client
}

func NewAMQPHealthChecker(client *amqp.Client) *AMQPHealthChecker {
	return &AMQPHealthChecker{client: client}
}

func (c *AMQPHealthChecker) Ping(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("rabbitmq client is not configured")
	}
	return c.client.PingContext(ctx)
}
