package port

import (
	"context"

	"microservice/internal/order/domain/event"
)

// OrderCreatedPublisher publica el evento de dominio OrderCreated.
// La aplicación depende de este contrato, no del cliente AMQP concreto.
type OrderCreatedPublisher interface {
	PublishOrderCreated(ctx context.Context, evt *event.OrderCreated) error
}
