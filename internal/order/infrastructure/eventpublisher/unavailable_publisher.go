package eventpublisher

import (
	"context"
	"errors"

	"microservice/internal/order/domain/event"

	"go.uber.org/zap"
)

// ErrPublisherUnavailable indica que la API arrancó sin conexión a RabbitMQ.
var ErrPublisherUnavailable = errors.New("event publisher unavailable: RabbitMQ is not connected")

// UnavailablePublisher permite que la API opere en modo degradado cuando el
// broker no está disponible: los pedidos se persisten y el fallo de publicación
// queda registrado, sin bloquear el arranque ni los endpoints de consulta.
type UnavailablePublisher struct {
	log *zap.Logger
}

func NewUnavailablePublisher(log *zap.Logger) *UnavailablePublisher {
	if log == nil {
		log = zap.NewNop()
	}
	return &UnavailablePublisher{log: log}
}

func (p *UnavailablePublisher) PublishOrderCreated(ctx context.Context, evt *event.OrderCreated) error {
	p.log.Warn("event publication skipped: broker unavailable",
		zap.String("event_id", evt.EventID),
		zap.String("order_id", evt.OrderID),
		zap.String("request_id", evt.RequestID),
	)
	return ErrPublisherUnavailable
}
