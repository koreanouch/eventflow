package domain

import "context"

type HealthRepository interface {
	Ping(ctx context.Context) error
}

// BrokerHealthChecker comprueba una dependencia opcional (RabbitMQ).
type BrokerHealthChecker interface {
	Ping(ctx context.Context) error
}
