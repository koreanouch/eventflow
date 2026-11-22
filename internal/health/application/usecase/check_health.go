package usecase

import (
	"context"
	"fmt"
	"microservice/internal/health/application/dto"
	domain "microservice/internal/health/domain/repository"
)

type HealthUseCase struct {
	repo   domain.HealthRepository
	broker domain.BrokerHealthChecker
}

// Constructor del Caso de Uso
func NewHealthUseCase(repo domain.HealthRepository, broker domain.BrokerHealthChecker) *HealthUseCase {
	return &HealthUseCase{repo: repo, broker: broker}
}

// Execute diferencia entre no estar listo (base de datos caída → 503) y operar
// en modo degradado (broker caído → 200 con status "degraded"). La API puede
// persistir y consultar pedidos sin RabbitMQ.
func (uc *HealthUseCase) Execute(ctx context.Context) (*dto.HealthResponse, error) {
	dbStatus := "connected"
	dbErr := uc.repo.Ping(ctx)
	if dbErr != nil {
		dbStatus = "disconnected"
	}

	brokerStatus := "disconnected"
	if uc.broker != nil && uc.broker.Ping(ctx) == nil {
		brokerStatus = "connected"
	}

	status := "ok"
	switch {
	case dbErr != nil:
		status = "unhealthy"
	case brokerStatus != "connected":
		status = "degraded"
	}

	res := dto.NewHealthResponse(status, dbStatus, brokerStatus)
	if dbErr != nil {
		return res, fmt.Errorf("database unavailable: %w", dbErr)
	}

	return res, nil
}
