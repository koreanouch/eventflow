package dto

import "time"

type HealthResponse struct {
	Timestamp time.Time `json:"timestamp" example:"2024-01-15T10:30:00Z"`
	Service   string    `json:"service" example:"microservice"`
	Status    string    `json:"status" example:"ok"`
	Database  string    `json:"database" example:"connected"`
	RabbitMQ  string    `json:"rabbitmq" example:"connected"`
}

// NewHealthResponse construye la respuesta con el estado de las dependencias.
func NewHealthResponse(status, dbStatus, brokerStatus string) *HealthResponse {
	return &HealthResponse{
		Timestamp: time.Now().UTC(),
		Service:   "microservice",
		Status:    status,
		Database:  dbStatus,
		RabbitMQ:  brokerStatus,
	}
}
