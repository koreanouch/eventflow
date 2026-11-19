package dto

import "time"

type CreateOrderResponse struct {
	ID        string    `json:"id" example:"550e8400-e29b-41d4-a716-446655440000"`
	Status    string    `json:"status" example:"pending"`
	CreatedAt time.Time `json:"created_at" example:"2024-01-15T10:30:00Z"`
}

func NewCreateOrderResponse(id, status string, createdAt time.Time) *CreateOrderResponse {
	return &CreateOrderResponse{
		ID:        id,
		Status:    status,
		CreatedAt: createdAt,
	}
}
