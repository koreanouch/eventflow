package dto

import "time"

type CreateOrderResponse struct {
	ID        string    `json:"id" example:"550e8400-e29b-41d4-a716-446655440000"`
	CreatedAt time.Time `json:"created_at" example:"2024-01-15T10:30:00Z"`
}

func NewCreateOrderResponse(id string, createdAt time.Time) *CreateOrderResponse {
	return &CreateOrderResponse{
		ID:        id,
		CreatedAt: createdAt,
	}
}
