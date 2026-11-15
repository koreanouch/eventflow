package dto

import "time"

type GetOrderResponse struct {
	ID          string            `json:"id" example:"550e8400-e29b-41d4-a716-446655440000"`
	CustomerID  string            `json:"customer_id" example:"cust-123"`
	Items       []GetOrderItemDTO `json:"items"`
	TotalAmount int64             `json:"total_amount" example:"5000"`
	Status      string            `json:"status" example:"pending"`
	CreatedAt   time.Time         `json:"created_at" example:"2024-01-15T10:30:00Z"`
}

type GetOrderItemDTO struct {
	ProductID string `json:"product_id" example:"prod-456"`
	Quantity  int64  `json:"quantity" example:"2"`
	UnitPrice int64  `json:"unit_price" example:"2500"`
}

func NewGetOrderResponse(
	id string,
	customerID string,
	items []GetOrderItemDTO,
	totalAmount int64,
	status string,
	createdAt time.Time,
) *GetOrderResponse {
	return &GetOrderResponse{
		ID:          id,
		CustomerID:  customerID,
		Items:       items,
		TotalAmount: totalAmount,
		Status:      status,
		CreatedAt:   createdAt,
	}
}
