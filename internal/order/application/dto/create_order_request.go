package dto

type CreateOrderRequest struct {
	CustomerID string         `json:"customer_id" binding:"required"`
	Items      []OrderItemDTO `json:"items" binding:"required,min=1"`
}

type OrderItemDTO struct {
	ProductID string `json:"product_id" binding:"required"`
	Quantity  int64  `json:"quantity" binding:"required,gt=0"`
	UnitPrice int64  `json:"unit_price" binding:"required,gte=0"`
}
