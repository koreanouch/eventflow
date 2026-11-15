package event

import (
	"fmt"
	"time"
)

type OrderCreated struct {
	EventID     string    `json:"event_id"`
	EventType   string    `json:"event_type"`
	Timestamp   time.Time `json:"timestamp"`
	OrderID     string    `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	TotalAmount float64   `json:"total_amount"`
}

func NewOrderCreated(orderID, customerID string, totalAmount float64) *OrderCreated {
	return &OrderCreated{
		EventID:     fmt.Sprintf("order.created.%s", orderID),
		EventType:   "order.created",
		Timestamp:   time.Now().UTC(),
		OrderID:     orderID,
		CustomerID:  customerID,
		TotalAmount: totalAmount,
	}
}
