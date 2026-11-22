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
	TotalAmount int64     `json:"total_amount"`
	RequestID   string    `json:"request_id,omitempty"`
}

func NewOrderCreated(orderID, customerID string, totalAmount int64) *OrderCreated {
	return &OrderCreated{
		EventID:     fmt.Sprintf("order.created.%s", orderID),
		EventType:   "order.created",
		Timestamp:   time.Now().UTC(),
		OrderID:     orderID,
		CustomerID:  customerID,
		TotalAmount: totalAmount,
	}
}
