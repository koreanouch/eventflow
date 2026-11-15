package entity

import (
	"fmt"
	"math"
	"time"
)

type Order struct {
	id          string
	customerID  string
	items       []OrderItem
	totalAmount int64
	status      OrderStatus
	createdAt   time.Time
}

// NewOrder creates a new order with validation and calculates total
func NewOrder(id string, customerID string, items []OrderItem) (*Order, error) {
	// Validate id
	if id == "" {
		return nil, fmt.Errorf("order id cannot be empty")
	}

	// Validate customerID
	if customerID == "" {
		return nil, fmt.Errorf("customer id cannot be empty")
	}

	// Validate items
	if len(items) == 0 {
		return nil, fmt.Errorf("order must have at least one item")
	}

	// Calculate total with overflow check
	var total int64 = 0
	for _, item := range items {
		subtotal, err := item.subtotal()
		if err != nil {
			return nil, fmt.Errorf("invalid item: %w", err)
		}

		// Check for overflow in sum
		if subtotal > math.MaxInt64-total {
			return nil, fmt.Errorf("order total exceeds int64 limit")
		}

		total += subtotal
	}

	// Defensive copy of items to prevent external modification
	itemsCopy := make([]OrderItem, len(items))
	copy(itemsCopy, items)

	return &Order{
		id:          id,
		customerID:  customerID,
		items:       itemsCopy,
		totalAmount: total,
		status:      StatusPending,
		createdAt:   time.Now().UTC(),
	}, nil
}

// RestoreOrder reconstructs an order with its persisted status and creation time.
func RestoreOrder(id, customerID string, items []OrderItem, status OrderStatus, createdAt time.Time) (*Order, error) {
	if status == "" || createdAt.IsZero() {
		return nil, fmt.Errorf("persisted order status and creation time are required")
	}
	order, err := NewOrder(id, customerID, items)
	if err != nil {
		return nil, err
	}
	order.status = status
	order.createdAt = createdAt
	return order, nil
}

// ID returns the order ID
func (o *Order) ID() string {
	return o.id
}

// CustomerID returns the customer ID
func (o *Order) CustomerID() string {
	return o.customerID
}

// Items returns a copy of the order items
func (o *Order) Items() []OrderItem {
	itemsCopy := make([]OrderItem, len(o.items))
	copy(itemsCopy, o.items)
	return itemsCopy
}

// TotalAmount returns the total amount
func (o *Order) TotalAmount() int64 {
	return o.totalAmount
}

// Status returns the order status
func (o *Order) Status() OrderStatus {
	return o.status
}

// CreatedAt returns the creation timestamp
func (o *Order) CreatedAt() time.Time {
	return o.createdAt
}
