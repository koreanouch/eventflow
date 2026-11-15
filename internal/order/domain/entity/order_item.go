package entity

import (
	"fmt"
	"math"
)

type OrderItem struct {
	productID string
	quantity  int64
	unitPrice int64
}

// NewOrderItem creates a new order item with validation
func NewOrderItem(productID string, quantity, unitPrice int64) (OrderItem, error) {
	item := OrderItem{
		productID: productID,
		quantity:  quantity,
		unitPrice: unitPrice,
	}
	if err := item.validate(); err != nil {
		return OrderItem{}, err
	}
	return item, nil
}

func (oi OrderItem) validate() error {
	if oi.productID == "" {
		return fmt.Errorf("product id cannot be empty")
	}
	if oi.quantity <= 0 {
		return fmt.Errorf("quantity must be greater than zero")
	}
	if oi.unitPrice < 0 {
		return fmt.Errorf("unit price cannot be negative")
	}
	if oi.unitPrice > 0 && oi.quantity > math.MaxInt64/oi.unitPrice {
		return fmt.Errorf("item subtotal exceeds int64 limit")
	}
	return nil
}

// ProductID returns the product ID
func (oi OrderItem) ProductID() string {
	return oi.productID
}

// Quantity returns the quantity
func (oi OrderItem) Quantity() int64 {
	return oi.quantity
}

// UnitPrice returns the unit price
func (oi OrderItem) UnitPrice() int64 {
	return oi.unitPrice
}

// subtotal calculates quantity * unitPrice (internal method)
func (oi OrderItem) subtotal() (int64, error) {
	if err := oi.validate(); err != nil {
		return 0, err
	}
	return oi.quantity * oi.unitPrice, nil
}
