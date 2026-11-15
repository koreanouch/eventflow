package entity

import (
	"math"
	"testing"
)

func TestNewOrderItem_Valid(t *testing.T) {
	item, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	if item.ProductID() != "PROD001" {
		t.Errorf("ProductID() = %q, want %q", item.ProductID(), "PROD001")
	}
	if item.Quantity() != 5 {
		t.Errorf("Quantity() = %d, want %d", item.Quantity(), 5)
	}
	if item.UnitPrice() != 1000 {
		t.Errorf("UnitPrice() = %d, want %d", item.UnitPrice(), 1000)
	}
}

func TestNewOrderItem_EmptyProductID(t *testing.T) {
	_, err := NewOrderItem("", 5, 1000)
	if err == nil {
		t.Fatal("NewOrderItem() expected error for empty productID")
	}
}

func TestNewOrderItem_ZeroQuantity(t *testing.T) {
	_, err := NewOrderItem("PROD001", 0, 1000)
	if err == nil {
		t.Fatal("NewOrderItem() expected error for zero quantity")
	}
}

func TestNewOrderItem_NegativeQuantity(t *testing.T) {
	_, err := NewOrderItem("PROD001", -5, 1000)
	if err == nil {
		t.Fatal("NewOrderItem() expected error for negative quantity")
	}
}

func TestNewOrderItem_NegativeUnitPrice(t *testing.T) {
	_, err := NewOrderItem("PROD001", 5, -1000)
	if err == nil {
		t.Fatal("NewOrderItem() expected error for negative unit price")
	}
}

func TestNewOrderItem_OverflowInSubtotal(t *testing.T) {
	// Try to cause overflow: quantity > MaxInt64 / unitPrice
	_, err := NewOrderItem("PROD001", math.MaxInt64, 2)
	if err == nil {
		t.Fatal("NewOrderItem() expected error for overflow")
	}
}

func TestNewOrderItem_ZeroUnitPrice(t *testing.T) {
	// UnitPrice = 0 should be valid (free item)
	item, err := NewOrderItem("PROD001", 100, 0)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error for zero price: %v", err)
	}
	if item.UnitPrice() != 0 {
		t.Errorf("UnitPrice() = %d, want %d", item.UnitPrice(), 0)
	}
}

func TestOrderItem_Subtotal(t *testing.T) {
	item, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	subtotal, err := item.subtotal()
	if err != nil {
		t.Fatalf("subtotal() returned unexpected error: %v", err)
	}
	if subtotal != 5000 {
		t.Errorf("subtotal() = %d, want %d", subtotal, 5000)
	}
}

func TestOrderItem_SubtotalInvalid(t *testing.T) {
	tests := []struct {
		name string
		item OrderItem
		want string
	}{
		{"zero value", OrderItem{}, "product id cannot be empty"},
		{"empty product ID", OrderItem{quantity: 1, unitPrice: 1}, "product id cannot be empty"},
		{"zero quantity", OrderItem{productID: "PROD001", unitPrice: 1}, "quantity must be greater than zero"},
		{"negative quantity", OrderItem{productID: "PROD001", quantity: -1, unitPrice: 1}, "quantity must be greater than zero"},
		{"negative unit price", OrderItem{productID: "PROD001", quantity: 1, unitPrice: -1}, "unit price cannot be negative"},
		{"subtotal overflow", OrderItem{productID: "PROD001", quantity: math.MaxInt64, unitPrice: 2}, "item subtotal exceeds int64 limit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.item.subtotal()
			if err == nil || err.Error() != tt.want {
				t.Fatalf("subtotal() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestNewOrderItem_MaxInt64Subtotal(t *testing.T) {
	// Create an item where quantity * unitPrice == MaxInt64
	// For simplicity, use quantity = MaxInt64 and unitPrice = 1
	// This should be valid and not cause overflow
	item, err := NewOrderItem("PROD001", math.MaxInt64, 1)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error for MaxInt64 subtotal: %v", err)
	}

	subtotal, err := item.subtotal()
	if err != nil {
		t.Fatalf("subtotal() returned unexpected error for MaxInt64: %v", err)
	}

	if subtotal != math.MaxInt64 {
		t.Errorf("subtotal() = %d, want %d", subtotal, math.MaxInt64)
	}
}
