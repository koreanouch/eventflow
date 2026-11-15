package entity

import (
	"math"
	"testing"
	"time"
)

func TestNewOrder_Valid(t *testing.T) {
	items := []OrderItem{}
	item, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items = append(items, item)

	order, err := NewOrder("ORDER001", "CUST001", items)
	if err != nil {
		t.Fatalf("NewOrder() returned unexpected error: %v", err)
	}
	if order.ID() != "ORDER001" {
		t.Errorf("ID() = %q, want %q", order.ID(), "ORDER001")
	}
	if order.CustomerID() != "CUST001" {
		t.Errorf("CustomerID() = %q, want %q", order.CustomerID(), "CUST001")
	}
	if order.TotalAmount() != 5000 {
		t.Errorf("TotalAmount() = %d, want %d", order.TotalAmount(), 5000)
	}
	if order.Status() != StatusPending {
		t.Errorf("Status() = %v, want %v", order.Status(), StatusPending)
	}
}

func TestNewOrder_EmptyID(t *testing.T) {
	items := []OrderItem{}
	item, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items = append(items, item)

	_, err = NewOrder("", "CUST001", items)
	if err == nil {
		t.Fatal("NewOrder() expected error for empty id")
	}
}

func TestNewOrder_EmptyCustomerID(t *testing.T) {
	items := []OrderItem{}
	item, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items = append(items, item)

	_, err = NewOrder("ORDER001", "", items)
	if err == nil {
		t.Fatal("NewOrder() expected error for empty customerID")
	}
}

func TestNewOrder_NoItems(t *testing.T) {
	_, err := NewOrder("ORDER001", "CUST001", []OrderItem{})
	if err == nil {
		t.Fatal("NewOrder() expected error for empty items")
	}
}

func TestNewOrder_NilItems(t *testing.T) {
	_, err := NewOrder("ORDER001", "CUST001", nil)
	if err == nil {
		t.Fatal("NewOrder() expected error for nil items")
	}
}

func TestNewOrder_InvalidItem(t *testing.T) {
	tests := []struct {
		name string
		item OrderItem
		want string
	}{
		{"zero value", OrderItem{}, "invalid item: product id cannot be empty"},
		{"empty product ID", OrderItem{quantity: 1, unitPrice: 1}, "invalid item: product id cannot be empty"},
		{"zero quantity", OrderItem{productID: "PROD001", unitPrice: 1}, "invalid item: quantity must be greater than zero"},
		{"negative quantity", OrderItem{productID: "PROD001", quantity: -1, unitPrice: 1}, "invalid item: quantity must be greater than zero"},
		{"negative unit price", OrderItem{productID: "PROD001", quantity: 1, unitPrice: -1}, "invalid item: unit price cannot be negative"},
		{"subtotal overflow", OrderItem{productID: "PROD001", quantity: math.MaxInt64, unitPrice: 2}, "invalid item: item subtotal exceeds int64 limit"},
	}
	validItem, err := NewOrderItem("PROD002", 2, 100)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order, err := NewOrder("ORDER001", "CUST001", []OrderItem{validItem, tt.item})
			if err == nil || err.Error() != tt.want {
				t.Fatalf("NewOrder() error = %v, want %q", err, tt.want)
			}
			if order != nil {
				t.Fatalf("NewOrder() returned an order for an invalid item: %+v", order)
			}
		})
	}
}

func TestNewOrder_MultipleItems(t *testing.T) {
	items := []OrderItem{}
	item1, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	item2, err := NewOrderItem("PROD002", 3, 2000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items = append(items, item1, item2)

	order, err := NewOrder("ORDER001", "CUST001", items)
	if err != nil {
		t.Fatalf("NewOrder() returned unexpected error: %v", err)
	}
	// Total should be (5 * 1000) + (3 * 2000) = 5000 + 6000 = 11000
	if order.TotalAmount() != 11000 {
		t.Errorf("TotalAmount() = %d, want %d", order.TotalAmount(), 11000)
	}
	if len(order.Items()) != 2 {
		t.Errorf("len(Items()) = %d, want %d", len(order.Items()), 2)
	}
}

func TestNewOrder_OverflowInTotal(t *testing.T) {
	items := []OrderItem{}
	// Create two items that will overflow when summed
	item1, err := NewOrderItem("PROD001", 1, math.MaxInt64-100)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	item2, err := NewOrderItem("PROD002", 1, 200)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items = append(items, item1, item2)

	_, err = NewOrder("ORDER001", "CUST001", items)
	if err == nil {
		t.Fatal("NewOrder() expected error for overflow in total")
	}
}

func TestOrder_ItemsCopy(t *testing.T) {
	items := []OrderItem{}
	item, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items = append(items, item)

	order, err := NewOrder("ORDER001", "CUST001", items)
	if err != nil {
		t.Fatalf("NewOrder() returned unexpected error: %v", err)
	}

	// Obtain items via order.Items()
	retrievedItems := order.Items()
	if len(retrievedItems) != 1 {
		t.Fatalf("len(Items()) = %d, want %d", len(retrievedItems), 1)
	}

	originalProductID := retrievedItems[0].ProductID()
	originalTotal := order.TotalAmount()

	// Modify a position in the retrieved slice
	modifiedItem, err := NewOrderItem("MODIFIED_PROD", 999, 9999)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	retrievedItems[0] = modifiedItem

	// Verify that the order conserves the original ProductID
	orderItemsAfter := order.Items()
	if orderItemsAfter[0].ProductID() != originalProductID {
		t.Errorf("order item ProductID = %q, want %q (should not be modified)", orderItemsAfter[0].ProductID(), originalProductID)
	}

	// Verify that TotalAmount is unchanged
	if order.TotalAmount() != originalTotal {
		t.Errorf("TotalAmount = %d, want %d (should not be modified)", order.TotalAmount(), originalTotal)
	}
}

func TestOrder_CreatedAt(t *testing.T) {
	items := []OrderItem{}
	item, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items = append(items, item)

	before := time.Now().UTC()
	order, err := NewOrder("ORDER001", "CUST001", items)
	if err != nil {
		t.Fatalf("NewOrder() returned unexpected error: %v", err)
	}
	after := time.Now().UTC()

	if order.CreatedAt().Before(before) || order.CreatedAt().After(after) {
		t.Errorf("CreatedAt() = %v, should be between %v and %v", order.CreatedAt(), before, after)
	}
}

func TestOrder_StatusPending(t *testing.T) {
	items := []OrderItem{}
	item, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items = append(items, item)

	order, err := NewOrder("ORDER001", "CUST001", items)
	if err != nil {
		t.Fatalf("NewOrder() returned unexpected error: %v", err)
	}
	if order.Status() != StatusPending {
		t.Errorf("Status() = %v, want %v", order.Status(), StatusPending)
	}
}

func TestNewOrder_DefensiveCopyOfItems(t *testing.T) {
	// Create two valid items
	item1, err := NewOrderItem("PROD001", 5, 1000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	item2, err := NewOrderItem("PROD002", 3, 2000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items := []OrderItem{item1, item2}

	// Create order with these items
	order, err := NewOrder("ORDER001", "CUST001", items)
	if err != nil {
		t.Fatalf("NewOrder() returned unexpected error: %v", err)
	}

	// Verify initial state
	retrievedItems := order.Items()
	if len(retrievedItems) != 2 {
		t.Fatalf("expected 2 items, got %d", len(retrievedItems))
	}
	if retrievedItems[0].ProductID() != "PROD001" {
		t.Errorf("first item ProductID = %q, want %q", retrievedItems[0].ProductID(), "PROD001")
	}
	if retrievedItems[1].ProductID() != "PROD002" {
		t.Errorf("second item ProductID = %q, want %q", retrievedItems[1].ProductID(), "PROD002")
	}

	// Modify the original slice after order creation
	modifiedItem, err := NewOrderItem("PROD999", 100, 5000)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items[0] = modifiedItem
	items[1] = modifiedItem

	// Verify that the order's internal items were NOT modified
	retrievedItemsAfter := order.Items()
	if len(retrievedItemsAfter) != 2 {
		t.Fatalf("expected 2 items after external modification, got %d", len(retrievedItemsAfter))
	}
	if retrievedItemsAfter[0].ProductID() != "PROD001" {
		t.Errorf("first item ProductID after external mod = %q, want %q", retrievedItemsAfter[0].ProductID(), "PROD001")
	}
	if retrievedItemsAfter[1].ProductID() != "PROD002" {
		t.Errorf("second item ProductID after external mod = %q, want %q", retrievedItemsAfter[1].ProductID(), "PROD002")
	}

	// Verify that the order total was also not affected
	expectedTotal := (5 * 1000) + (3 * 2000) // 5000 + 6000 = 11000
	if order.TotalAmount() != int64(expectedTotal) {
		t.Errorf("TotalAmount after external mod = %d, want %d", order.TotalAmount(), expectedTotal)
	}
}

func TestNewOrder_TotalExactlyMaxInt64(t *testing.T) {
	// Create an order whose total is exactly MaxInt64
	// Use a single item with quantity=1 and unitPrice=MaxInt64
	item, err := NewOrderItem("PROD001", 1, math.MaxInt64)
	if err != nil {
		t.Fatalf("NewOrderItem() returned unexpected error: %v", err)
	}
	items := []OrderItem{item}

	order, err := NewOrder("ORDER001", "CUST001", items)
	if err != nil {
		t.Fatalf("NewOrder() returned unexpected error for MaxInt64 total: %v", err)
	}

	if order.TotalAmount() != math.MaxInt64 {
		t.Errorf("TotalAmount() = %d, want %d", order.TotalAmount(), math.MaxInt64)
	}
}
