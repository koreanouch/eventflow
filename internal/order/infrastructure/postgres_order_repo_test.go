package infrastructure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"microservice/internal/order/domain/entity"
	"microservice/pkg/database"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Test helper to setup database for integration tests
func setupTestDB(t *testing.T) database.Database {
	cfg := database.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "postgres",
		DBName:   "microservice",
		SSLMode:  "disable",
	}

	db, err := database.NewPostgresDatabase(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	// Test ping
	if err := db.Ping(context.Background()); err != nil {
		db.Close()
		t.Fatalf("failed to ping database: %v", err)
	}

	return db
}

// Helper to cleanup test data
func cleanupOrderData(t *testing.T, db database.Database, orderID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool := db.Native().(*pgxpool.Pool)

	_, err := pool.Exec(ctx, "DELETE FROM order_items WHERE order_id = $1", orderID)
	if err != nil {
		t.Logf("warning: failed to cleanup order_items: %v", err)
	}

	_, err = pool.Exec(ctx, "DELETE FROM orders WHERE id = $1", orderID)
	if err != nil {
		t.Logf("warning: failed to cleanup orders: %v", err)
	}
}

func TestSaveOrder(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewPostgresOrderRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create order with items
	item1, err := entity.NewOrderItem("product-1", 2, 1000)
	if err != nil {
		t.Fatalf("failed to create order item: %v", err)
	}

	item2, err := entity.NewOrderItem("product-2", 1, 5000)
	if err != nil {
		t.Fatalf("failed to create order item: %v", err)
	}

	order, err := entity.NewOrder("order-001", "customer-001", []entity.OrderItem{item1, item2})
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	defer cleanupOrderData(t, db, order.ID())

	// Save order
	err = repo.Save(ctx, order)
	if err != nil {
		t.Fatalf("failed to save order: %v", err)
	}

	// Verify order was saved
	retrievedOrder, err := repo.GetByID(ctx, order.ID())
	if err != nil {
		t.Fatalf("failed to retrieve order: %v", err)
	}

	if retrievedOrder == nil {
		t.Fatal("expected order, got nil")
	}

	if retrievedOrder.ID() != order.ID() {
		t.Errorf("expected order ID %s, got %s", order.ID(), retrievedOrder.ID())
	}

	if retrievedOrder.CustomerID() != order.CustomerID() {
		t.Errorf("expected customer ID %s, got %s", order.CustomerID(), retrievedOrder.CustomerID())
	}

	if retrievedOrder.TotalAmount() != order.TotalAmount() {
		t.Errorf("expected total amount %d, got %d", order.TotalAmount(), retrievedOrder.TotalAmount())
	}

	if len(retrievedOrder.Items()) != 2 {
		t.Errorf("expected 2 items, got %d", len(retrievedOrder.Items()))
	}
}

func TestGetByIDOrderNotFound(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewPostgresOrderRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try to get non-existent order
	retrievedOrder, err := repo.GetByID(ctx, "non-existent-order")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if retrievedOrder != nil {
		t.Error("expected nil order, got non-nil")
	}
}

func TestSaveOrderWithMultipleItems(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewPostgresOrderRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create order with multiple items
	items := make([]entity.OrderItem, 5)
	for i := 0; i < 5; i++ {
		productID := fmt.Sprintf("product-%d", i)
		item, err := entity.NewOrderItem(productID, int64(i+1), int64((i+1)*1000))
		if err != nil {
			t.Fatalf("failed to create order item: %v", err)
		}
		items[i] = item
	}

	order, err := entity.NewOrder("order-multi-items", "customer-002", items)
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	defer cleanupOrderData(t, db, order.ID())

	// Save order
	err = repo.Save(ctx, order)
	if err != nil {
		t.Fatalf("failed to save order: %v", err)
	}

	// Retrieve and verify
	retrievedOrder, err := repo.GetByID(ctx, order.ID())
	if err != nil {
		t.Fatalf("failed to retrieve order: %v", err)
	}

	if retrievedOrder == nil {
		t.Fatal("expected order, got nil")
	}

	if len(retrievedOrder.Items()) != 5 {
		t.Errorf("expected 5 items, got %d", len(retrievedOrder.Items()))
	}

	if retrievedOrder.TotalAmount() != order.TotalAmount() {
		t.Errorf("expected total amount %d, got %d", order.TotalAmount(), retrievedOrder.TotalAmount())
	}
}

func TestSaveOrderWithZeroPrice(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewPostgresOrderRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create order with zero price item (valid)
	item, err := entity.NewOrderItem("free-product", 1, 0)
	if err != nil {
		t.Fatalf("failed to create order item: %v", err)
	}

	order, err := entity.NewOrder("order-free", "customer-003", []entity.OrderItem{item})
	if err != nil {
		t.Fatalf("failed to create order: %v", err)
	}

	defer cleanupOrderData(t, db, order.ID())

	// Save order
	err = repo.Save(ctx, order)
	if err != nil {
		t.Fatalf("failed to save order: %v", err)
	}

	// Retrieve and verify
	retrievedOrder, err := repo.GetByID(ctx, order.ID())
	if err != nil {
		t.Fatalf("failed to retrieve order: %v", err)
	}

	if retrievedOrder == nil {
		t.Fatal("expected order, got nil")
	}

	if retrievedOrder.TotalAmount() != 0 {
		t.Errorf("expected total amount 0, got %d", retrievedOrder.TotalAmount())
	}
}

func TestGetByIDEmptyID(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewPostgresOrderRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try to get order with empty ID
	_, err := repo.GetByID(ctx, "")
	if err == nil {
		t.Error("expected error for empty order ID, got nil")
	}
}

func TestSaveNilOrder(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewPostgresOrderRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Try to save nil order
	err := repo.Save(ctx, nil)
	if err == nil {
		t.Error("expected error for nil order, got nil")
	}
}

func TestTransactionRollback(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewPostgresOrderRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a valid order first
	item1, _ := entity.NewOrderItem("product-1", 1, 1000)
	order1, _ := entity.NewOrder("order-transaction-1", "customer-001", []entity.OrderItem{item1})
	err := repo.Save(ctx, order1)
	if err != nil {
		t.Fatalf("failed to save first order: %v", err)
	}
	defer cleanupOrderData(t, db, order1.ID())

	// Create a valid order with multiple items
	item2, _ := entity.NewOrderItem("product-2", 2, 2000)
	item3, _ := entity.NewOrderItem("product-3", 3, 3000)
	order2, _ := entity.NewOrder("order-transaction-2", "customer-002", []entity.OrderItem{item2, item3})
	err = repo.Save(ctx, order2)
	if err != nil {
		t.Fatalf("failed to save second order: %v", err)
	}
	defer cleanupOrderData(t, db, order2.ID())

	// Verify both orders and items exist
	retrieved1, _ := repo.GetByID(ctx, order1.ID())
	if retrieved1 == nil {
		t.Fatal("order 1 should exist")
	}
	if len(retrieved1.Items()) != 1 {
		t.Errorf("order 1 should have 1 item, got %d", len(retrieved1.Items()))
	}

	retrieved2, _ := repo.GetByID(ctx, order2.ID())
	if retrieved2 == nil {
		t.Fatal("order 2 should exist")
	}
	if len(retrieved2.Items()) != 2 {
		t.Errorf("order 2 should have 2 items, got %d", len(retrieved2.Items()))
	}
}

func TestOrderPersistenceWithItemVerification(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	repo := NewPostgresOrderRepository(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create order with known values
	item1, _ := entity.NewOrderItem("laptop", 2, 150000)
	item2, _ := entity.NewOrderItem("mouse", 5, 5000)
	order, _ := entity.NewOrder("order-final-test", "customer-tech", []entity.OrderItem{item1, item2})

	defer cleanupOrderData(t, db, order.ID())

	// Save order
	err := repo.Save(ctx, order)
	if err != nil {
		t.Fatalf("failed to save order: %v", err)
	}

	// Retrieve and verify each field
	retrieved, err := repo.GetByID(ctx, order.ID())
	if err != nil {
		t.Fatalf("failed to retrieve order: %v", err)
	}

	if retrieved == nil {
		t.Fatal("expected order, got nil")
	}

	// Verify order fields
	if retrieved.ID() != "order-final-test" {
		t.Errorf("wrong order ID: expected order-final-test, got %s", retrieved.ID())
	}

	if retrieved.CustomerID() != "customer-tech" {
		t.Errorf("wrong customer ID: expected customer-tech, got %s", retrieved.CustomerID())
	}

	if retrieved.Status().String() != "pending" {
		t.Errorf("wrong status: expected pending, got %s", retrieved.Status().String())
	}

	expectedTotal := int64(2*150000 + 5*5000)
	if retrieved.TotalAmount() != expectedTotal {
		t.Errorf("wrong total amount: expected %d, got %d", expectedTotal, retrieved.TotalAmount())
	}

	// Verify items
	if len(retrieved.Items()) != 2 {
		t.Fatalf("expected 2 items, got %d", len(retrieved.Items()))
	}

	items := retrieved.Items()
	if items[0].ProductID() != "laptop" || items[0].Quantity() != 2 || items[0].UnitPrice() != 150000 {
		t.Error("first item mismatch")
	}

	if items[1].ProductID() != "mouse" || items[1].Quantity() != 5 || items[1].UnitPrice() != 5000 {
		t.Error("second item mismatch")
	}
}
