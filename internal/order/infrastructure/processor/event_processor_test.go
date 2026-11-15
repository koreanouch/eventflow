package processor_test

import (
	"context"
	"testing"
	"time"

	"microservice/internal/order/domain/event"
	"microservice/internal/order/infrastructure/processor"
	"microservice/pkg/database"
	"microservice/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

var testLogger *zap.Logger

func setupLogger() {
	if testLogger == nil {
		logger.Init("test")
		testLogger = logger.L()
	}
}

func getTestLogger() *zap.Logger {
	setupLogger()
	return testLogger
}

func TestProcessOrderCreatedEvent_Success(t *testing.T) {
	// Setup logger
	setupLogger()

	// Setup database
	db := setupTestDatabase(t)
	defer db.Close()

	log := getTestLogger()
	proc := processor.NewEventProcessor(db, log)

	// Create a test event
	evt := event.NewOrderCreated("order-123", "customer-456", 100.50)

	// Process the event
	err := proc.ProcessOrderCreated(context.Background(), evt, "test-worker")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify event was recorded
	pool := db.Native().(*pgxpool.Pool)
	var count int
	err = pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM processed_events WHERE event_id = $1", evt.EventID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query processed events: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 processed event, got %d", count)
	}

	// Verify notification was created
	err = pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM notifications WHERE event_id = $1", evt.EventID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query notifications: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 notification, got %d", count)
	}
}

func TestProcessOrderCreatedEvent_Idempotency(t *testing.T) {
	// Setup logger
	setupLogger()

	// Setup database
	db := setupTestDatabase(t)
	defer db.Close()

	log := getTestLogger()
	proc := processor.NewEventProcessor(db, log)

	// Create a test event
	evt := event.NewOrderCreated("order-789", "customer-999", 200.75)

	// Process the event twice
	err := proc.ProcessOrderCreated(context.Background(), evt, "test-worker")
	if err != nil {
		t.Fatalf("First processing failed: %v", err)
	}

	err = proc.ProcessOrderCreated(context.Background(), evt, "test-worker")
	if err != nil {
		t.Fatalf("Second processing failed: %v", err)
	}

	// Verify only one event was recorded
	pool := db.Native().(*pgxpool.Pool)
	var count int
	err = pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM processed_events WHERE event_id = $1 AND consumer_name = 'test-worker'", evt.EventID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query processed events: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 processed event after idempotent call, got %d", count)
	}

	// Verify only one notification was created
	err = pool.QueryRow(context.Background(),
		"SELECT COUNT(*) FROM notifications WHERE event_id = $1", evt.EventID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query notifications: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1 notification after idempotent call, got %d", count)
	}
}

func TestParseOrderCreatedEvent(t *testing.T) {
	// Setup logger
	setupLogger()

	db := setupTestDatabase(t)
	defer db.Close()

	log := getTestLogger()
	proc := processor.NewEventProcessor(db, log)

	// JSON body
	body := []byte(`{
		"event_id": "order.created.test-order-001",
		"event_type": "order.created",
		"timestamp": "2024-01-15T10:30:00Z",
		"order_id": "test-order-001",
		"customer_id": "test-customer-001",
		"total_amount": 150.50
	}`)

	evt, err := proc.ParseOrderCreatedEvent(body)
	if err != nil {
		t.Fatalf("Failed to parse event: %v", err)
	}

	if evt.OrderID != "test-order-001" {
		t.Fatalf("Expected order_id 'test-order-001', got %s", evt.OrderID)
	}

	if evt.CustomerID != "test-customer-001" {
		t.Fatalf("Expected customer_id 'test-customer-001', got %s", evt.CustomerID)
	}

	if evt.TotalAmount != 150.50 {
		t.Fatalf("Expected total_amount 150.50, got %f", evt.TotalAmount)
	}
}

func setupTestDatabase(t *testing.T) database.Database {
	// Connect to test database (assumes POSTGRES_* env vars are set for test)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	db, err := database.NewPostgresDatabase(ctx, database.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "postgres",
		DBName:   "microservice",
		SSLMode:  "disable",
	})

	if err != nil {
		t.Skipf("Skipping integration test: could not connect to database: %v", err)
	}

	return db
}
