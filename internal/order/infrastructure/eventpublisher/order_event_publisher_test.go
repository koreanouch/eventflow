package eventpublisher_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"microservice/cmd/config"
	"microservice/internal/order/domain/event"
	"microservice/internal/order/infrastructure/eventpublisher"
	"microservice/pkg/amqp"
	"microservice/pkg/logger"
)

func TestPublishOrderCreatedEvent_WithConfirm(t *testing.T) {
	// Setup logger
	logger.Init("test")
	log := logger.L()
	defer log.Sync()

	// Setup AMQP client (assumes RabbitMQ is running)
	client, err := amqp.NewClient("localhost", 5672, "guest", "guest", log)
	if err != nil {
		t.Skipf("Skipping integration test: could not connect to RabbitMQ: %v", err)
	}
	defer client.Close()

	// Enable publisher confirms
	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("Failed to enable publisher confirms: %v", err)
	}

	// Setup configuration
	cfg := &config.Configuration{
		RabbitMQExchange:     "orders.exchange",
		RabbitMQRoutingKey:   "order.created",
		RabbitMQMaxRetries:   3,
		RabbitMQRetryDelayMs: 5000,
	}

	// Declare exchange first
	if err := client.DeclareExchange("orders.exchange", "topic", true); err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	// Create publisher
	publisher := eventpublisher.NewOrderEventPublisher(client, cfg, log)

	// Create event
	evt := event.NewOrderCreated("test-order-001", "test-customer-001", 10000)

	// Publish event with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = publisher.PublishOrderCreated(ctx, evt)
	if err != nil {
		t.Fatalf("Failed to publish event: %v", err)
	}

	// Verify message was confirmed by RabbitMQ
	t.Logf("Event published and confirmed by RabbitMQ: %s", evt.EventID)
}

func TestPublishOrderCreatedEvent_NilEvent(t *testing.T) {
	// Setup logger
	logger.Init("test")
	log := logger.L()
	defer log.Sync()

	// Setup AMQP client
	client, err := amqp.NewClient("localhost", 5672, "guest", "guest", log)
	if err != nil {
		t.Skipf("Skipping integration test: could not connect to RabbitMQ: %v", err)
	}
	defer client.Close()

	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("Failed to enable publisher confirms: %v", err)
	}

	cfg := &config.Configuration{
		RabbitMQExchange:   "orders.exchange",
		RabbitMQRoutingKey: "order.created",
	}

	publisher := eventpublisher.NewOrderEventPublisher(client, cfg, log)

	// Try to publish nil event
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = publisher.PublishOrderCreated(ctx, nil)
	if err == nil {
		t.Fatalf("Expected error when publishing nil event")
	}

	if err.Error() != "event cannot be nil" {
		t.Fatalf("Expected 'event cannot be nil', got %v", err)
	}
}

func TestPublishOrderCreatedEvent_ContextTimeout(t *testing.T) {
	// Setup logger
	logger.Init("test")
	log := logger.L()
	defer log.Sync()

	// Setup AMQP client
	client, err := amqp.NewClient("localhost", 5672, "guest", "guest", log)
	if err != nil {
		t.Skipf("Skipping integration test: could not connect to RabbitMQ: %v", err)
	}
	defer client.Close()

	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("Failed to enable publisher confirms: %v", err)
	}

	cfg := &config.Configuration{
		RabbitMQExchange:   "orders.exchange",
		RabbitMQRoutingKey: "order.created",
	}

	if err := client.DeclareExchange("orders.exchange", "topic", true); err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	publisher := eventpublisher.NewOrderEventPublisher(client, cfg, log)
	evt := event.NewOrderCreated("test-order-timeout", "test-customer-001", 10000)

	// Use very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	err = publisher.PublishOrderCreated(ctx, evt)
	if err == nil {
		t.Fatalf("Expected timeout error")
	}

	if err.Error() != "publisher confirmation timeout: context deadline exceeded" {
		t.Logf("Got error: %v", err)
	}
}

func TestOrderCreatedEventJSON(t *testing.T) {
	// Create event
	evt := event.NewOrderCreated("test-order-123", "customer-456", 25075)

	// Marshal to JSON
	body, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	// Unmarshal back
	var evt2 event.OrderCreated
	if err := json.Unmarshal(body, &evt2); err != nil {
		t.Fatalf("Failed to unmarshal event: %v", err)
	}

	// Verify fields
	if evt2.OrderID != "test-order-123" {
		t.Fatalf("Expected order_id 'test-order-123', got %s", evt2.OrderID)
	}

	if evt2.CustomerID != "customer-456" {
		t.Fatalf("Expected customer_id 'customer-456', got %s", evt2.CustomerID)
	}

	if evt2.TotalAmount != 25075 {
		t.Fatalf("Expected total_amount 25075, got %d", evt2.TotalAmount)
	}

	// Verify event ID is stable
	if evt.EventID != evt2.EventID {
		t.Fatalf("Event ID should be stable: %s vs %s", evt.EventID, evt2.EventID)
	}
}
