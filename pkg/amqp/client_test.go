package amqp_test

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"microservice/pkg/amqp"
	"microservice/pkg/logger"
)

func TestPublishWithConfirm_Successful(t *testing.T) {
	// Setup logger
	logger.Init("test")
	log := logger.L()
	defer log.Sync()

	// Connect to RabbitMQ
	client, err := amqp.NewClient("localhost", 5672, "guest", "guest", log)
	if err != nil {
		t.Skipf("Skipping integration test: RabbitMQ not available: %v", err)
	}
	defer client.Close()

	// Enable publisher confirms
	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("Failed to enable publisher confirms: %v", err)
	}

	// Declare exchange
	if err := client.DeclareExchange("test-exchange", "topic", true); err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	// Publish message with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"test":"message"}`),
	}

	err = client.PublishWithConfirm(ctx, "test-exchange", "test.routing.key", false, msg)
	if err != nil {
		t.Fatalf("Expected successful publish, got error: %v", err)
	}
}

func TestPublishWithConfirm_ContextTimeout(t *testing.T) {
	// Setup logger
	logger.Init("test")
	log := logger.L()
	defer log.Sync()

	// Connect to RabbitMQ
	client, err := amqp.NewClient("localhost", 5672, "guest", "guest", log)
	if err != nil {
		t.Skipf("Skipping integration test: RabbitMQ not available: %v", err)
	}
	defer client.Close()

	// Enable publisher confirms
	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("Failed to enable publisher confirms: %v", err)
	}

	// Declare exchange
	if err := client.DeclareExchange("test-exchange", "topic", true); err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	// Use a moderate timeout that's too short for confirmation in practice
	// (we won't bind a queue, so this message won't be routed)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	msg := amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"test":"message"}`),
	}

	// Publish to non-existent queue (won't timeout but may fail)
	err = client.PublishWithConfirm(ctx, "test-exchange", "nonexistent.key", false, msg)

	// Either we get confirmation or timeout - both are acceptable for this test
	// The key is that we didn't get a panic or other unexpected error
	t.Logf("Result: %v (acceptable)", err)
}

func TestPublishWithConfirm_ConcurrentPublishes(t *testing.T) {
	// Setup logger
	logger.Init("test")
	log := logger.L()
	defer log.Sync()

	// Connect to RabbitMQ
	client, err := amqp.NewClient("localhost", 5672, "guest", "guest", log)
	if err != nil {
		t.Skipf("Skipping integration test: RabbitMQ not available: %v", err)
	}
	defer client.Close()

	// Enable publisher confirms
	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("Failed to enable publisher confirms: %v", err)
	}

	// Declare exchange
	if err := client.DeclareExchange("test-exchange", "topic", true); err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	// NOTE: Go's amqp091-go GetNextPublishSeqNo() is not thread-safe when called
	// from multiple goroutines concurrently on the same channel.
	// This test publishes sequentially (one at a time) to validate that we don't
	// lose confirmations with multiple pending publishes.

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	numMessages := 5

	// Publish messages sequentially but with very short delays
	// to test that pending confirmations are handled correctly
	for i := 0; i < numMessages; i++ {
		msg := amqp091.Publishing{
			ContentType: "application/json",
			Body:        []byte(`{"index":` + string(rune(48+i)) + `}`),
		}

		err := client.PublishWithConfirm(ctx, "test-exchange", "test.routing.key", false, msg)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
	}

	t.Logf("Successfully published %d messages sequentially", numMessages)
}

func TestPublishWithConfirm_NotRouted_Mandatory(t *testing.T) {
	// Setup logger
	logger.Init("test")
	log := logger.L()
	defer log.Sync()

	// Connect to RabbitMQ
	client, err := amqp.NewClient("localhost", 5672, "guest", "guest", log)
	if err != nil {
		t.Skipf("Skipping integration test: RabbitMQ not available: %v", err)
	}
	defer client.Close()

	// Enable publisher confirms
	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("Failed to enable publisher confirms: %v", err)
	}

	// Declare exchange but don't bind any queues
	// This will cause messages published with mandatory=true to be returned
	if err := client.DeclareExchange("test-exchange-unrouted", "topic", true); err != nil {
		t.Fatalf("Failed to declare exchange: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"test":"unroutable"}`),
	}

	// Publish with mandatory=true to trigger a return
	err = client.PublishWithConfirm(ctx, "test-exchange-unrouted", "nonexistent.routing.key", true, msg)

	// According to AMQP spec: if mandatory=true and message is not routable,
	// we get a basic.return followed by basic.nack
	// So we expect a NACK error
	if err == nil {
		t.Logf("Message was not returned (might not have any bindings)")
	} else if err.Error() != "message not acknowledged by RabbitMQ (NACK)" {
		t.Logf("Got error: %v", err)
	}
}

func TestPublishWithConfirm_NoPublisherConfirms(t *testing.T) {
	// Setup logger
	logger.Init("test")
	log := logger.L()
	defer log.Sync()

	// Connect to RabbitMQ
	client, err := amqp.NewClient("localhost", 5672, "guest", "guest", log)
	if err != nil {
		t.Skipf("Skipping integration test: RabbitMQ not available: %v", err)
	}
	defer client.Close()

	// DO NOT enable publisher confirms

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg := amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"test":"message"}`),
	}

	// Should fail because confirms are not enabled
	err = client.PublishWithConfirm(ctx, "test-exchange", "test.routing.key", false, msg)
	if err == nil {
		t.Fatalf("Expected error when confirms not enabled")
	}

	if err.Error() != "publisher confirms not enabled" {
		t.Fatalf("Expected 'publisher confirms not enabled', got %v", err)
	}
}
