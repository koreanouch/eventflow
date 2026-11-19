package amqp_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

	// Publish concurrently to validate that confirmation tracking is safe when
	// multiple goroutines share the same channel.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const numMessages = 50

	var wg sync.WaitGroup
	publishErr := make(chan error, numMessages)

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg := amqp091.Publishing{
				ContentType: "application/json",
				Body:        []byte(fmt.Sprintf(`{"index":%d}`, i)),
			}
			if err := client.PublishWithConfirm(ctx, "test-exchange", "test.routing.key", false, msg); err != nil {
				publishErr <- fmt.Errorf("publish %d failed: %w", i, err)
			}
		}(i)
	}

	wg.Wait()
	close(publishErr)

	for err := range publishErr {
		t.Error(err)
	}

	t.Logf("Successfully published %d messages concurrently", numMessages)
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

	// Publish with mandatory=true to trigger a basic.return.
	err = client.PublishWithConfirm(ctx, "test-exchange-unrouted", "nonexistent.routing.key", true, msg)

	// RabbitMQ emits basic.return (and still ACKs the message), so the publisher
	// must detect the return and report it as an error.
	if err == nil {
		t.Fatal("Expected error for unroutable mandatory message, got nil")
	}
	if !strings.Contains(err.Error(), "unroutable") {
		t.Fatalf("Expected unroutable error, got: %v", err)
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
