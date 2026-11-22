package amqp

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"microservice/pkg/logger"
)

// Estas pruebas de integración requieren un RabbitMQ accesible en localhost:5672
// (el de docker compose). Se omiten si el broker no está disponible.

func newReconnectTestClient(t *testing.T) *Client {
	t.Helper()

	logger.Init("test")
	client, err := NewClient("localhost", 5672, "guest", "guest", logger.L())
	if err != nil {
		t.Skipf("Skipping integration test: RabbitMQ not available: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func newTestContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	return context.WithTimeout(context.Background(), 10*time.Second)
}

// breakConnection simula la caída del broker cerrando el canal y la conexión
// subyacentes, que es el estado que deja una desconexión real.
func breakConnection(t *testing.T, client *Client) {
	t.Helper()

	client.mu.Lock()
	defer client.mu.Unlock()

	if client.channel != nil {
		_ = client.channel.Close()
	}
	if client.conn != nil {
		_ = client.conn.Close()
	}

	if !client.connectionBrokenLocked() {
		t.Fatal("expected broken connection after closing channel and connection")
	}
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestEnsureConnected_ReconnectsAndRunsSetupHooks(t *testing.T) {
	client := newReconnectTestClient(t)

	var hookRuns atomic.Int32
	client.OnReconnect(func(ch *amqp091.Channel) error {
		hookRuns.Add(1)
		return ch.ExchangeDeclare(uniqueName("amqp-reconnect-exchange"), "topic", false, false, false, false, nil)
	})

	ctx, cancel := newTestContext(t)
	defer cancel()

	// Con la conexión sana no se ejecuta ningún hook.
	if err := client.EnsureConnected(ctx); err != nil {
		t.Fatalf("EnsureConnected with healthy connection: %v", err)
	}
	if got := hookRuns.Load(); got != 0 {
		t.Fatalf("setup hooks ran %d times with a healthy connection, want 0", got)
	}

	breakConnection(t, client)

	if err := client.EnsureConnected(ctx); err != nil {
		t.Fatalf("EnsureConnected after connection loss: %v", err)
	}
	if got := hookRuns.Load(); got != 1 {
		t.Fatalf("setup hooks ran %d times after reconnect, want 1", got)
	}
	if err := client.Ping(); err != nil {
		t.Fatalf("Ping after reconnect: %v", err)
	}
}

func TestPingContext_RecoversConnectionForReadiness(t *testing.T) {
	client := newReconnectTestClient(t)

	ctx, cancel := newTestContext(t)
	defer cancel()

	breakConnection(t, client)

	if err := client.Ping(); err == nil {
		t.Fatal("Ping should report the broken connection without reconnecting")
	}

	if err := client.PingContext(ctx); err != nil {
		t.Fatalf("PingContext should reconnect and report healthy, got: %v", err)
	}
	if err := client.Ping(); err != nil {
		t.Fatalf("Ping after PingContext reconnect: %v", err)
	}
}

func TestLazyClient_ConnectsOnFirstUse(t *testing.T) {
	logger.Init("test")
	client := NewLazyClient("localhost", 5672, "guest", "guest", logger.L())
	t.Cleanup(func() { _ = client.Close() })

	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("EnablePublisherConfirms on lazy client: %v", err)
	}

	var hookRuns atomic.Int32
	client.OnReconnect(func(*amqp091.Channel) error {
		hookRuns.Add(1)
		return nil
	})

	if err := client.Ping(); err == nil {
		t.Fatal("Ping should fail before the lazy client connects")
	}

	ctx, cancel := newTestContext(t)
	defer cancel()

	if err := client.EnsureConnected(ctx); err != nil {
		t.Fatalf("EnsureConnected on lazy client: %v", err)
	}
	if got := hookRuns.Load(); got != 1 {
		t.Fatalf("setup hooks ran %d times on first connect, want 1", got)
	}

	queueName := uniqueName("amqp-lazy-queue")
	if _, err := client.DeclareQueue(queueName, false, nil); err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}

	if err := client.PublishWithConfirm(ctx, "", queueName, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"test":"lazy"}`),
	}); err != nil {
		t.Fatalf("PublishWithConfirm on lazy client: %v", err)
	}
}

func TestPublishWithConfirm_RecoversAfterConnectionLoss(t *testing.T) {
	client := newReconnectTestClient(t)

	if err := client.EnablePublisherConfirms(); err != nil {
		t.Fatalf("EnablePublisherConfirms: %v", err)
	}

	queueName := uniqueName("amqp-publish-queue")
	if _, err := client.DeclareQueue(queueName, false, nil); err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}

	breakConnection(t, client)

	ctx, cancel := newTestContext(t)
	defer cancel()

	if err := client.PublishWithConfirm(ctx, "", queueName, false, amqp091.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"test":"reconnect"}`),
	}); err != nil {
		t.Fatalf("PublishWithConfirm should reconnect and publish, got: %v", err)
	}
}

func TestClose_PreventsReconnect(t *testing.T) {
	client := newReconnectTestClient(t)

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ctx, cancel := newTestContext(t)
	defer cancel()

	if err := client.EnsureConnected(ctx); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("EnsureConnected after Close = %v, want ErrClientClosed", err)
	}
}
