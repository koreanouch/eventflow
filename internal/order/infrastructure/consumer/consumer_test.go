package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"microservice/internal/order/domain/event"
	"microservice/pkg/reqid"

	amqplib "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type fakeProcessor struct {
	evt           *event.OrderCreated
	parseErr      error
	processErr    error
	waitForCtx    bool
	seenRequestID string
}

func (f *fakeProcessor) ParseOrderCreatedEvent(body []byte) (*event.OrderCreated, error) {
	if f.parseErr != nil {
		return nil, f.parseErr
	}
	if f.evt != nil {
		return f.evt, nil
	}
	return &event.OrderCreated{EventID: "order.created.test", OrderID: "test"}, nil
}

func (f *fakeProcessor) ProcessOrderCreated(ctx context.Context, evt *event.OrderCreated, consumerName string) error {
	f.seenRequestID = reqid.FromContext(ctx)
	if f.waitForCtx {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.processErr
}

type publishCall struct {
	exchange   string
	routingKey string
	msg        amqplib.Publishing
}

type fakePublisher struct {
	calls []publishCall
	err   error
}

func (f *fakePublisher) PublishWithConfirm(ctx context.Context, exchange, routingKey string, mandatory bool, msg amqplib.Publishing) error {
	f.calls = append(f.calls, publishCall{exchange: exchange, routingKey: routingKey, msg: msg})
	return f.err
}

type ackState struct {
	acked   int
	nacked  int
	requeue bool
}

func newMessage(body []byte, headers amqplib.Table) (Message, *ackState) {
	state := &ackState{}
	return Message{
		Body:      body,
		MessageID: "msg-1",
		Headers:   headers,
		Ack: func(multiple bool) error {
			state.acked++
			return nil
		},
		Nack: func(multiple, requeue bool) error {
			state.nacked++
			state.requeue = requeue
			return nil
		},
	}, state
}

func testConfig() Config {
	return Config{
		ConsumerName:      "order-worker",
		RetryExchange:     "orders.exchange.retry",
		RetryQueueBase:    "orders.retry",
		DLQExchange:       "orders.exchange",
		DLQRoutingKey:     "dead-letter.retry",
		MaxRetries:        3,
		RetryDelayMs:      5000,
		MaxRetryDelayMs:   60000,
		ProcessingTimeout: time.Second,
		PublishTimeout:    time.Second,
	}
}

func newTestHandler(processor EventProcessor, publisher Publisher) *Handler {
	return NewHandler(processor, publisher, testConfig(), zap.NewNop())
}

func TestHandle_SuccessAcksWithoutPublishing(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(&fakeProcessor{}, publisher)
	msg, state := newMessage([]byte(`{}`), nil)

	handler.Handle(context.Background(), msg)

	if state.acked != 1 || state.nacked != 0 {
		t.Fatalf("ack=%d nack=%d, want ack=1 nack=0", state.acked, state.nacked)
	}
	if len(publisher.calls) != 0 {
		t.Fatalf("published %d messages, want 0", len(publisher.calls))
	}
}

func TestHandle_TransientErrorSchedulesRetry(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(&fakeProcessor{processErr: errors.New("db down")}, publisher)
	msg, state := newMessage([]byte(`{}`), amqplib.Table{"x-request-id": "req-123"})

	handler.Handle(context.Background(), msg)

	if state.acked != 1 || state.nacked != 0 {
		t.Fatalf("ack=%d nack=%d, want ack=1", state.acked, state.nacked)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("published %d messages, want 1", len(publisher.calls))
	}

	call := publisher.calls[0]
	if call.exchange != "orders.exchange.retry" || call.routingKey != "orders.retry.1" {
		t.Errorf("retry published to %s/%s, want orders.exchange.retry/orders.retry.1", call.exchange, call.routingKey)
	}
	if got := call.msg.Headers["x-retry-count"]; got != int32(1) {
		t.Errorf("x-retry-count = %v, want 1", got)
	}
	if got := call.msg.Headers["x-request-id"]; got != "req-123" {
		t.Errorf("x-request-id = %v, want req-123", got)
	}
	if string(call.msg.Body) != `{}` {
		t.Errorf("body = %q, want original body", call.msg.Body)
	}
}

func TestHandle_UsesNextRetryQueue(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(&fakeProcessor{processErr: errors.New("db down")}, publisher)
	msg, _ := newMessage([]byte(`{}`), amqplib.Table{"x-retry-count": int64(1)})

	handler.Handle(context.Background(), msg)

	if len(publisher.calls) != 1 {
		t.Fatalf("published %d messages, want 1", len(publisher.calls))
	}
	if got := publisher.calls[0].routingKey; got != "orders.retry.2" {
		t.Errorf("routing key = %q, want orders.retry.2", got)
	}
	if got := publisher.calls[0].msg.Headers["x-retry-count"]; got != int32(2) {
		t.Errorf("x-retry-count = %v, want 2", got)
	}
}

func TestHandle_MaxRetriesSendsToDLQ(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(&fakeProcessor{processErr: errors.New("db down")}, publisher)
	msg, state := newMessage([]byte(`{}`), amqplib.Table{"x-retry-count": int32(3)})

	handler.Handle(context.Background(), msg)

	if state.acked != 1 || state.nacked != 0 {
		t.Fatalf("ack=%d nack=%d, want ack=1", state.acked, state.nacked)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("published %d messages, want 1", len(publisher.calls))
	}

	call := publisher.calls[0]
	if call.exchange != "orders.exchange" || call.routingKey != "dead-letter.retry" {
		t.Errorf("DLQ published to %s/%s, want orders.exchange/dead-letter.retry", call.exchange, call.routingKey)
	}
	if got := call.msg.Headers["x-dead-letter-reason"]; got != "max_retries_exceeded" {
		t.Errorf("dead-letter reason = %v, want max_retries_exceeded", got)
	}
}

func TestHandle_RetryPublishFailureNacksWithRequeue(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("broker down")}
	handler := newTestHandler(&fakeProcessor{processErr: errors.New("db down")}, publisher)
	msg, state := newMessage([]byte(`{}`), nil)

	handler.Handle(context.Background(), msg)

	if state.acked != 0 || state.nacked != 1 {
		t.Fatalf("ack=%d nack=%d, want nack=1", state.acked, state.nacked)
	}
	if !state.requeue {
		t.Error("NACK requeue = false, want true to preserve the message")
	}
}

func TestHandle_DLQPublishFailureNacksWithRequeue(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("broker down")}
	handler := newTestHandler(&fakeProcessor{processErr: errors.New("db down")}, publisher)
	msg, state := newMessage([]byte(`{}`), amqplib.Table{"x-retry-count": int32(3)})

	handler.Handle(context.Background(), msg)

	if state.acked != 0 || state.nacked != 1 || !state.requeue {
		t.Fatalf("ack=%d nack=%d requeue=%v, want nack with requeue", state.acked, state.nacked, state.requeue)
	}
}

func TestHandle_MalformedMessageGoesToDLQ(t *testing.T) {
	publisher := &fakePublisher{}
	handler := newTestHandler(&fakeProcessor{parseErr: errors.New("invalid json")}, publisher)
	msg, state := newMessage([]byte(`{`), nil)

	handler.Handle(context.Background(), msg)

	if state.acked != 1 || state.nacked != 0 {
		t.Fatalf("ack=%d nack=%d, want ack=1", state.acked, state.nacked)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("published %d messages, want 1", len(publisher.calls))
	}
	if got := publisher.calls[0].msg.Headers["x-dead-letter-reason"]; got != "invalid_message" {
		t.Errorf("dead-letter reason = %v, want invalid_message", got)
	}
}

func TestHandle_ProcessingTimeoutStillSchedulesRetry(t *testing.T) {
	publisher := &fakePublisher{}
	cfg := testConfig()
	cfg.ProcessingTimeout = 5 * time.Millisecond
	handler := NewHandler(
		&fakeProcessor{waitForCtx: true},
		publisher,
		cfg,
		zap.NewNop(),
	)
	msg, state := newMessage([]byte(`{}`), nil)

	handler.Handle(context.Background(), msg)

	if state.acked != 1 {
		t.Fatalf("ack=%d, want 1 after timeout", state.acked)
	}
	if len(publisher.calls) != 1 {
		t.Fatalf("published %d messages, want retry publication", len(publisher.calls))
	}
	if got := publisher.calls[0].routingKey; got != "orders.retry.1" {
		t.Errorf("routing key = %q, want orders.retry.1", got)
	}
}

func TestHandle_PropagatesRequestIDToProcessorContext(t *testing.T) {
	processor := &fakeProcessor{}
	handler := newTestHandler(processor, &fakePublisher{})
	msg, _ := newMessage([]byte(`{}`), amqplib.Table{"x-request-id": "req-abc"})

	handler.Handle(context.Background(), msg)

	if processor.seenRequestID != "req-abc" {
		t.Errorf("processor context request id = %q, want req-abc", processor.seenRequestID)
	}
}

func TestRetryCountFromHeaders(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers amqplib.Table
		want    int32
	}{
		{name: "missing", headers: amqplib.Table{}, want: 0},
		{name: "nil table", headers: nil, want: 0},
		{name: "int32", headers: amqplib.Table{"x-retry-count": int32(2)}, want: 2},
		{name: "int64", headers: amqplib.Table{"x-retry-count": int64(3)}, want: 3},
		{name: "int", headers: amqplib.Table{"x-retry-count": 1}, want: 1},
		{name: "float64", headers: amqplib.Table{"x-retry-count": float64(4)}, want: 4},
		{name: "negative", headers: amqplib.Table{"x-retry-count": int32(-1)}, want: 0},
		{name: "unsupported", headers: amqplib.Table{"x-retry-count": "two"}, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryCountFromHeaders(tc.headers); got != tc.want {
				t.Errorf("retryCountFromHeaders(%v) = %d, want %d", tc.headers, got, tc.want)
			}
		})
	}
}

func TestCopyHeadersDoesNotMutateOriginal(t *testing.T) {
	original := amqplib.Table{"x-retry-count": int32(1)}
	copied := copyHeaders(original)
	copied["x-retry-count"] = int32(9)
	copied["x-dead-letter-reason"] = "test"

	if original["x-retry-count"] != int32(1) {
		t.Errorf("original header was mutated: %v", original["x-retry-count"])
	}
	if _, ok := original["x-dead-letter-reason"]; ok {
		t.Error("original table gained a key from the copy")
	}
}
