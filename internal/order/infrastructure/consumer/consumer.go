package consumer

import (
	"context"
	"fmt"
	"microservice/internal/order/domain/event"
	"microservice/internal/order/infrastructure/queue"
	"microservice/pkg/metrics"
	"microservice/pkg/reqid"
	"strconv"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// EventProcessor es el contrato que necesita el consumidor para procesar
// eventos. Lo implementa processor.EventProcessor.
type EventProcessor interface {
	ParseOrderCreatedEvent(body []byte) (*event.OrderCreated, error)
	ProcessOrderCreated(ctx context.Context, evt *event.OrderCreated, consumerName string) error
}

// Publisher abstrae la publicación con confirmación (amqp.Client).
type Publisher interface {
	PublishWithConfirm(ctx context.Context, exchange, routingKey string, mandatory bool, msg amqplib.Publishing) error
}

// Config agrupa los parámetros de reintento y tiempos del consumidor.
type Config struct {
	ConsumerName      string
	RetryExchange     string
	RetryQueueBase    string
	DLQExchange       string
	DLQRoutingKey     string
	MaxRetries        int
	RetryDelayMs      int
	MaxRetryDelayMs   int
	ProcessingTimeout time.Duration
	PublishTimeout    time.Duration
}

// Message desacopla el handler del tipo concreto amqp.Delivery, lo que permite
// probar ACK/NACK y la política de reintentos sin un broker real.
type Message struct {
	Body      []byte
	MessageID string
	Headers   amqplib.Table
	Ack       func(multiple bool) error
	Nack      func(multiple, requeue bool) error
}

type Handler struct {
	processor EventProcessor
	publisher Publisher
	cfg       Config
	log       *zap.Logger
}

func NewHandler(processor EventProcessor, publisher Publisher, cfg Config, log *zap.Logger) *Handler {
	if log == nil {
		log = zap.NewNop()
	}
	return &Handler{processor: processor, publisher: publisher, cfg: cfg, log: log}
}

// Handle aplica la política de confirmación: ACK solo cuando el evento se
// procesó o quedó registrado en la DLQ; NACK con requeue si la publicación de
// reintento/DLQ falla, para no perder el mensaje.
func (h *Handler) Handle(ctx context.Context, msg Message) {
	requestID := headerString(msg.Headers, "x-request-id")
	log := h.log.With(zap.String("message_id", msg.MessageID))
	if requestID != "" {
		log = log.With(zap.String("request_id", requestID))
	}

	// La publicación no hereda el deadline de procesamiento: si el evento
	// expiró, el contexto original ya está cancelado y no podría publicarse.
	// Se quita también la cancelación por shutdown para que el mensaje no se
	// pierda a mitad de la transición a retry/DLQ.
	publishCtx, cancelPublish := context.WithTimeout(context.WithoutCancel(ctx), h.cfg.PublishTimeout)
	defer cancelPublish()

	evt, err := h.processor.ParseOrderCreatedEvent(msg.Body)
	if err != nil {
		metrics.Inc(metrics.WorkerMessagesInvalidTotal, nil)
		log.Error("failed to parse message, sending to DLQ", zap.Error(err))
		if err := h.sendToDLQ(publishCtx, msg, 0, "invalid_message", log); err != nil {
			log.Error("failed to send malformed message to DLQ, NACKing", zap.Error(err))
			h.nack(msg, false, log)
			return
		}
		h.ack(msg, log)
		return
	}

	retryCount := retryCountFromHeaders(msg.Headers)

	// La correlación viaja en el evento para que cualquier log del worker
	// pueda incluirla sin conocer las cabeceras AMQP.
	processCtx, cancelProcess := context.WithTimeout(ctx, h.cfg.ProcessingTimeout)
	if requestID != "" {
		processCtx = reqid.WithContext(processCtx, requestID)
	}
	processStart := time.Now()
	err = h.processor.ProcessOrderCreated(processCtx, evt, h.cfg.ConsumerName)
	cancelProcess()
	metrics.Observe(metrics.WorkerProcessingDurationSeconds, map[string]string{
		"consumer": h.cfg.ConsumerName,
	}, time.Since(processStart).Seconds())
	if err != nil {
		metrics.Inc(metrics.WorkerMessagesFailedTotal, nil)
		log.Error("failed to process event",
			zap.String("event_id", evt.EventID),
			zap.Int32("retry_count", retryCount),
			zap.Error(err),
		)

		if retryCount >= int32(h.cfg.MaxRetries) {
			log.Error("max retries exceeded, sending to DLQ",
				zap.String("event_id", evt.EventID),
				zap.Int32("retry_count", retryCount),
			)
			if err := h.sendToDLQ(publishCtx, msg, retryCount, "max_retries_exceeded", log); err != nil {
				log.Error("failed to send to DLQ, NACKing with requeue", zap.Error(err))
				h.nack(msg, true, log)
				return
			}
			h.ack(msg, log)
			return
		}

		nextRetryCount := retryCount + 1
		if err := h.sendToRetry(publishCtx, msg, nextRetryCount, log); err != nil {
			log.Error("failed to publish to retry queue, NACKing with requeue",
				zap.Int32("retry_count", nextRetryCount),
				zap.Error(err),
			)
			h.nack(msg, true, log)
			return
		}

		h.ack(msg, log)
		return
	}

	h.ack(msg, log)
	metrics.Inc(metrics.WorkerMessagesProcessedTotal, nil)
	log.Info("message processed successfully",
		zap.String("event_id", evt.EventID),
		zap.String("consumer", h.cfg.ConsumerName),
	)
}

func (h *Handler) sendToRetry(ctx context.Context, msg Message, retryCount int32, log *zap.Logger) error {
	headers := copyHeaders(msg.Headers)
	headers["x-retry-count"] = retryCount

	retryQueue := queue.RetryQueueName(h.cfg.RetryQueueBase, int(retryCount))
	delayMs := queue.RetryDelayMs(h.cfg.RetryDelayMs, h.cfg.MaxRetryDelayMs, int(retryCount))

	err := h.publisher.PublishWithConfirm(ctx, h.cfg.RetryExchange, retryQueue, true, amqplib.Publishing{
		ContentType:  "application/json",
		Body:         msg.Body,
		DeliveryMode: amqplib.Persistent,
		MessageId:    msg.MessageID,
		Headers:      headers,
	})
	if err != nil {
		metrics.Inc(metrics.WorkerPublishFailuresTotal, map[string]string{"target": "retry"})
		return fmt.Errorf("failed to publish to retry queue: %w", err)
	}

	metrics.Inc(metrics.WorkerMessagesRetriedTotal, map[string]string{
		"retry": strconv.Itoa(int(retryCount)),
	})
	log.Info("message sent to retry queue",
		zap.Int32("retry_count", retryCount),
		zap.String("queue", retryQueue),
		zap.Int("delay_ms", delayMs),
	)
	return nil
}

func (h *Handler) sendToDLQ(ctx context.Context, msg Message, retryCount int32, reason string, log *zap.Logger) error {
	headers := copyHeaders(msg.Headers)
	headers["x-retry-count"] = retryCount
	headers["x-dead-letter-reason"] = reason

	err := h.publisher.PublishWithConfirm(ctx, h.cfg.DLQExchange, h.cfg.DLQRoutingKey, true, amqplib.Publishing{
		ContentType:  "application/json",
		Body:         msg.Body,
		DeliveryMode: amqplib.Persistent,
		MessageId:    msg.MessageID,
		Timestamp:    time.Now().UTC(),
		Headers:      headers,
	})
	if err != nil {
		metrics.Inc(metrics.WorkerPublishFailuresTotal, map[string]string{"target": "dlq"})
		return fmt.Errorf("failed to publish to DLQ: %w", err)
	}

	metrics.Inc(metrics.WorkerMessagesDeadLetteredTotal, map[string]string{"reason": reason})
	log.Info("message sent to DLQ",
		zap.Int32("retry_count", retryCount),
		zap.String("reason", reason),
		zap.String("routing_key", h.cfg.DLQRoutingKey),
	)
	return nil
}

func (h *Handler) ack(msg Message, log *zap.Logger) {
	if err := msg.Ack(false); err != nil {
		log.Error("failed to ACK message", zap.Error(err))
	}
}

func (h *Handler) nack(msg Message, requeue bool, log *zap.Logger) {
	if err := msg.Nack(false, requeue); err != nil {
		log.Error("failed to NACK message", zap.Bool("requeue", requeue), zap.Error(err))
	}
}

// retryCountFromHeaders tolera los tipos que RabbitMQ puede usar al decodificar
// un entero de cabecera (int32, int64, float64 o int).
func retryCountFromHeaders(headers amqplib.Table) int32 {
	value, ok := headers["x-retry-count"]
	if !ok {
		return 0
	}

	var count int64
	switch v := value.(type) {
	case int32:
		count = int64(v)
	case int64:
		count = v
	case int:
		count = int64(v)
	case float64:
		count = int64(v)
	default:
		return 0
	}
	if count < 0 {
		return 0
	}
	return int32(count)
}

func headerString(headers amqplib.Table, key string) string {
	value, ok := headers[key]
	if !ok {
		return ""
	}
	s, _ := value.(string)
	return s
}

func copyHeaders(headers amqplib.Table) amqplib.Table {
	out := make(amqplib.Table, len(headers)+2)
	for key, value := range headers {
		out[key] = value
	}
	return out
}
