package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"microservice/internal/order/domain/event"
	"microservice/pkg/database"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type EventProcessor struct {
	db  database.Database
	log *zap.Logger
}

func NewEventProcessor(db database.Database, log *zap.Logger) *EventProcessor {
	return &EventProcessor{
		db:  db,
		log: log,
	}
}

func (ep *EventProcessor) ProcessOrderCreated(ctx context.Context, evt *event.OrderCreated, consumerName string) error {
	if evt == nil {
		return fmt.Errorf("event cannot be nil")
	}

	// Get the native pool connection
	pool := ep.db.Native().(*pgxpool.Pool)

	// Use transaction to ensure atomicity of event recording and notification
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// Check if event was already processed by this consumer
	var count int
	err = tx.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM processed_events
		 WHERE event_id = $1 AND consumer_name = $2`,
		evt.EventID,
		consumerName,
	).Scan(&count)

	if err != nil {
		return fmt.Errorf("failed to check processed events: %w", err)
	}

	if count > 0 {
		// Event already processed, commit transaction and return success
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		ep.log.Info("Event already processed, skipping duplicate",
			zap.String("event_id", evt.EventID),
			zap.String("request_id", evt.RequestID),
		)
		return nil
	}

	// Insert event into processed_events table
	_, err = tx.Exec(
		ctx,
		`INSERT INTO processed_events (event_id, event_type, order_id, consumer_name)
		 VALUES ($1, $2, $3, $4)`,
		evt.EventID,
		evt.EventType,
		evt.OrderID,
		consumerName,
	)

	if err != nil {
		return fmt.Errorf("failed to insert processed event: %w", err)
	}

	// Create notification record
	message := fmt.Sprintf("Order %s created for customer %s with total amount %d",
		evt.OrderID, evt.CustomerID, evt.TotalAmount)

	_, err = tx.Exec(
		ctx,
		`INSERT INTO notifications (event_id, order_id, customer_id, message, status)
		 VALUES ($1, $2, $3, $4, $5)`,
		evt.EventID,
		evt.OrderID,
		evt.CustomerID,
		message,
		"created",
	)

	if err != nil {
		return fmt.Errorf("failed to insert notification: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	ep.log.Info("Event processed successfully",
		zap.String("event_id", evt.EventID),
		zap.String("order_id", evt.OrderID),
		zap.String("consumer", consumerName),
		zap.String("request_id", evt.RequestID),
	)

	return nil
}

// ParseOrderCreatedEvent parses a JSON message into an OrderCreated event
func (ep *EventProcessor) ParseOrderCreatedEvent(body []byte) (*event.OrderCreated, error) {
	var evt event.OrderCreated
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}
	return &evt, nil
}
