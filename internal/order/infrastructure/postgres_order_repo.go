package infrastructure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"microservice/internal/order/domain/entity"
	domainrepo "microservice/internal/order/domain/repository"
	"microservice/pkg/database"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresOrderRepository struct {
	db database.Database
}

func NewPostgresOrderRepository(db database.Database) domainrepo.OrderRepository {
	return &PostgresOrderRepository{db: db}
}

// Save persists an order and all its items within a single transaction.
// If any insertion fails, the transaction is rolled back.
func (r *PostgresOrderRepository) Save(ctx context.Context, order *entity.Order) error {
	if order == nil {
		return fmt.Errorf("order cannot be nil")
	}

	pool := r.db.Native().(*pgxpool.Pool)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// Insert order
	insertOrderQuery := `
		INSERT INTO orders (id, customer_id, total_amount, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	_, err = tx.Exec(ctx, insertOrderQuery,
		order.ID(),
		order.CustomerID(),
		order.TotalAmount(),
		order.Status().String(),
		order.CreatedAt(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert order: %w", err)
	}

	if err = insertOrderItems(ctx, tx, order); err != nil {
		return err
	}

	// Commit transaction
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func insertOrderItems(ctx context.Context, tx pgx.Tx, order *entity.Order) error {
	insertItemQuery := `
		INSERT INTO order_items (order_id, product_id, quantity, unit_price)
		VALUES ($1, $2, $3, $4)
	`
	for _, item := range order.Items() {
		_, err := tx.Exec(ctx, insertItemQuery,
			order.ID(),
			item.ProductID(),
			item.Quantity(),
			item.UnitPrice(),
		)
		if err != nil {
			return fmt.Errorf("failed to insert order item: %w", err)
		}
	}

	return nil
}

// SaveIdempotent serializes competing requests through the unique key.
func (r *PostgresOrderRepository) SaveIdempotent(ctx context.Context, order *entity.Order, key string, requestHash [32]byte) (domainrepo.CreatedOrder, error) {
	var result domainrepo.CreatedOrder
	if order == nil || key == "" {
		return result, fmt.Errorf("order and idempotency key are required")
	}

	pool := r.db.Native().(*pgxpool.Pool)
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return result, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	err = tx.QueryRow(ctx, `
		INSERT INTO orders (id, customer_id, total_amount, status, created_at, idempotency_key, request_hash)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id, status, created_at`,
		order.ID(), order.CustomerID(), order.TotalAmount(), order.Status().String(),
		order.CreatedAt(), key, requestHash[:],
	).Scan(&result.ID, &result.Status, &result.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		var existingHash []byte
		err = tx.QueryRow(ctx, `
			SELECT id, status, created_at, request_hash FROM orders WHERE idempotency_key = $1`,
			key,
		).Scan(&result.ID, &result.Status, &result.CreatedAt, &existingHash)
		if err != nil {
			return domainrepo.CreatedOrder{}, fmt.Errorf("failed to query idempotent order: %w", err)
		}
		if !bytes.Equal(existingHash, requestHash[:]) {
			return domainrepo.CreatedOrder{}, domainrepo.ErrIdempotencyConflict
		}
	} else if err != nil {
		return domainrepo.CreatedOrder{}, fmt.Errorf("failed to insert idempotent order: %w", err)
	} else if err = insertOrderItems(ctx, tx, order); err != nil {
		return domainrepo.CreatedOrder{}, err
	}

	if err = tx.Commit(ctx); err != nil {
		return domainrepo.CreatedOrder{}, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return result, nil
}

// GetByID retrieves an order and all its items by ID.
// If the order does not exist, returns nil, nil so that the GetOrderUseCase
// can produce ErrOrderNotFound.
func (r *PostgresOrderRepository) GetByID(ctx context.Context, id string) (*entity.Order, error) {
	if id == "" {
		return nil, fmt.Errorf("order id cannot be empty")
	}

	pool := r.db.Native().(*pgxpool.Pool)

	// Fetch order
	getOrderQuery := `
		SELECT id, customer_id, total_amount, status, created_at
		FROM orders
		WHERE id = $1
	`

	var orderID, customerID, status string
	var totalAmount int64
	var createdAt time.Time

	err := pool.QueryRow(ctx, getOrderQuery, id).Scan(&orderID, &customerID, &totalAmount, &status, &createdAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to query order: %w", err)
	}

	// Fetch order items
	getItemsQuery := `
		SELECT product_id, quantity, unit_price
		FROM order_items
		WHERE order_id = $1
		ORDER BY id ASC
	`

	rows, err := pool.Query(ctx, getItemsQuery, id)
	if err != nil {
		return nil, fmt.Errorf("failed to query order items: %w", err)
	}
	defer rows.Close()

	var items []entity.OrderItem
	for rows.Next() {
		var productID string
		var quantity, unitPrice int64

		err = rows.Scan(&productID, &quantity, &unitPrice)
		if err != nil {
			return nil, fmt.Errorf("failed to scan order item: %w", err)
		}

		item, err := entity.NewOrderItem(productID, quantity, unitPrice)
		if err != nil {
			return nil, fmt.Errorf("invalid item from database: %w", err)
		}

		items = append(items, item)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating order items: %w", err)
	}

	// If no items found, something is inconsistent. Return error.
	if len(items) == 0 {
		return nil, fmt.Errorf("order %s has no items (data inconsistency)", id)
	}

	// Reconstruct order from persisted data
	order, err := entity.RestoreOrder(orderID, customerID, items, entity.OrderStatus(status), createdAt)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct order from database: %w", err)
	}
	if order.TotalAmount() != totalAmount {
		return nil, fmt.Errorf("order %s has inconsistent total amount", orderID)
	}

	return order, nil
}
