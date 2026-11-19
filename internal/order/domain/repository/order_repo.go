package repository

import (
	"context"
	"errors"
	"microservice/internal/order/domain/entity"
	"time"
)

var ErrIdempotencyConflict = errors.New("idempotency key already used with different content")

type CreatedOrder struct {
	ID        string
	Status    string
	CreatedAt time.Time
}

type OrderRepository interface {
	Save(ctx context.Context, order *entity.Order) error
	SaveIdempotent(ctx context.Context, order *entity.Order, key string, requestHash [32]byte) (CreatedOrder, error)
	GetByID(ctx context.Context, id string) (*entity.Order, error)
}
