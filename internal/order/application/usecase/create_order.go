package usecase

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"microservice/internal/order/application/dto"
	"microservice/internal/order/domain/entity"
	domainrepo "microservice/internal/order/domain/repository"
	"microservice/pkg/apperrors"
	"strings"

	"github.com/google/uuid"
)

type CreateOrderUseCase struct {
	repo domainrepo.OrderRepository
}

func NewCreateOrderUseCase(repo domainrepo.OrderRepository) *CreateOrderUseCase {
	return &CreateOrderUseCase{repo: repo}
}

func (uc *CreateOrderUseCase) Execute(ctx context.Context, req *dto.CreateOrderRequest, key string) (*dto.CreateOrderResponse, error) {
	if strings.TrimSpace(key) == "" || len(key) > 255 {
		return nil, apperrors.NewValidationError(
			"ORDER_IDEMPOTENCY_KEY_INVALID",
			"idempotency key must be nonempty and at most 255 bytes",
		)
	}
	if req == nil {
		return nil, apperrors.NewValidationError("ORDER_REQUEST_INVALID", "create order request is required")
	}

	if strings.TrimSpace(req.CustomerID) == "" {
		return nil, apperrors.NewValidationError("ORDER_CUSTOMER_REQUIRED", "customer id is required")
	}

	if len(req.Items) == 0 {
		return nil, apperrors.NewValidationError("ORDER_ITEMS_REQUIRED", "at least one item is required")
	}

	// Convert DTO items to domain items
	items := make([]entity.OrderItem, 0, len(req.Items))
	for i, itemReq := range req.Items {
		item, err := entity.NewOrderItem(itemReq.ProductID, itemReq.Quantity, itemReq.UnitPrice)
		if err != nil {
			return nil, apperrors.NewValidationError(
				"ORDER_ITEM_INVALID",
				fmt.Sprintf("invalid item at position %d: %v", i, err),
			)
		}
		items = append(items, item)
	}

	// Generate order ID
	orderID := uuid.New().String()

	// Create order entity
	order, err := entity.NewOrder(orderID, req.CustomerID, items)
	if err != nil {
		return nil, apperrors.NewValidationError("ORDER_INVALID", err.Error())
	}

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to encode order request: %w", err)
	}
	requestHash := sha256.Sum256(payload)

	persisted, err := uc.repo.SaveIdempotent(ctx, order, key, requestHash)
	if err != nil {
		if errors.Is(err, domainrepo.ErrIdempotencyConflict) {
			return nil, apperrors.NewConflictError(
				"ORDER_IDEMPOTENCY_CONFLICT",
				"idempotency key already used with different content",
			)
		}
		return nil, fmt.Errorf("failed to save order: %w", err)
	}

	return dto.NewCreateOrderResponse(persisted.ID, persisted.Status, persisted.CreatedAt), nil
}
