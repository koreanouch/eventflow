package usecase

import (
	"context"
	"fmt"

	"microservice/internal/order/application/dto"
	domainrepo "microservice/internal/order/domain/repository"
	"microservice/pkg/apperrors"
)

// ErrOrderNotFound is returned when the requested order does not exist.
var ErrOrderNotFound = apperrors.NewNotFoundError("ORDER_NOT_FOUND", "order not found")

type GetOrderUseCase struct {
	repo domainrepo.OrderRepository
}

func NewGetOrderUseCase(repo domainrepo.OrderRepository) *GetOrderUseCase {
	return &GetOrderUseCase{repo: repo}
}

func (uc *GetOrderUseCase) Execute(ctx context.Context, orderID string) (*dto.GetOrderResponse, error) {
	if orderID == "" {
		return nil, apperrors.NewValidationError("ORDER_ID_REQUIRED", "order id cannot be empty")
	}

	order, err := uc.repo.GetByID(ctx, orderID)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve order: %w", err)
	}

	if order == nil {
		return nil, ErrOrderNotFound
	}

	// Convert domain items to DTO items
	items := make([]dto.GetOrderItemDTO, 0, len(order.Items()))
	for _, item := range order.Items() {
		items = append(items, dto.GetOrderItemDTO{
			ProductID: item.ProductID(),
			Quantity:  item.Quantity(),
			UnitPrice: item.UnitPrice(),
		})
	}

	return dto.NewGetOrderResponse(
		order.ID(),
		order.CustomerID(),
		items,
		order.TotalAmount(),
		order.Status().String(),
		order.CreatedAt(),
	), nil
}
