package usecase

import (
	"context"
	"errors"
	"fmt"

	"microservice/internal/order/application/dto"
	domainrepo "microservice/internal/order/domain/repository"
)

var ErrOrderNotFound = errors.New("order not found")

type GetOrderUseCase struct {
	repo domainrepo.OrderRepository
}

func NewGetOrderUseCase(repo domainrepo.OrderRepository) *GetOrderUseCase {
	return &GetOrderUseCase{repo: repo}
}

func (uc *GetOrderUseCase) Execute(ctx context.Context, orderID string) (*dto.GetOrderResponse, error) {
	if orderID == "" {
		return nil, fmt.Errorf("order id cannot be empty")
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
