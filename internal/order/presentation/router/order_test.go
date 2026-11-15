package router

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"microservice/internal/order/application/dto"
	"microservice/internal/order/application/usecase"
	"microservice/internal/order/domain/entity"
	"microservice/internal/order/domain/event"
	"microservice/internal/order/domain/repository"
	"microservice/internal/order/presentation/handler"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type mockOrderRepository struct {
	saveErr    error
	getByIDErr error
	orders     map[string]*entity.Order
}

type mockEventPublisher struct {
	publishErr error
}

func (m *mockEventPublisher) PublishOrderCreated(ctx context.Context, evt *event.OrderCreated) error {
	return m.publishErr
}

func newMockOrderRepository() *mockOrderRepository {
	return &mockOrderRepository{
		orders: make(map[string]*entity.Order),
	}
}

func (m *mockOrderRepository) Save(ctx context.Context, order *entity.Order) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.orders[order.ID()] = order
	return nil
}

func (m *mockOrderRepository) SaveIdempotent(ctx context.Context, order *entity.Order, key string, hash [32]byte) (repository.CreatedOrder, error) {
	if err := m.Save(ctx, order); err != nil {
		return repository.CreatedOrder{}, err
	}
	return repository.CreatedOrder{ID: order.ID(), CreatedAt: order.CreatedAt()}, nil
}

func (m *mockOrderRepository) GetByID(ctx context.Context, id string) (*entity.Order, error) {
	if m.getByIDErr != nil {
		return nil, m.getByIDErr
	}
	if order, exists := m.orders[id]; exists {
		return order, nil
	}
	return nil, nil
}

func TestRegisterRoutes_POST_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	api := router.Group("api")

	mockRepo := newMockOrderRepository()
	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	orderHandler := handler.NewOrderHandler(createUC, getUC, mockPublisher, logger)

	RegisterRoutes(api, orderHandler)

	req := dto.CreateOrderRequest{
		CustomerID: "cust-123",
		Items: []dto.OrderItemDTO{
			{
				ProductID: "prod-1",
				Quantity:  2,
				UnitPrice: 1000,
			},
		},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/api/v1/orders", bytes.NewBuffer(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", "test-key")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, httpReq)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	var res dto.CreateOrderResponse
	json.Unmarshal(w.Body.Bytes(), &res)

	if res.ID == "" {
		t.Errorf("Expected order ID to be set")
	}
}

func TestRegisterRoutes_GET_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	api := router.Group("api")

	mockRepo := newMockOrderRepository()

	// Create and save an order
	item, _ := entity.NewOrderItem("prod-1", 2, 1000)
	order, _ := entity.NewOrder("order-123", "cust-456", []entity.OrderItem{item})
	_ = mockRepo.Save(context.Background(), order)

	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	orderHandler := handler.NewOrderHandler(createUC, getUC, mockPublisher, logger)

	RegisterRoutes(api, orderHandler)

	httpReq := httptest.NewRequest(http.MethodGet, "/api/v1/orders/order-123", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, httpReq)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var res dto.GetOrderResponse
	json.Unmarshal(w.Body.Bytes(), &res)

	if res.ID != "order-123" {
		t.Errorf("Expected order ID order-123, got %s", res.ID)
	}
}

func TestRegisterRoutes_GET_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	api := router.Group("api")

	mockRepo := newMockOrderRepository()
	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	orderHandler := handler.NewOrderHandler(createUC, getUC, mockPublisher, logger)

	RegisterRoutes(api, orderHandler)

	httpReq := httptest.NewRequest(http.MethodGet, "/api/v1/orders/nonexistent", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, httpReq)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}
