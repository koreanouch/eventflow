package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"microservice/internal/order/application/dto"
	"microservice/internal/order/application/usecase"
	"microservice/internal/order/domain/entity"
	"microservice/internal/order/domain/event"
	"microservice/internal/order/domain/repository"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Mock repositories for testing
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

func TestCreateOrder_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, mockPublisher, logger)

	req := dto.CreateOrderRequest{
		CustomerID: "cust-123",
		Items: []dto.OrderItemDTO{
			{
				ProductID: "prod-1",
				Quantity:  2,
				UnitPrice: 1000,
			},
			{
				ProductID: "prod-2",
				Quantity:  1,
				UnitPrice: 500,
			},
		},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBuffer(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", "test-key")
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq

	handler.CreateOrder(c)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	var res dto.CreateOrderResponse
	json.Unmarshal(w.Body.Bytes(), &res)

	if res.ID == "" {
		t.Errorf("Expected order ID to be set")
	}

	if res.CreatedAt.IsZero() {
		t.Errorf("Expected CreatedAt to be set")
	}
}

func TestCreateOrder_InvalidRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, mockPublisher, logger)

	// Missing customer_id
	req := `{"items": [{"product_id": "prod-1", "quantity": 1, "unit_price": 100}]}`

	httpReq := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(req))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", "test-key")
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq

	handler.CreateOrder(c)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestCreateOrder_InternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	mockRepo.saveErr = errors.New("database error")
	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, mockPublisher, logger)

	req := dto.CreateOrderRequest{
		CustomerID: "cust-123",
		Items: []dto.OrderItemDTO{
			{
				ProductID: "prod-1",
				Quantity:  1,
				UnitPrice: 1000,
			},
		},
	}

	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBuffer(body))
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", "test-key")
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq

	handler.CreateOrder(c)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}
}

func TestCreateOrder_IdempotencyKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name    string
		key     string
		repoErr error
		want    int
	}{
		{name: "missing", want: http.StatusBadRequest},
		{name: "blank", key: "   ", want: http.StatusBadRequest},
		{name: "too long", key: strings.Repeat("x", 256), want: http.StatusBadRequest},
		{name: "conflict", key: "key", repoErr: repository.ErrIdempotencyConflict, want: http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := newMockOrderRepository()
			repo.saveErr = tc.repoErr
			mockPublisher := &mockEventPublisher{}
			logger := zap.NewNop()
			h := NewOrderHandler(usecase.NewCreateOrderUseCase(repo), usecase.NewGetOrderUseCase(repo), mockPublisher, logger)
			req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(
				`{"customer_id":"cust","items":[{"product_id":"prod","quantity":1,"unit_price":100}]}`))
			req.Header.Set("Content-Type", "application/json")
			if tc.key != "" {
				req.Header.Set("Idempotency-Key", tc.key)
			}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = req
			h.CreateOrder(c)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
		})
	}
}

func TestGetOrder_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()

	// Create and save an order
	item, _ := entity.NewOrderItem("prod-1", 2, 1000)
	order, _ := entity.NewOrder("order-123", "cust-456", []entity.OrderItem{item})
	_ = mockRepo.Save(context.Background(), order)

	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, mockPublisher, logger)

	httpReq := httptest.NewRequest(http.MethodGet, "/orders/order-123", nil)
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq
	c.Params = gin.Params{{Key: "id", Value: "order-123"}}

	handler.GetOrder(c)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var res dto.GetOrderResponse
	json.Unmarshal(w.Body.Bytes(), &res)

	if res.ID != "order-123" {
		t.Errorf("Expected order ID order-123, got %s", res.ID)
	}

	if res.CustomerID != "cust-456" {
		t.Errorf("Expected customer ID cust-456, got %s", res.CustomerID)
	}

	if res.TotalAmount != 2000 {
		t.Errorf("Expected total 2000, got %d", res.TotalAmount)
	}

	if res.Status != "pending" {
		t.Errorf("Expected status pending, got %s", res.Status)
	}

	if len(res.Items) != 1 {
		t.Errorf("Expected 1 item, got %d", len(res.Items))
	}
}

func TestGetOrder_NotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, mockPublisher, logger)

	httpReq := httptest.NewRequest(http.MethodGet, "/orders/nonexistent", nil)
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq
	c.Params = gin.Params{{Key: "id", Value: "nonexistent"}}

	handler.GetOrder(c)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}

	var res map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &res)

	if res["error"] != "order not found" {
		t.Errorf("Expected 'order not found' error, got %v", res["error"])
	}
}

func TestGetOrder_InternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	mockRepo.getByIDErr = errors.New("database connection error")
	createUC := usecase.NewCreateOrderUseCase(mockRepo)
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, mockPublisher, logger)

	httpReq := httptest.NewRequest(http.MethodGet, "/orders/order-123", nil)
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq
	c.Params = gin.Params{{Key: "id", Value: "order-123"}}

	handler.GetOrder(c)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}
}
