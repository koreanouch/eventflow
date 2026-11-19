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
	"microservice/pkg/httputil"

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
	events     []*event.OrderCreated
}

func (m *mockEventPublisher) PublishOrderCreated(ctx context.Context, evt *event.OrderCreated) error {
	m.events = append(m.events, evt)
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
	return repository.CreatedOrder{ID: order.ID(), Status: order.Status().String(), CreatedAt: order.CreatedAt()}, nil
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
	mockPublisher := &mockEventPublisher{}
	createUC := usecase.NewCreateOrderUseCase(mockRepo, mockPublisher, zap.NewNop())
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, logger)

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

	if res.Status != "pending" {
		t.Errorf("Expected initial status pending, got %q", res.Status)
	}
}

func TestCreateOrder_PublishesEventFromUseCase(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	mockPublisher := &mockEventPublisher{}
	h := NewOrderHandler(
		usecase.NewCreateOrderUseCase(mockRepo, mockPublisher, zap.NewNop()),
		usecase.NewGetOrderUseCase(mockRepo),
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(
		`{"customer_id":"cust-123","items":[{"product_id":"prod-1","quantity":2,"unit_price":1000}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "test-key")
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = req
	h.CreateOrder(c)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}

	var res dto.CreateOrderResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("invalid response: %v", err)
	}

	if len(mockPublisher.events) != 1 {
		t.Fatalf("published %d events, want 1", len(mockPublisher.events))
	}
	evt := mockPublisher.events[0]
	if evt.OrderID != res.ID || evt.EventID != "order.created."+res.ID {
		t.Errorf("event references order %q/%q, want persisted order %q", evt.OrderID, evt.EventID, res.ID)
	}
	if evt.TotalAmount != 2000 {
		t.Errorf("TotalAmount = %d, want 2000", evt.TotalAmount)
	}
}

func TestCreateOrder_PublishFailureStillReturns201(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	mockPublisher := &mockEventPublisher{publishErr: errors.New("broker down")}
	h := NewOrderHandler(
		usecase.NewCreateOrderUseCase(mockRepo, mockPublisher, zap.NewNop()),
		usecase.NewGetOrderUseCase(mockRepo),
		zap.NewNop(),
	)

	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewBufferString(
		`{"customer_id":"cust-123","items":[{"product_id":"prod-1","quantity":1,"unit_price":100}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "test-key")
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = req
	h.CreateOrder(c)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 even when publication fails: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "broker down") {
		t.Errorf("response leaked publication error: %s", w.Body.String())
	}
}

func TestCreateOrder_PayloadTooLarge(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	handler := NewOrderHandler(
		usecase.NewCreateOrderUseCase(mockRepo, &mockEventPublisher{}, zap.NewNop()),
		usecase.NewGetOrderUseCase(mockRepo),
		zap.NewNop(),
	)

	body := `{"customer_id":"` + strings.Repeat("a", 1024) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "test-key")
	req.Body = http.MaxBytesReader(httptest.NewRecorder(), req.Body, 16)
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = req

	handler.CreateOrder(c)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("Expected status 413, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCreateOrder_InvalidRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	mockPublisher := &mockEventPublisher{}
	createUC := usecase.NewCreateOrderUseCase(mockRepo, mockPublisher, zap.NewNop())
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, logger)

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
	mockPublisher := &mockEventPublisher{}
	createUC := usecase.NewCreateOrderUseCase(mockRepo, mockPublisher, zap.NewNop())
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, logger)

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

	if strings.Contains(w.Body.String(), "database error") {
		t.Errorf("Response leaked internal error details: %s", w.Body.String())
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
			h := NewOrderHandler(usecase.NewCreateOrderUseCase(repo, mockPublisher, zap.NewNop()), usecase.NewGetOrderUseCase(repo), logger)
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
			if tc.want == http.StatusConflict {
				var res httputil.ErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
					t.Fatalf("invalid error payload: %v", err)
				}
				if res.Error.Code != "ORDER_IDEMPOTENCY_CONFLICT" {
					t.Fatalf("error code = %q, want ORDER_IDEMPOTENCY_CONFLICT", res.Error.Code)
				}
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

	mockPublisher := &mockEventPublisher{}
	createUC := usecase.NewCreateOrderUseCase(mockRepo, mockPublisher, zap.NewNop())
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, logger)

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
	mockPublisher := &mockEventPublisher{}
	createUC := usecase.NewCreateOrderUseCase(mockRepo, mockPublisher, zap.NewNop())
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, logger)

	httpReq := httptest.NewRequest(http.MethodGet, "/orders/nonexistent", nil)
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq
	c.Params = gin.Params{{Key: "id", Value: "nonexistent"}}

	handler.GetOrder(c)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}

	var res httputil.ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("Response is not a valid error payload: %v (%s)", err, w.Body.String())
	}

	if res.Error.Code != "ORDER_NOT_FOUND" {
		t.Errorf("Expected ORDER_NOT_FOUND error code, got %q", res.Error.Code)
	}
}

func TestGetOrder_InternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mockRepo := newMockOrderRepository()
	mockRepo.getByIDErr = errors.New("database connection error")
	mockPublisher := &mockEventPublisher{}
	createUC := usecase.NewCreateOrderUseCase(mockRepo, mockPublisher, zap.NewNop())
	getUC := usecase.NewGetOrderUseCase(mockRepo)
	logger := zap.NewNop()
	handler := NewOrderHandler(createUC, getUC, logger)

	httpReq := httptest.NewRequest(http.MethodGet, "/orders/order-123", nil)
	w := httptest.NewRecorder()

	c, _ := gin.CreateTestContext(w)
	c.Request = httpReq
	c.Params = gin.Params{{Key: "id", Value: "order-123"}}

	handler.GetOrder(c)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	if strings.Contains(w.Body.String(), "database connection error") {
		t.Errorf("Response leaked internal error details: %s", w.Body.String())
	}
}
