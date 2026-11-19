package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"microservice/internal/order/application/dto"
	"microservice/internal/order/domain/entity"
	"microservice/internal/order/domain/event"
	"microservice/internal/order/domain/repository"
)

// Mock repository for testing
type mockOrderRepository struct {
	saveErr    error
	getByIDErr error
	orders     map[string]*entity.Order
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

type mockEventPublisher struct {
	published []*event.OrderCreated
	err       error
}

func (m *mockEventPublisher) PublishOrderCreated(ctx context.Context, evt *event.OrderCreated) error {
	m.published = append(m.published, evt)
	return m.err
}

func validCreateOrderRequest() *dto.CreateOrderRequest {
	return &dto.CreateOrderRequest{
		CustomerID: "cust-123",
		Items: []dto.OrderItemDTO{
			{ProductID: "prod-1", Quantity: 2, UnitPrice: 1000},
			{ProductID: "prod-2", Quantity: 1, UnitPrice: 500},
		},
	}
}

func TestCreateOrderUseCase_Execute_Success(t *testing.T) {
	mockRepo := newMockOrderRepository()
	uc := NewCreateOrderUseCase(mockRepo, nil, nil)

	req := &dto.CreateOrderRequest{
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

	res, err := uc.Execute(context.Background(), req, "test-key")

	if err != nil {
		t.Fatalf("Execute() returned unexpected error: %v", err)
	}

	if res == nil {
		t.Fatalf("Execute() returned nil response")
	}

	if res.ID == "" {
		t.Errorf("Execute().ID is empty, expected a valid UUID")
	}

	if res.CreatedAt.IsZero() {
		t.Errorf("Execute().CreatedAt is zero, expected a valid timestamp")
	}

	// Verify order was saved in repository
	saved, err := mockRepo.GetByID(context.Background(), res.ID)
	if err != nil {
		t.Fatalf("Failed to retrieve saved order: %v", err)
	}

	if saved == nil {
		t.Fatalf("Order was not saved to repository")
	}

	if saved.CustomerID() != "cust-123" {
		t.Errorf("Saved order CustomerID = %q, want %q", saved.CustomerID(), "cust-123")
	}

	if saved.TotalAmount() != 2500 {
		t.Errorf("Saved order TotalAmount = %d, want %d", saved.TotalAmount(), 2500)
	}

	if len(saved.Items()) != 2 {
		t.Errorf("Saved order has %d items, want 2", len(saved.Items()))
	}
}

func TestCreateOrderUseCase_Execute_NilRequest(t *testing.T) {
	mockRepo := newMockOrderRepository()
	uc := NewCreateOrderUseCase(mockRepo, nil, nil)

	res, err := uc.Execute(context.Background(), nil, "test-key")

	if err == nil {
		t.Fatalf("Execute() expected error for nil request")
	}

	if res != nil {
		t.Fatalf("Execute() expected nil response for invalid request")
	}
}

func TestCreateOrderUseCase_RequiresIdempotencyKey(t *testing.T) {
	uc := NewCreateOrderUseCase(newMockOrderRepository(), nil, nil)
	req := &dto.CreateOrderRequest{
		CustomerID: "cust",
		Items:      []dto.OrderItemDTO{{ProductID: "prod", Quantity: 1, UnitPrice: 1}},
	}
	for _, key := range []string{"", "   "} {
		if _, err := uc.Execute(context.Background(), req, key); err == nil {
			t.Errorf("expected error for key %q", key)
		}
	}
}

func TestCreateOrderUseCase_Execute_EmptyCustomerID(t *testing.T) {
	mockRepo := newMockOrderRepository()
	uc := NewCreateOrderUseCase(mockRepo, nil, nil)

	req := &dto.CreateOrderRequest{
		CustomerID: "",
		Items: []dto.OrderItemDTO{
			{
				ProductID: "prod-1",
				Quantity:  1,
				UnitPrice: 1000,
			},
		},
	}

	res, err := uc.Execute(context.Background(), req, "test-key")

	if err == nil {
		t.Fatalf("Execute() expected error for empty customer id")
	}

	if res != nil {
		t.Fatalf("Execute() expected nil response for invalid request")
	}
}

func TestCreateOrderUseCase_Execute_NoItems(t *testing.T) {
	mockRepo := newMockOrderRepository()
	uc := NewCreateOrderUseCase(mockRepo, nil, nil)

	req := &dto.CreateOrderRequest{
		CustomerID: "cust-123",
		Items:      []dto.OrderItemDTO{},
	}

	res, err := uc.Execute(context.Background(), req, "test-key")

	if err == nil {
		t.Fatalf("Execute() expected error for empty items")
	}

	if res != nil {
		t.Fatalf("Execute() expected nil response for invalid request")
	}
}

func TestCreateOrderUseCase_Execute_InvalidItem(t *testing.T) {
	mockRepo := newMockOrderRepository()
	uc := NewCreateOrderUseCase(mockRepo, nil, nil)

	req := &dto.CreateOrderRequest{
		CustomerID: "cust-123",
		Items: []dto.OrderItemDTO{
			{
				ProductID: "",
				Quantity:  1,
				UnitPrice: 1000,
			},
		},
	}

	res, err := uc.Execute(context.Background(), req, "test-key")

	if err == nil {
		t.Fatalf("Execute() expected error for invalid item (empty product id)")
	}

	if res != nil {
		t.Fatalf("Execute() expected nil response for invalid item")
	}
}

func TestCreateOrderUseCase_Execute_RepositoryError(t *testing.T) {
	mockRepo := newMockOrderRepository()
	mockRepo.saveErr = errors.New("database error")

	uc := NewCreateOrderUseCase(mockRepo, nil, nil)

	req := &dto.CreateOrderRequest{
		CustomerID: "cust-123",
		Items: []dto.OrderItemDTO{
			{
				ProductID: "prod-1",
				Quantity:  1,
				UnitPrice: 1000,
			},
		},
	}

	res, err := uc.Execute(context.Background(), req, "test-key")

	if !errors.Is(err, mockRepo.saveErr) {
		t.Fatalf("Execute() error = %v, want wrapped repository error", err)
	}

	if res != nil {
		t.Fatalf("Execute() expected nil response on repository error")
	}
}

func TestCreateOrderUseCase_PublishesEventFromDomainAggregate(t *testing.T) {
	mockRepo := newMockOrderRepository()
	mockPublisher := &mockEventPublisher{}
	uc := NewCreateOrderUseCase(mockRepo, mockPublisher, nil)

	res, err := uc.Execute(context.Background(), validCreateOrderRequest(), "test-key")
	if err != nil {
		t.Fatalf("Execute() returned unexpected error: %v", err)
	}

	if len(mockPublisher.published) != 1 {
		t.Fatalf("published %d events, want 1", len(mockPublisher.published))
	}

	evt := mockPublisher.published[0]
	if evt.EventID != "order.created."+res.ID {
		t.Errorf("EventID = %q, want event tied to persisted order %q", evt.EventID, res.ID)
	}
	if evt.EventType != "order.created" {
		t.Errorf("EventType = %q, want order.created", evt.EventType)
	}
	if evt.OrderID != res.ID {
		t.Errorf("OrderID = %q, want persisted ID %q", evt.OrderID, res.ID)
	}
	if evt.CustomerID != "cust-123" {
		t.Errorf("CustomerID = %q, want cust-123", evt.CustomerID)
	}
	if evt.TotalAmount != 2500 {
		t.Errorf("TotalAmount = %d, want 2500 (domain total)", evt.TotalAmount)
	}
	if evt.Timestamp.IsZero() {
		t.Error("Timestamp is zero")
	}
}

func TestCreateOrderUseCase_PublishFailureDoesNotFailRequest(t *testing.T) {
	mockRepo := newMockOrderRepository()
	mockPublisher := &mockEventPublisher{err: errors.New("broker down")}
	uc := NewCreateOrderUseCase(mockRepo, mockPublisher, nil)

	res, err := uc.Execute(context.Background(), validCreateOrderRequest(), "test-key")
	if err != nil {
		t.Fatalf("Execute() must not fail when only publication fails: %v", err)
	}
	if res == nil {
		t.Fatal("Execute() returned nil response")
	}
	if _, err := mockRepo.GetByID(context.Background(), res.ID); err != nil {
		t.Fatalf("order was not persisted: %v", err)
	}
}

func TestCreateOrderUseCase_DoesNotPublishWhenPersistenceFails(t *testing.T) {
	mockRepo := newMockOrderRepository()
	mockRepo.saveErr = errors.New("database error")
	mockPublisher := &mockEventPublisher{}
	uc := NewCreateOrderUseCase(mockRepo, mockPublisher, nil)

	if _, err := uc.Execute(context.Background(), validCreateOrderRequest(), "test-key"); err == nil {
		t.Fatal("Execute() expected error when persistence fails")
	}
	if len(mockPublisher.published) != 0 {
		t.Fatalf("published %d events after failed persistence, want 0", len(mockPublisher.published))
	}
}

func TestCreateOrderUseCase_ReplayEventReferencesPersistedOrder(t *testing.T) {
	persisted := repository.CreatedOrder{
		ID:        "existing-order",
		Status:    "pending",
		CreatedAt: time.Now().UTC(),
	}
	repo := &replayOrderRepository{persisted: persisted}
	mockPublisher := &mockEventPublisher{}
	uc := NewCreateOrderUseCase(repo, mockPublisher, nil)

	res, err := uc.Execute(context.Background(), validCreateOrderRequest(), "replayed-key")
	if err != nil {
		t.Fatalf("Execute() returned unexpected error: %v", err)
	}
	if res.ID != "existing-order" {
		t.Fatalf("response ID = %q, want persisted order", res.ID)
	}
	if len(mockPublisher.published) != 1 {
		t.Fatalf("published %d events, want 1", len(mockPublisher.published))
	}
	if got := mockPublisher.published[0].OrderID; got != "existing-order" {
		t.Errorf("event OrderID = %q, want persisted order ID", got)
	}
	if got := mockPublisher.published[0].EventID; got != "order.created.existing-order" {
		t.Errorf("event EventID = %q, want stable ID of the persisted order", got)
	}
}

type replayOrderRepository struct {
	persisted repository.CreatedOrder
}

func (r *replayOrderRepository) Save(ctx context.Context, order *entity.Order) error { return nil }

func (r *replayOrderRepository) SaveIdempotent(ctx context.Context, order *entity.Order, key string, hash [32]byte) (repository.CreatedOrder, error) {
	return r.persisted, nil
}

func (r *replayOrderRepository) GetByID(ctx context.Context, id string) (*entity.Order, error) {
	return nil, nil
}

func TestGetOrderUseCase_Execute_Success(t *testing.T) {
	mockRepo := newMockOrderRepository()

	// Create and save an order
	item, _ := entity.NewOrderItem("prod-1", 2, 1000)
	order, _ := entity.NewOrder("order-123", "cust-456", []entity.OrderItem{item})
	_ = mockRepo.Save(context.Background(), order)

	uc := NewGetOrderUseCase(mockRepo)

	res, err := uc.Execute(context.Background(), "order-123")

	if err != nil {
		t.Fatalf("Execute() returned unexpected error: %v", err)
	}

	if res == nil {
		t.Fatalf("Execute() returned nil response")
	}

	if res.ID != "order-123" {
		t.Errorf("Execute().ID = %q, want %q", res.ID, "order-123")
	}

	if res.CustomerID != "cust-456" {
		t.Errorf("Execute().CustomerID = %q, want %q", res.CustomerID, "cust-456")
	}

	if res.TotalAmount != 2000 {
		t.Errorf("Execute().TotalAmount = %d, want %d", res.TotalAmount, 2000)
	}

	if res.Status != "pending" {
		t.Errorf("Execute().Status = %q, want %q", res.Status, "pending")
	}

	if len(res.Items) != 1 {
		t.Errorf("Execute().Items has %d items, want 1", len(res.Items))
	}

	if res.Items[0].ProductID != "prod-1" {
		t.Errorf("Execute().Items[0].ProductID = %q, want %q", res.Items[0].ProductID, "prod-1")
	}

	if !res.CreatedAt.Before(time.Now().Add(1 * time.Second)) {
		t.Errorf("Execute().CreatedAt seems invalid")
	}
}

func TestGetOrderUseCase_Execute_EmptyOrderID(t *testing.T) {
	mockRepo := newMockOrderRepository()
	uc := NewGetOrderUseCase(mockRepo)

	res, err := uc.Execute(context.Background(), "")

	if err == nil {
		t.Fatalf("Execute() expected error for empty order id")
	}

	if res != nil {
		t.Fatalf("Execute() expected nil response for invalid request")
	}
}

func TestGetOrderUseCase_Execute_OrderNotFound(t *testing.T) {
	mockRepo := newMockOrderRepository()
	uc := NewGetOrderUseCase(mockRepo)

	res, err := uc.Execute(context.Background(), "nonexistent-id")

	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("Execute() error = %v, want ErrOrderNotFound", err)
	}

	if res != nil {
		t.Fatalf("Execute() expected nil response when order not found")
	}
}

func TestGetOrderUseCase_Execute_RepositoryError(t *testing.T) {
	mockRepo := newMockOrderRepository()
	mockRepo.getByIDErr = errors.New("database connection error")

	uc := NewGetOrderUseCase(mockRepo)

	res, err := uc.Execute(context.Background(), "order-123")

	if !errors.Is(err, mockRepo.getByIDErr) {
		t.Fatalf("Execute() error = %v, want wrapped repository error", err)
	}
	if errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("Execute() returned ErrOrderNotFound for a repository error")
	}

	if res != nil {
		t.Fatalf("Execute() expected nil response on repository error")
	}
}

// Verify mock implements interface
var _ repository.OrderRepository = (*mockOrderRepository)(nil)
