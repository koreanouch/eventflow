package infrastructure

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"microservice/internal/order/application/dto"
	"microservice/internal/order/application/usecase"
	"microservice/internal/order/domain/event"
	"microservice/internal/order/presentation/handler"
	"microservice/pkg/database"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type postResult struct {
	status int
	body   string
	order  dto.CreateOrderResponse
}

func orderTestRouter(db database.Database) *gin.Engine {
	repo := NewPostgresOrderRepository(db)
	mockPublisher := &mockEventPublisher{}
	logger := zap.NewNop()
	h := handler.NewOrderHandler(usecase.NewCreateOrderUseCase(repo, mockPublisher, logger), usecase.NewGetOrderUseCase(repo), logger)
	r := gin.New()
	r.POST("/orders", h.CreateOrder)
	r.GET("/orders/:id", h.GetOrder)
	return r
}

type mockEventPublisher struct{}

func (m *mockEventPublisher) PublishOrderCreated(ctx context.Context, evt *event.OrderCreated) error {
	return nil
}

func postOrder(r *gin.Engine, key, body string) postResult {
	req := httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	result := postResult{status: w.Code, body: w.Body.String()}
	if w.Code == http.StatusCreated {
		if err := json.Unmarshal(w.Body.Bytes(), &result.order); err != nil {
			result.body = fmt.Sprintf("invalid response JSON: %v", err)
			result.status = 0
		}
	}
	return result
}

func cleanIdempotencyKeys(t *testing.T, db database.Database, keys ...string) {
	t.Helper()
	t.Cleanup(func() {
		pool := db.Native().(*pgxpool.Pool)
		for _, key := range keys {
			if _, err := pool.Exec(context.Background(), "DELETE FROM orders WHERE idempotency_key = $1", key); err != nil {
				t.Errorf("cleanup idempotency key: %v", err)
			}
		}
	})
}

func countIdempotentOrder(t *testing.T, db database.Database, key string, wantOrders, wantItems int) {
	t.Helper()
	pool := db.Native().(*pgxpool.Pool)
	var orders, items int
	err := pool.QueryRow(context.Background(), `
		SELECT count(DISTINCT o.id), count(i.id)
		FROM orders o LEFT JOIN order_items i ON i.order_id = o.id
		WHERE o.idempotency_key = $1`, key).Scan(&orders, &items)
	if err != nil {
		t.Fatal(err)
	}
	if orders != wantOrders || items != wantItems {
		t.Fatalf("key %q: got %d orders and %d items, want %d and %d", key, orders, items, wantOrders, wantItems)
	}
}

const orderPayload = `{"customer_id":"customer","items":[{"product_id":"one","quantity":2,"unit_price":100},{"product_id":"two","quantity":1,"unit_price":300}]}`
const differentOrderPayload = `{"customer_id":"customer","items":[{"product_id":"one","quantity":3,"unit_price":100},{"product_id":"two","quantity":1,"unit_price":300}]}`

func TestOrderIdempotencyHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := setupTestDB(t)
	t.Cleanup(db.Close)
	key, otherKey := uuid.NewString(), uuid.NewString()
	cleanIdempotencyKeys(t, db, key, otherKey)
	r := orderTestRouter(db)

	first := postOrder(r, key, orderPayload)
	replay := postOrder(r, key, `{"items":[{"unit_price":100,"quantity":2,"product_id":"one"},{"unit_price":300,"product_id":"two","quantity":1}],"customer_id":"customer"}`)
	other := postOrder(r, otherKey, orderPayload)
	conflict := postOrder(r, key, differentOrderPayload)
	for _, result := range []postResult{first, replay, other} {
		if result.status != http.StatusCreated {
			t.Fatalf("POST status %d: %s", result.status, result.body)
		}
	}
	if first.order.ID == "" || first.order.CreatedAt.IsZero() || first.order.ID != replay.order.ID || !first.order.CreatedAt.Equal(replay.order.CreatedAt) {
		t.Fatalf("replay differs: first=%+v replay=%+v", first.order, replay.order)
	}
	if first.order.ID == other.order.ID {
		t.Fatal("different keys returned the same order")
	}
	if conflict.status != http.StatusConflict {
		t.Fatalf("conflict status %d: %s", conflict.status, conflict.body)
	}
	reordered := postOrder(r, key, `{"customer_id":"customer","items":[{"product_id":"two","quantity":1,"unit_price":300},{"product_id":"one","quantity":2,"unit_price":100}]}`)
	if reordered.status != http.StatusConflict {
		t.Fatalf("reordered items status %d: %s", reordered.status, reordered.body)
	}
	countIdempotentOrder(t, db, key, 1, 2)
	countIdempotentOrder(t, db, otherKey, 1, 2)

	// A new repository instance must return the persisted response after a restart.
	replay = postOrder(orderTestRouter(db), key, orderPayload)
	if replay.status != http.StatusCreated || replay.order.ID != first.order.ID || !replay.order.CreatedAt.Equal(first.order.CreatedAt) {
		t.Fatalf("replay with new repository: %+v", replay)
	}
	get := httptest.NewRecorder()
	r.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/orders/"+first.order.ID, nil))
	var fetched dto.GetOrderResponse
	if get.Code != http.StatusOK {
		t.Fatalf("GET status %d: %s", get.Code, get.Body.String())
	}
	if err := json.NewDecoder(bytes.NewReader(get.Body.Bytes())).Decode(&fetched); err != nil {
		t.Fatal(err)
	}
	if !fetched.CreatedAt.Equal(first.order.CreatedAt) {
		t.Fatalf("GET timestamp %v != persisted POST timestamp %v", fetched.CreatedAt, first.order.CreatedAt)
	}
}

func TestOrderIdempotencyConcurrent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("mixed=%t", mixed), func(t *testing.T) {
			db := setupTestDB(t)
			t.Cleanup(db.Close)
			db2 := setupTestDB(t)
			t.Cleanup(db2.Close)
			key := uuid.NewString()
			cleanIdempotencyKeys(t, db, key)
			routers := []*gin.Engine{orderTestRouter(db), orderTestRouter(db2)}
			const n = 24
			results := make([]postResult, n)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range results {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					body := orderPayload
					if mixed && i%2 == 1 {
						body = differentOrderPayload
					}
					<-start
					results[i] = postOrder(routers[(i/2)%2], key, body)
				}(i)
			}
			close(start)
			wg.Wait()
			var winner postResult
			winnerParity := -1
			for i, result := range results {
				if result.status == http.StatusCreated {
					if winnerParity == -1 {
						winner, winnerParity = result, i%2
					}
					if mixed && i%2 != winnerParity {
						t.Fatalf("different contents both succeeded: %+v", result)
					}
					if result.order.ID != winner.order.ID || !result.order.CreatedAt.Equal(winner.order.CreatedAt) {
						t.Fatalf("inconsistent successful replay: %+v vs %+v", result.order, winner.order)
					}
				} else if result.status != http.StatusConflict {
					t.Fatalf("unexpected status %d: %s", result.status, result.body)
				}
			}
			if winnerParity == -1 {
				t.Fatal("no successful request")
			}
			for i, result := range results {
				want := http.StatusCreated
				if mixed && i%2 != winnerParity {
					want = http.StatusConflict
				}
				if result.status != want {
					t.Fatalf("request %d: status %d, want %d", i, result.status, want)
				}
			}
			countIdempotentOrder(t, db, key, 1, 2)
			get := httptest.NewRecorder()
			routers[0].ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/orders/"+winner.order.ID, nil))
			var fetched dto.GetOrderResponse
			if get.Code != http.StatusOK {
				t.Fatalf("GET status %d: %s", get.Code, get.Body.String())
			}
			if err := json.Unmarshal(get.Body.Bytes(), &fetched); err != nil {
				t.Fatal(err)
			}
			wantQuantity := int64(2)
			if mixed && winnerParity == 1 {
				wantQuantity = 3
			}
			if len(fetched.Items) != 2 || fetched.Items[0].ProductID != "one" ||
				fetched.Items[0].Quantity != wantQuantity || fetched.Items[1].ProductID != "two" {
				t.Fatalf("persisted items do not match winning request: %+v", fetched.Items)
			}
		})
	}
}

func TestOrderIdempotencyRollsBackItems(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := setupTestDB(t)
	t.Cleanup(db.Close)
	key := uuid.NewString()
	cleanIdempotencyKeys(t, db, key)
	r := orderTestRouter(db)
	body := fmt.Sprintf(`{"customer_id":"customer","items":[{"product_id":"first","quantity":1,"unit_price":1},{"product_id":%q,"quantity":1,"unit_price":1}]}`, strings.Repeat("x", 37))
	result := postOrder(r, key, body)
	if result.status != http.StatusInternalServerError {
		t.Fatalf("expected item insert failure, got %d: %s", result.status, result.body)
	}
	countIdempotentOrder(t, db, key, 0, 0)
	result = postOrder(r, key, orderPayload)
	if result.status != http.StatusCreated {
		t.Fatalf("retry after rollback: status %d: %s", result.status, result.body)
	}
	countIdempotentOrder(t, db, key, 1, 2)
}
