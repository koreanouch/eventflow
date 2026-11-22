package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"microservice/internal/health/application/dto"
	"microservice/internal/health/application/usecase"

	"github.com/gin-gonic/gin"
)

type fakeHealthRepo struct {
	err error
}

func (f *fakeHealthRepo) Ping(ctx context.Context) error { return f.err }

type fakeBroker struct {
	err error
}

func (f *fakeBroker) Ping(ctx context.Context) error { return f.err }

func TestHealth_Returns200WhenReady(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHealthHandler(usecase.NewHealthUseCase(&fakeHealthRepo{}, &fakeBroker{}))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)

	h.Health(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var res dto.HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("invalid response: %v", err)
	}
	if res.Status != "ok" || res.Database != "connected" || res.RabbitMQ != "connected" {
		t.Errorf("unexpected health payload: %+v", res)
	}
}

func TestHealth_Returns200DegradedWhenBrokerDown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHealthHandler(usecase.NewHealthUseCase(&fakeHealthRepo{}, &fakeBroker{err: errors.New("down")}))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)

	h.Health(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 in degraded mode", w.Code)
	}
	var res dto.HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("invalid response: %v", err)
	}
	if res.Status != "degraded" {
		t.Errorf("status = %q, want degraded", res.Status)
	}
}

func TestHealth_Returns503WhenDatabaseDown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHealthHandler(usecase.NewHealthUseCase(&fakeHealthRepo{err: errors.New("db down")}, nil))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)

	h.Health(c)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", w.Code)
	}
	if !json.Valid(w.Body.Bytes()) {
		t.Errorf("response is not valid JSON: %s", w.Body.String())
	}
}
