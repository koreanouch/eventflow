package usecase

import (
	"context"
	"errors"
	"testing"
)

type mockHealthRepository struct {
	err error
}

func (m *mockHealthRepository) Ping(ctx context.Context) error {
	return m.err
}

type mockBrokerChecker struct {
	err error
}

func (m *mockBrokerChecker) Ping(ctx context.Context) error {
	return m.err
}

func TestHealthUseCase_AllDependenciesHealthy(t *testing.T) {
	uc := NewHealthUseCase(&mockHealthRepository{}, &mockBrokerChecker{})

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() returned unexpected error: %v", err)
	}
	if res == nil {
		t.Fatal("Execute() returned nil response")
	}
	if res.Service != "microservice" {
		t.Errorf("Service = %q, want microservice", res.Service)
	}
	if res.Timestamp.IsZero() {
		t.Error("Timestamp is zero")
	}
	if res.Status != "ok" {
		t.Errorf("Status = %q, want ok", res.Status)
	}
	if res.Database != "connected" {
		t.Errorf("Database = %q, want connected", res.Database)
	}
	if res.RabbitMQ != "connected" {
		t.Errorf("RabbitMQ = %q, want connected", res.RabbitMQ)
	}
}

func TestHealthUseCase_DatabaseDownReturnsError(t *testing.T) {
	uc := NewHealthUseCase(&mockHealthRepository{err: errors.New("db down")}, &mockBrokerChecker{})

	res, err := uc.Execute(context.Background())
	if err == nil {
		t.Fatal("Execute() expected error when database is down")
	}
	if res == nil {
		t.Fatal("Execute() should still return a response for diagnostics")
	}
	if res.Status != "unhealthy" {
		t.Errorf("Status = %q, want unhealthy", res.Status)
	}
	if res.Database != "disconnected" {
		t.Errorf("Database = %q, want disconnected", res.Database)
	}
}

func TestHealthUseCase_BrokerDownIsDegradedButReady(t *testing.T) {
	uc := NewHealthUseCase(&mockHealthRepository{}, &mockBrokerChecker{err: errors.New("broker down")})

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() must not fail when only the optional broker is down: %v", err)
	}
	if res.Status != "degraded" {
		t.Errorf("Status = %q, want degraded", res.Status)
	}
	if res.RabbitMQ != "disconnected" {
		t.Errorf("RabbitMQ = %q, want disconnected", res.RabbitMQ)
	}
	if res.Database != "connected" {
		t.Errorf("Database = %q, want connected", res.Database)
	}
}

func TestHealthUseCase_NoBrokerConfiguredIsDegraded(t *testing.T) {
	uc := NewHealthUseCase(&mockHealthRepository{}, nil)

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() returned unexpected error: %v", err)
	}
	if res.Status != "degraded" {
		t.Errorf("Status = %q, want degraded", res.Status)
	}
	if res.RabbitMQ != "disconnected" {
		t.Errorf("RabbitMQ = %q, want disconnected", res.RabbitMQ)
	}
}
