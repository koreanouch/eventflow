package httputil

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"microservice/pkg/apperrors"
)

func decodeError(t *testing.T, body []byte) ErrorResponse {
	t.Helper()

	var res ErrorResponse
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("invalid error payload: %v (%s)", err, body)
	}
	return res
}

func TestMapErrorToHttp_WrappedDomainError(t *testing.T) {
	err := fmt.Errorf("saving order: %w", apperrors.NewConflictError("ORDER_IDEMPOTENCY_CONFLICT", "key already used"))

	status, res := MapErrorToHttp(err)

	if status != http.StatusConflict {
		t.Fatalf("status = %d, want %d", status, http.StatusConflict)
	}
	if res.Error.Code != "ORDER_IDEMPOTENCY_CONFLICT" {
		t.Fatalf("code = %q, want ORDER_IDEMPOTENCY_CONFLICT", res.Error.Code)
	}
	if res.Error.Type != string(apperrors.ErrorTypeConflict) {
		t.Fatalf("type = %q, want %q", res.Error.Type, apperrors.ErrorTypeConflict)
	}
}

func TestMapErrorToHttp_Validation(t *testing.T) {
	_, res := MapErrorToHttp(apperrors.NewValidationError("ORDER_INVALID", "quantity must be greater than 0"))

	if res.Error.Type != string(apperrors.ErrorTypeValidation) {
		t.Fatalf("type = %q, want %q", res.Error.Type, apperrors.ErrorTypeValidation)
	}
}

func TestMapErrorToHttp_PayloadTooLarge(t *testing.T) {
	status, res := MapErrorToHttp(apperrors.NewPayloadTooLargeError("REQUEST_TOO_LARGE", "request body is too large"))

	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", status, http.StatusRequestEntityTooLarge)
	}
	if res.Error.Code != "REQUEST_TOO_LARGE" {
		t.Fatalf("code = %q, want REQUEST_TOO_LARGE", res.Error.Code)
	}
}

func TestMapErrorToHttp_UnknownErrorDoesNotLeakDetails(t *testing.T) {
	internal := errors.New(`failed to query order: ERROR: relation "orders" does not exist (SQLSTATE 42P01)`)

	status, res := MapErrorToHttp(fmt.Errorf("handler: %w", internal))

	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", status)
	}
	if res.Error.Code != "INTERNAL_001" {
		t.Fatalf("code = %q, want INTERNAL_001", res.Error.Code)
	}
	if res.Error.Message != "Internal server error" {
		t.Fatalf("message = %q, want generic message", res.Error.Message)
	}

	encoded, _ := json.Marshal(res)
	for _, leak := range []string{"SQLSTATE", "orders", "relation", internal.Error()} {
		if strings.Contains(string(encoded), leak) {
			t.Fatalf("response leaked internal detail %q: %s", leak, encoded)
		}
	}
}
