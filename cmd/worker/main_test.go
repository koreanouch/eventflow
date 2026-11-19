package main

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRetryCountFromHeaders(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers amqp.Table
		want    int32
	}{
		{name: "missing", headers: amqp.Table{}, want: 0},
		{name: "nil table", headers: nil, want: 0},
		{name: "int32", headers: amqp.Table{"x-retry-count": int32(2)}, want: 2},
		{name: "int64", headers: amqp.Table{"x-retry-count": int64(3)}, want: 3},
		{name: "int", headers: amqp.Table{"x-retry-count": 1}, want: 1},
		{name: "float64", headers: amqp.Table{"x-retry-count": float64(4)}, want: 4},
		{name: "negative", headers: amqp.Table{"x-retry-count": int32(-1)}, want: 0},
		{name: "unsupported", headers: amqp.Table{"x-retry-count": "two"}, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryCountFromHeaders(tc.headers); got != tc.want {
				t.Errorf("retryCountFromHeaders(%v) = %d, want %d", tc.headers, got, tc.want)
			}
		})
	}
}

func TestCopyHeadersDoesNotMutateOriginal(t *testing.T) {
	original := amqp.Table{"x-retry-count": int32(1)}
	copied := copyHeaders(original)
	copied["x-retry-count"] = int32(9)
	copied["x-dead-letter-reason"] = "test"

	if original["x-retry-count"] != int32(1) {
		t.Errorf("original header was mutated: %v", original["x-retry-count"])
	}
	if _, ok := original["x-dead-letter-reason"]; ok {
		t.Error("original table gained a key from the copy")
	}
}
