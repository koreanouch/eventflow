package queue

import "testing"

func TestRetryQueueName(t *testing.T) {
	for _, tc := range []struct {
		attempt int
		want    string
	}{
		{attempt: 0, want: "orders.retry.1"},
		{attempt: 1, want: "orders.retry.1"},
		{attempt: 3, want: "orders.retry.3"},
	} {
		if got := RetryQueueName("orders.retry", tc.attempt); got != tc.want {
			t.Errorf("RetryQueueName(attempt=%d) = %q, want %q", tc.attempt, got, tc.want)
		}
	}
}

func TestRetryDelayMs(t *testing.T) {
	for _, tc := range []struct {
		base, max, attempt int
		want               int
	}{
		{base: 5000, max: 60000, attempt: 1, want: 5000},
		{base: 5000, max: 60000, attempt: 2, want: 10000},
		{base: 5000, max: 60000, attempt: 3, want: 20000},
		{base: 5000, max: 20000, attempt: 4, want: 20000},
		{base: 0, max: 60000, attempt: 1, want: 0},
		{base: 1000, max: 0, attempt: 3, want: 1000},
		{base: 1000, max: 100, attempt: 2, want: 1000},
	} {
		if got := RetryDelayMs(tc.base, tc.max, tc.attempt); got != tc.want {
			t.Errorf("RetryDelayMs(base=%d,max=%d,attempt=%d) = %d, want %d",
				tc.base, tc.max, tc.attempt, got, tc.want)
		}
	}
}
