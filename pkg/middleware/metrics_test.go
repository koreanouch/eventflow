package middleware

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"microservice/pkg/metrics"

	"github.com/gin-gonic/gin"
)

func TestMetricsRecordsRequestsAndLatency(t *testing.T) {
	gin.SetMode(gin.TestMode)
	metrics.Reset()
	defer metrics.Reset()

	router := gin.New()
	router.Use(Metrics())
	router.GET("/orders/:id", func(c *gin.Context) {
		c.Status(http.StatusCreated)
	})

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/orders/abc", nil))

	want := map[string]string{"method": http.MethodGet, "route": "/orders/:id", "status": strconv.Itoa(http.StatusCreated)}
	if got := metrics.CounterValue(metrics.HTTPRequestsTotal, want); got != 1 {
		t.Errorf("http_requests_total = %d, want 1", got)
	}

	var out strings.Builder
	if err := metrics.Default().WritePrometheus(&out); err != nil {
		t.Fatalf("WritePrometheus() error: %v", err)
	}
	if !strings.Contains(out.String(), `http_request_duration_seconds_count{method="GET",route="/orders/:id"} 1`) {
		t.Errorf("latency observation missing:\n%s", out.String())
	}
}

func TestMetricsUsesUnmatchedForUnknownRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	metrics.Reset()
	defer metrics.Reset()

	router := gin.New()
	router.Use(Metrics())

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/does-not-exist", nil))

	want := map[string]string{"method": http.MethodGet, "route": "unmatched", "status": strconv.Itoa(http.StatusNotFound)}
	if got := metrics.CounterValue(metrics.HTTPRequestsTotal, want); got != 1 {
		t.Errorf("http_requests_total for unmatched route = %d, want 1", got)
	}
}
