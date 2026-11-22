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

func TestMetricsHandlerExposesPrometheus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	metrics.Reset()
	defer metrics.Reset()

	metrics.Inc("demo_total", nil)
	metrics.Observe("demo_duration_seconds", map[string]string{"route": "/test"}, 0.5)

	router := gin.New()
	router.GET("/metrics", MetricsHandler())

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want text/plain", ct)
	}
	body := w.Body.String()
	for _, want := range []string{
		"# TYPE demo_total counter",
		"demo_total 1",
		"# TYPE demo_duration_seconds summary",
		`demo_duration_seconds_count{route="/test"} 1`,
		`demo_duration_seconds_sum{route="/test"} 0.5`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("response missing %q:\n%s", want, body)
		}
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
