package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newTestRouter(maxBodyBytes int64, timeout time.Duration) *gin.Engine {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.Use(Limits(maxBodyBytes, timeout))
	router.POST("/echo", func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.Status(http.StatusRequestEntityTooLarge)
			return
		}
		c.String(http.StatusOK, string(body))
	})
	router.GET("/wait", func(c *gin.Context) {
		select {
		case <-c.Request.Context().Done():
			c.Status(http.StatusGatewayTimeout)
		case <-time.After(time.Second):
			c.Status(http.StatusOK)
		}
	})

	return router
}

func TestLimits_RejectsOversizedBody(t *testing.T) {
	router := newTestRouter(16, time.Second)

	req := httptest.NewRequest(http.MethodPost, "/echo", strings.NewReader(strings.Repeat("a", 64)))
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestLimits_AllowsBodyWithinLimit(t *testing.T) {
	router := newTestRouter(16, time.Second)

	req := httptest.NewRequest(http.MethodPost, "/echo", strings.NewReader("ok"))
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestLimits_AppliesRequestDeadline(t *testing.T) {
	router := newTestRouter(0, 20*time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/wait", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusGatewayTimeout)
	}
}
