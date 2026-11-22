package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"microservice/pkg/reqid"

	"github.com/gin-gonic/gin"
)

func TestRequestIDPropagatesProvidedValue(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestID())
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, reqid.FromContext(c.Request.Context()))
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set(reqid.Header, "corr-123")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != "corr-123" {
		t.Errorf("context request id = %q, want corr-123", w.Body.String())
	}
	if got := w.Header().Get(reqid.Header); got != "corr-123" {
		t.Errorf("response header = %q, want corr-123", got)
	}
}

func TestRequestIDGeneratesWhenMissingOrInvalid(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestID())
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, reqid.FromContext(c.Request.Context()))
	})

	for _, header := range []string{"", "invalid value with spaces"} {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		if header != "" {
			req.Header.Set(reqid.Header, header)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		id := w.Header().Get(reqid.Header)
		if id == "" {
			t.Fatal("response header X-Request-ID is empty")
		}
		if id == header {
			t.Errorf("invalid header %q was propagated unchanged", header)
		}
		if w.Body.String() != id {
			t.Errorf("context id %q != response header %q", w.Body.String(), id)
		}
	}
}
