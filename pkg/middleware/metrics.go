package middleware

import (
	"net/http"
	"strconv"
	"time"

	"microservice/pkg/metrics"

	"github.com/gin-gonic/gin"
)

// Metrics registra cantidad de peticiones por método, ruta y estado, además de
// la latencia acumulada. Usa la ruta registrada (no la URL concreta) para
// mantener acotada la cardinalidad de las etiquetas.
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		c.Next()

		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}

		metrics.Inc(metrics.HTTPRequestsTotal, map[string]string{
			"method": c.Request.Method,
			"route":  route,
			"status": strconv.Itoa(c.Writer.Status()),
		})
		metrics.Observe(metrics.HTTPRequestDurationSeconds, map[string]string{
			"method": c.Request.Method,
			"route":  route,
		}, time.Since(start).Seconds())
	}
}

// MetricsHandler expone el registro global en formato texto de Prometheus.
// Debe registrarse antes de los middlewares de negocio para que el scraping no
// contamine las métricas de tráfico de la API.
func MetricsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = metrics.Default().WritePrometheus(c.Writer)
	}
}

// MetricsHTTPHandler expone el registro global como http.Handler. Se usa en
// procesos que no montan Gin, como el servidor de métricas del worker.
func MetricsHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = metrics.Default().WritePrometheus(w)
	})
}
