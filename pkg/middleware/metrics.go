package middleware

import (
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
