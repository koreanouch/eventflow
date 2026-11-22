package middleware

import (
	"microservice/pkg/reqid"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// AccessLog registra cada petición con log estructurado e incluye el
// identificador de correlación. Debe registrarse después de RequestID.
func AccessLog(log *zap.Logger) gin.HandlerFunc {
	if log == nil {
		log = zap.NewNop()
	}

	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		log.Info("http request",
			zap.String("request_id", reqid.FromContext(c.Request.Context())),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Int("bytes", c.Writer.Size()),
			zap.Duration("duration", time.Since(start)),
		)
	}
}
