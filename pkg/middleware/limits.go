package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Limits aplica un tamaño máximo al cuerpo de la petición y un deadline al contexto.
// El deadline acota el trabajo del handler (incluidas queries y publicaciones),
// de modo que una dependencia lenta no retenga la petición indefinidamente.
func Limits(maxBodyBytes int64, timeout time.Duration) gin.HandlerFunc {
	return func(c *gin.Context) {
		if maxBodyBytes > 0 && c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes)
		}

		if timeout > 0 {
			ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
			defer cancel()
			c.Request = c.Request.WithContext(ctx)
		}

		c.Next()
	}
}
