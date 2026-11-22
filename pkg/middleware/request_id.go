package middleware

import (
	"microservice/pkg/reqid"

	"github.com/gin-gonic/gin"
)

// RequestID garantiza que cada petición tenga un identificador de correlación.
// Respeta X-Request-ID si es válido y, en cualquier caso, lo devuelve en la
// respuesta y lo deja disponible en el contexto de la petición.
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := reqid.Sanitize(c.GetHeader(reqid.Header))
		c.Request = c.Request.WithContext(reqid.WithContext(c.Request.Context(), id))
		c.Header(reqid.Header, id)
		c.Set("request_id", id)
		c.Next()
	}
}
