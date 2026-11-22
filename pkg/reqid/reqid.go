package reqid

import (
	"context"
	"strings"

	"github.com/google/uuid"
)

// Header es la cabecera HTTP y AMQP usada para propagar la correlación.
const Header = "X-Request-ID"

type contextKey struct{}

// New genera un identificador de correlación.
func New() string {
	return uuid.NewString()
}

// WithContext adjunta el identificador al contexto.
func WithContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// FromContext recupera el identificador o devuelve "" si no existe.
func FromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(contextKey{}).(string)
	return id
}

// Sanitize valida un identificador recibido del cliente. Solo acepta valores
// cortos y con caracteres seguros para logs y cabeceras; si no es válido,
// genera uno nuevo.
func Sanitize(id string) string {
	id = strings.TrimSpace(id)
	if id == "" || len(id) > 128 {
		return New()
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.', r == ':':
		default:
			return New()
		}
	}
	return id
}
