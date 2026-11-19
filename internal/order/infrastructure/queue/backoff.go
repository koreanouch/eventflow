package queue

import "fmt"

// RetryQueueName devuelve el nombre de la cola de reintentos para un intento.
// Cada intento usa una cola distinta con su propio TTL, lo que permite una
// espera creciente sin bloquear mensajes con esperas mayores.
func RetryQueueName(base string, attempt int) string {
	if attempt < 1 {
		attempt = 1
	}
	return fmt.Sprintf("%s.%d", base, attempt)
}

// RetryDelayMs calcula la espera del intento indicado (1-based) con crecimiento
// exponencial desde baseMs, limitada por maxMs.
func RetryDelayMs(baseMs, maxMs, attempt int) int {
	if baseMs <= 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	if maxMs <= 0 || maxMs < baseMs {
		maxMs = baseMs
	}

	delay := baseMs
	for i := 1; i < attempt; i++ {
		if delay >= maxMs/2 {
			return maxMs
		}
		delay *= 2
	}
	if delay > maxMs {
		return maxMs
	}
	return delay
}
