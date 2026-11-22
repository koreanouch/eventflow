// Package metrics implementa un registro en memoria de contadores y resúmenes
// con exposición en formato Prometheus. Es deliberadamente mínimo: permite
// observar el comportamiento del servicio en local sin agregar dependencias ni
// un sistema de métricas distribuido.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Nombres de las métricas expuestas. Se centralizan para evitar diferencias
// tipográficas entre los distintos paquetes que las registran.
const (
	HTTPRequestsTotal               = "http_requests_total"
	HTTPRequestDurationSeconds      = "http_request_duration_seconds"
	OrdersCreatedTotal              = "orders_created_total"
	OrdersIdempotentReplaysTotal    = "orders_idempotent_replays_total"
	OrderEventsPublishedTotal       = "order_events_published_total"
	OrderEventPublishFailuresTotal  = "order_event_publish_failures_total"
	OrderEventPublishSkippedTotal   = "order_event_publish_skipped_total"
	WorkerMessagesProcessedTotal    = "worker_messages_processed_total"
	WorkerMessagesFailedTotal       = "worker_messages_failed_total"
	WorkerMessagesInvalidTotal      = "worker_messages_invalid_total"
	WorkerMessagesRetriedTotal      = "worker_messages_retried_total"
	WorkerMessagesDeadLetteredTotal = "worker_messages_dead_lettered_total"
	WorkerPublishFailuresTotal      = "worker_publish_failures_total"
	WorkerProcessingDurationSeconds = "worker_processing_duration_seconds"
)

// Registry acumula series identificadas por nombre y etiquetas. Es seguro para
// uso concurrente.
type Registry struct {
	mu        sync.Mutex
	counters  map[string]*counter
	summaries map[string]*summary
}

type counter struct {
	name   string
	labels map[string]string
	value  uint64
}

type summary struct {
	name   string
	labels map[string]string
	sum    float64
	count  uint64
}

var defaultRegistry = NewRegistry()

// NewRegistry crea un registro vacío.
func NewRegistry() *Registry {
	return &Registry{
		counters:  make(map[string]*counter),
		summaries: make(map[string]*summary),
	}
}

// Default devuelve el registro global del proceso.
func Default() *Registry {
	return defaultRegistry
}

// Inc incrementa en uno un contador.
func Inc(name string, labels map[string]string) {
	defaultRegistry.Inc(name, labels)
}

// Observe registra una observación en un resumen (suma y cantidad).
func Observe(name string, labels map[string]string, value float64) {
	defaultRegistry.Observe(name, labels, value)
}

// CounterValue devuelve el valor actual de una serie. Pensado para pruebas y
// diagnósticos.
func CounterValue(name string, labels map[string]string) uint64 {
	return defaultRegistry.CounterValue(name, labels)
}

// Reset vacía el registro global. Se usa en pruebas para aislar escenarios.
func Reset() {
	defaultRegistry.Reset()
}

// Inc incrementa en uno un contador.
func (r *Registry) Inc(name string, labels map[string]string) {
	if name == "" {
		return
	}
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.counters[key]
	if !ok {
		entry = &counter{name: name, labels: copyLabels(labels)}
		r.counters[key] = entry
	}
	entry.value++
}

// Observe agrega una observación a un resumen.
func (r *Registry) Observe(name string, labels map[string]string, value float64) {
	if name == "" {
		return
	}
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.summaries[key]
	if !ok {
		entry = &summary{name: name, labels: copyLabels(labels)}
		r.summaries[key] = entry
	}
	entry.sum += value
	entry.count++
}

// CounterValue devuelve el valor actual de una serie de contador.
func (r *Registry) CounterValue(name string, labels map[string]string) uint64 {
	key := seriesKey(name, labels)

	r.mu.Lock()
	defer r.mu.Unlock()

	if entry, ok := r.counters[key]; ok {
		return entry.value
	}
	return 0
}

// Reset vacía el registro.
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.counters = make(map[string]*counter)
	r.summaries = make(map[string]*summary)
}

// WritePrometheus escribe todas las series en formato texto de Prometheus.
func (r *Registry) WritePrometheus(w io.Writer) error {
	r.mu.Lock()
	counters := make([]counter, 0, len(r.counters))
	for _, entry := range r.counters {
		counters = append(counters, *entry)
	}
	summaries := make([]summary, 0, len(r.summaries))
	for _, entry := range r.summaries {
		summaries = append(summaries, *entry)
	}
	r.mu.Unlock()

	sort.Slice(counters, func(i, j int) bool {
		if counters[i].name != counters[j].name {
			return counters[i].name < counters[j].name
		}
		return formatLabels(counters[i].labels) < formatLabels(counters[j].labels)
	})

	for i, entry := range counters {
		if i == 0 || counters[i-1].name != entry.name {
			if _, err := fmt.Fprintf(w, "# TYPE %s counter\n", entry.name); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "%s%s %d\n", entry.name, renderLabels(entry.labels), entry.value); err != nil {
			return err
		}
	}

	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].name != summaries[j].name {
			return summaries[i].name < summaries[j].name
		}
		return formatLabels(summaries[i].labels) < formatLabels(summaries[j].labels)
	})

	for i, entry := range summaries {
		if i == 0 || summaries[i-1].name != entry.name {
			if _, err := fmt.Fprintf(w, "# TYPE %s summary\n", entry.name); err != nil {
				return err
			}
		}
		rendered := renderLabels(entry.labels)
		if _, err := fmt.Fprintf(w, "%s_count%s %d\n", entry.name, rendered, entry.count); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "%s_sum%s %s\n", entry.name, rendered, formatFloat(entry.sum)); err != nil {
			return err
		}
	}

	return nil
}

func seriesKey(name string, labels map[string]string) string {
	return name + "{" + formatLabels(labels) + "}"
}

// formatLabels ordena las etiquetas para que la clave sea estable.
func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, key := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(key)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(labels[key]))
		b.WriteByte('"')
	}
	return b.String()
}

func renderLabels(labels map[string]string) string {
	formatted := formatLabels(labels)
	if formatted == "" {
		return ""
	}
	return "{" + formatted + "}"
}

func escapeLabel(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return replacer.Replace(value)
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func copyLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for key, value := range labels {
		out[key] = value
	}
	return out
}
