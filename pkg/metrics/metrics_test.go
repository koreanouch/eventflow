package metrics

import (
	"strings"
	"sync"
	"testing"
)

func TestCounterValueByLabels(t *testing.T) {
	reg := NewRegistry()

	reg.Inc("http_requests_total", map[string]string{"method": "GET", "status": "200"})
	reg.Inc("http_requests_total", map[string]string{"method": "GET", "status": "200"})
	reg.Inc("http_requests_total", map[string]string{"method": "POST", "status": "500"})

	if got := reg.CounterValue("http_requests_total", map[string]string{"method": "GET", "status": "200"}); got != 2 {
		t.Errorf("GET/200 = %d, want 2", got)
	}
	if got := reg.CounterValue("http_requests_total", map[string]string{"status": "500", "method": "POST"}); got != 1 {
		t.Errorf("POST/500 (label order must not matter) = %d, want 1", got)
	}
	if got := reg.CounterValue("http_requests_total", map[string]string{"method": "DELETE"}); got != 0 {
		t.Errorf("unknown series = %d, want 0", got)
	}
}

func TestObserveAccumulatesSumAndCount(t *testing.T) {
	reg := NewRegistry()
	reg.Observe("request_duration_seconds", nil, 0.5)
	reg.Observe("request_duration_seconds", nil, 1.5)

	var out strings.Builder
	if err := reg.WritePrometheus(&out); err != nil {
		t.Fatalf("WritePrometheus() error: %v", err)
	}

	body := out.String()
	if !strings.Contains(body, "request_duration_seconds_count 2") {
		t.Errorf("missing summary count in:\n%s", body)
	}
	if !strings.Contains(body, "request_duration_seconds_sum 2") {
		t.Errorf("missing summary sum in:\n%s", body)
	}
}

func TestWritePrometheusFormatIsStable(t *testing.T) {
	reg := NewRegistry()
	reg.Inc("b_total", map[string]string{"b": "2", "a": "1"})
	reg.Inc("a_total", nil)
	reg.Inc("a_total", nil)

	var out strings.Builder
	if err := reg.WritePrometheus(&out); err != nil {
		t.Fatalf("WritePrometheus() error: %v", err)
	}

	want := "# TYPE a_total counter\n" +
		"a_total 2\n" +
		"# TYPE b_total counter\n" +
		`b_total{a="1",b="2"} 1` + "\n"
	if got := out.String(); got != want {
		t.Errorf("unexpected exposition:\n%s\nwant:\n%s", got, want)
	}
}

func TestWritePrometheusEscapesLabels(t *testing.T) {
	reg := NewRegistry()
	reg.Inc("quotes_total", map[string]string{"value": "line\nwith \"quotes\" and \\"})

	var out strings.Builder
	if err := reg.WritePrometheus(&out); err != nil {
		t.Fatalf("WritePrometheus() error: %v", err)
	}

	want := `quotes_total{value="line\nwith \"quotes\" and \\"} 1`
	if !strings.Contains(out.String(), want) {
		t.Errorf("escaped label not found in:\n%s", out.String())
	}
}

func TestReset(t *testing.T) {
	reg := NewRegistry()
	reg.Inc("counter_total", nil)
	reg.Observe("summary_seconds", nil, 1)

	reg.Reset()

	if got := reg.CounterValue("counter_total", nil); got != 0 {
		t.Errorf("counter after reset = %d, want 0", got)
	}
	var out strings.Builder
	if err := reg.WritePrometheus(&out); err != nil {
		t.Fatalf("WritePrometheus() error: %v", err)
	}
	if out.String() != "" {
		t.Errorf("exposition after reset = %q, want empty", out.String())
	}
}

func TestConcurrentIncrements(t *testing.T) {
	reg := NewRegistry()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				reg.Inc("concurrent_total", map[string]string{"worker": "a"})
			}
		}()
	}
	wg.Wait()

	if got := reg.CounterValue("concurrent_total", map[string]string{"worker": "a"}); got != 10000 {
		t.Errorf("concurrent counter = %d, want 10000", got)
	}
}

func TestEmptyNameIsIgnored(t *testing.T) {
	reg := NewRegistry()
	reg.Inc("", nil)
	reg.Observe("", nil, 1)

	var out strings.Builder
	if err := reg.WritePrometheus(&out); err != nil {
		t.Fatalf("WritePrometheus() error: %v", err)
	}
	if out.String() != "" {
		t.Errorf("empty name produced output: %q", out.String())
	}
}

func TestPackageLevelHelpersUseDefaultRegistry(t *testing.T) {
	Reset()
	defer Reset()

	Inc(OrdersCreatedTotal, nil)
	Inc(OrdersCreatedTotal, nil)
	if got := CounterValue(OrdersCreatedTotal, nil); got != 2 {
		t.Errorf("CounterValue() = %d, want 2", got)
	}

	Observe(HTTPRequestDurationSeconds, map[string]string{"route": "/test"}, 0.25)
	var out strings.Builder
	if err := Default().WritePrometheus(&out); err != nil {
		t.Fatalf("WritePrometheus() error: %v", err)
	}
	if !strings.Contains(out.String(), `http_request_duration_seconds_count{route="/test"} 1`) {
		t.Errorf("default registry output missing observation:\n%s", out.String())
	}
}
