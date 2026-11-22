package reqid

import (
	"context"
	"testing"
)

func TestWithAndFromContext(t *testing.T) {
	ctx := WithContext(context.Background(), "req-123")
	if got := FromContext(ctx); got != "req-123" {
		t.Errorf("FromContext() = %q, want req-123", got)
	}
	if got := FromContext(context.Background()); got != "" {
		t.Errorf("FromContext(empty) = %q, want empty", got)
	}
	if got := FromContext(nil); got != "" {
		t.Errorf("FromContext(nil) = %q, want empty", got)
	}
}

func TestSanitize(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{name: "keeps valid", in: "abc-123_X.Y:Z", want: "abc-123_X.Y:Z"},
		{name: "trims spaces", in: "  req-1  ", want: "req-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Sanitize(tc.in); got != tc.want {
				t.Errorf("Sanitize(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	for _, in := range []string{"", "   ", "req with spaces", "inject\nlog", string(make([]byte, 129))} {
		got := Sanitize(in)
		if got == "" || got == in {
			t.Errorf("Sanitize(%q) = %q, want a generated replacement", in, got)
		}
	}
}
