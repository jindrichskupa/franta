package session

import (
	"context"
	"testing"

	"franta/internal/config"
)

func TestBuildAndStop(t *testing.T) {
	ctx := context.Background()
	cl := config.Cluster{Brokers: []string{"localhost:9092"}, Auth: config.Auth{Type: "plaintext"}}
	s, err := Build(ctx, cl, "local", "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if s.Name != "local" || s.Client == nil || s.ProdClient == nil || s.Cons == nil || s.Producer == nil {
		t.Fatalf("incomplete session: %+v", s)
	}
	if s.Gen() != 0 {
		t.Fatalf("fresh gen = %d", s.Gen())
	}
	// Stop is safe to call (never started → no goroutine to wait on) and idempotent.
	s.Stop()
	s.Stop()
	if !s.stopped {
		t.Fatal("stopped flag not set")
	}
}
