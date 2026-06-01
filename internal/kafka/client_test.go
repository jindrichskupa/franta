package kafka

import (
	"testing"

	"franta/internal/config"
)

func TestNewClientLazySucceeds(t *testing.T) {
	cl, err := NewClient(config.Cluster{
		Brokers: []string{"localhost:9092"},
		Auth:    config.Auth{Type: "plaintext"},
	}, "topic-a")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()
}

func TestNewClientPropagatesAuthError(t *testing.T) {
	_, err := NewClient(config.Cluster{
		Brokers: []string{"localhost:9092"},
		Auth:    config.Auth{Type: "scram", Username: "u", Password: "p", Mechanism: "bad"},
	})
	if err == nil {
		t.Fatal("NewClient = nil error, want auth error")
	}
}
