package auth

import (
	"testing"

	"franta/internal/config"
)

func TestBuildPlaintextNoOpts(t *testing.T) {
	opts, err := Build(config.Cluster{Auth: config.Auth{Type: "plaintext"}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(opts) != 0 {
		t.Fatalf("plaintext opts = %d, want 0", len(opts))
	}
}

func TestBuildScramRequiresMechanism(t *testing.T) {
	_, err := Build(config.Cluster{Auth: config.Auth{
		Type: "scram", Username: "u", Password: "p", Mechanism: "bogus",
	}})
	if err == nil {
		t.Fatal("Build = nil error, want error for bad mechanism")
	}
}

func TestBuildScramOK(t *testing.T) {
	opts, err := Build(config.Cluster{TLS: true, Auth: config.Auth{
		Type: "scram", Username: "u", Password: "p", Mechanism: "SCRAM-SHA-512",
	}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// one TLS opt + one SASL opt
	if len(opts) != 2 {
		t.Fatalf("opts = %d, want 2", len(opts))
	}
}

func TestBuildIAMReturnsOneOpt(t *testing.T) {
	opts, err := Build(config.Cluster{TLS: true, Auth: config.Auth{Type: "iam", Region: "eu-west-1"}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(opts) != 2 {
		t.Fatalf("opts = %d, want 2 (TLS + SASL)", len(opts))
	}
}
