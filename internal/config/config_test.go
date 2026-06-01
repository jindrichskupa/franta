package config

import "testing"

func TestLoadValid(t *testing.T) {
	c, err := Load("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DefaultCluster != "local" {
		t.Fatalf("DefaultCluster = %q", c.DefaultCluster)
	}
	if len(c.Clusters) != 2 {
		t.Fatalf("clusters = %d, want 2", len(c.Clusters))
	}
	if c.Clusters["staging"].TLS != true {
		t.Fatalf("staging.TLS = false, want true")
	}
}

func TestValidateRejectsScramWithoutMechanism(t *testing.T) {
	c, err := Load("testdata/bad_auth.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := c.Validate(); err == nil {
		t.Fatal("Validate() = nil, want error for missing scram mechanism")
	}
}

func TestResolveUsesDefaultWhenNameEmpty(t *testing.T) {
	c, _ := Load("testdata/valid.yaml")
	name, cl, err := c.Resolve("")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if name != "local" || len(cl.Brokers) != 1 {
		t.Fatalf("Resolve() = %q %v", name, cl.Brokers)
	}
}

func TestResolveUnknownCluster(t *testing.T) {
	c, _ := Load("testdata/valid.yaml")
	if _, _, err := c.Resolve("nope"); err == nil {
		t.Fatal("Resolve(nope) = nil error, want error")
	}
}

func TestResolvePasswordFromEnv(t *testing.T) {
	t.Setenv("STAGING_PW", "s3cret")
	a := Auth{PasswordEnv: "STAGING_PW"}
	pw, err := a.ResolvePassword()
	if err != nil || pw != "s3cret" {
		t.Fatalf("ResolvePassword() = %q, %v", pw, err)
	}
}

func TestDanglingDefaultClusterDoesNotBlockExplicit(t *testing.T) {
	c := &Config{
		DefaultCluster: "missing",
		Clusters: map[string]Cluster{
			"real": {Brokers: []string{"b:9092"}, Auth: Auth{Type: "plaintext"}},
		},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil with a stale default_cluster", err)
	}
	if _, _, err := c.Resolve("real"); err != nil {
		t.Fatalf("Resolve(real) = %v, want nil", err)
	}
	if _, _, err := c.Resolve(""); err == nil {
		t.Fatal("Resolve(\"\") with missing default = nil, want error")
	}
}

func TestSchemaRegistryConfig(t *testing.T) {
	c := &Config{Clusters: map[string]Cluster{
		"a": {Brokers: []string{"b:9092"}, Auth: Auth{Type: "plaintext"}, SchemaRegistry: &SchemaRegistry{URL: "http://sr:8081"}},
	}}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}

	// missing URL is rejected
	c.Clusters["a"] = Cluster{Brokers: []string{"b:9092"}, Auth: Auth{Type: "plaintext"}, SchemaRegistry: &SchemaRegistry{}}
	if err := c.Validate(); err == nil {
		t.Fatal("Validate = nil, want error for missing SR url")
	}

	// unknown type rejected
	c.Clusters["a"] = Cluster{Brokers: []string{"b:9092"}, Auth: Auth{Type: "plaintext"}, SchemaRegistry: &SchemaRegistry{URL: "http://sr", Type: "bogus"}}
	if err := c.Validate(); err == nil {
		t.Fatal("Validate = nil, want error for unknown SR type")
	}
}

func TestSchemaRegistryResolvePassword(t *testing.T) {
	t.Setenv("SR_PW", "s3cret")
	s := SchemaRegistry{PasswordEnv: "SR_PW"}
	pw, err := s.ResolvePassword()
	if err != nil || pw != "s3cret" {
		t.Fatalf("ResolvePassword() = %q, %v", pw, err)
	}
}

func TestSchemaRegistryGlueConfig(t *testing.T) {
	// Glue is now accepted; URL is not required.
	c := &Config{Clusters: map[string]Cluster{
		"a": {
			Brokers: []string{"b:9092"},
			Auth:    Auth{Type: "plaintext"},
			SchemaRegistry: &SchemaRegistry{
				Type:         "glue",
				Region:       "us-east-1",
				RegistryName: "default",
			},
		},
	}}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate(glue) = %v, want nil", err)
	}
}

func TestSchemaRegistryUnknownTypeRejected(t *testing.T) {
	c := &Config{Clusters: map[string]Cluster{
		"a": {
			Brokers:        []string{"b:9092"},
			Auth:           Auth{Type: "plaintext"},
			SchemaRegistry: &SchemaRegistry{Type: "bogus", URL: "http://x"},
		},
	}}
	if err := c.Validate(); err == nil {
		t.Fatal("Validate = nil, want error for unknown SR type")
	}
}
