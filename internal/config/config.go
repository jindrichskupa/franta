// Package config loads and validates franta's YAML cluster configuration.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Auth describes how to authenticate to a cluster.
type Auth struct {
	Type        string `yaml:"type"` // plaintext | plain | scram | iam
	Mechanism   string `yaml:"mechanism"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	PasswordEnv string `yaml:"password_env"`
	Region      string `yaml:"region"`
	Profile     string `yaml:"profile"`
}

// ResolvePassword returns the literal password, or reads PasswordEnv from the
// environment. Returns an error if PasswordEnv is set but unset/empty.
func (a Auth) ResolvePassword() (string, error) {
	if a.Password != "" {
		return a.Password, nil
	}
	if a.PasswordEnv != "" {
		v, ok := os.LookupEnv(a.PasswordEnv)
		if !ok || v == "" {
			return "", fmt.Errorf("password env %q is not set", a.PasswordEnv)
		}
		return v, nil
	}
	return "", nil
}

// SchemaRegistry configures a Schema Registry for decoding record bytes.
// Confluent uses URL + optional basic auth. Glue uses Region/Profile/
// RegistryName + optional Endpoint override (private / custom AWS endpoint).
type SchemaRegistry struct {
	Type         string `yaml:"type"`          // "" | "confluent" | "glue"
	URL          string `yaml:"url"`           // confluent
	Username     string `yaml:"username"`      // confluent
	Password     string `yaml:"password"`      // confluent
	PasswordEnv  string `yaml:"password_env"`  // confluent
	Region       string `yaml:"region"`        // glue (optional, AWS default chain)
	Profile      string `yaml:"profile"`       // glue (optional)
	RegistryName string `yaml:"registry_name"` // glue (optional, defaults to "default")
	Endpoint     string `yaml:"endpoint"`      // glue (optional, custom AWS endpoint)
}

// ResolvePassword returns the literal Password, or reads PasswordEnv from the
// environment. Returns an error if PasswordEnv is set but unset/empty.
func (s SchemaRegistry) ResolvePassword() (string, error) {
	if s.Password != "" {
		return s.Password, nil
	}
	if s.PasswordEnv != "" {
		v, ok := os.LookupEnv(s.PasswordEnv)
		if !ok || v == "" {
			return "", fmt.Errorf("schema_registry password env %q is not set", s.PasswordEnv)
		}
		return v, nil
	}
	return "", nil
}

// Cluster is a single named broker target.
type Cluster struct {
	Brokers        []string        `yaml:"brokers"`
	TLS            bool            `yaml:"tls"`
	Auth           Auth            `yaml:"auth"`
	SchemaRegistry *SchemaRegistry `yaml:"schema_registry"`
	// DefaultSeek is the start position used when no --from flag is provided
	// (same mini-syntax as the CLI: end | beginning | last:N | <duration> |
	// RFC3339). Empty falls back to "end".
	DefaultSeek string `yaml:"default_seek"`
}

// SavedFilter is one named DSL filter the user can recall from the TUI.
type SavedFilter struct {
	Name  string `yaml:"name"`
	Query string `yaml:"query"`
}

// Config is the top-level configuration document.
type Config struct {
	DefaultCluster string             `yaml:"default_cluster"`
	Clusters       map[string]Cluster `yaml:"clusters"`
	// SavedFilters loaded from the main config.yaml (treated as read-only — the
	// app preserves comments in config.yaml by writing user-added filters to a
	// separate filters.yaml in the same directory).
	SavedFilters []SavedFilter `yaml:"saved_filters"`
}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return &c, nil
}

// Validate checks structural rules and fails fast with a clear message.
func (c *Config) Validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("config has no clusters")
	}
	for name, cl := range c.Clusters {
		if len(cl.Brokers) == 0 {
			return fmt.Errorf("cluster %q: no brokers", name)
		}
		switch cl.Auth.Type {
		case "", "plaintext":
		case "plain":
			if cl.Auth.Username == "" {
				return fmt.Errorf("cluster %q: plain auth requires username", name)
			}
		case "scram":
			if cl.Auth.Username == "" {
				return fmt.Errorf("cluster %q: scram auth requires username", name)
			}
			if cl.Auth.Mechanism != "SCRAM-SHA-256" && cl.Auth.Mechanism != "SCRAM-SHA-512" {
				return fmt.Errorf("cluster %q: scram mechanism must be SCRAM-SHA-256 or SCRAM-SHA-512", name)
			}
		case "iam":
		default:
			return fmt.Errorf("cluster %q: unknown auth type %q", name, cl.Auth.Type)
		}
		if cl.SchemaRegistry != nil {
			switch cl.SchemaRegistry.Type {
			case "", "confluent":
				if cl.SchemaRegistry.URL == "" {
					return fmt.Errorf("cluster %q: schema_registry (confluent) requires url", name)
				}
			case "glue":
				// URL is not used for Glue; Region/Profile/RegistryName/Endpoint are
				// all optional. Credentials come from the AWS default chain. Reject
				// `url` to catch the natural Confluent-to-Glue typo.
				if cl.SchemaRegistry.URL != "" {
					return fmt.Errorf("cluster %q: schema_registry (glue) does not use url; set endpoint for a custom AWS endpoint", name)
				}
			default:
				return fmt.Errorf("cluster %q: unknown schema_registry type %q", name, cl.SchemaRegistry.Type)
			}
		}
	}
	// default_cluster existence is not checked here: an explicit --cluster makes
	// the default irrelevant, and Resolve("") reports a missing default when one
	// is actually needed.
	return nil
}

// Resolve returns the named cluster, or the default when name is empty.
func (c *Config) Resolve(name string) (string, Cluster, error) {
	if name == "" {
		name = c.DefaultCluster
	}
	if name == "" {
		return "", Cluster{}, fmt.Errorf("no cluster specified and no default_cluster set")
	}
	cl, ok := c.Clusters[name]
	if !ok {
		return "", Cluster{}, fmt.Errorf("cluster %q not found", name)
	}
	return name, cl, nil
}

// DefaultPath returns ~/.config/franta/config.yaml.
func DefaultPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "config.yaml"
	}
	return dir + "/franta/config.yaml"
}
