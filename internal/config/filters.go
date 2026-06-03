package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// FiltersFile is the YAML schema for the side-file the app rewrites when the
// user saves a filter via ctrl+s. Lives alongside config.yaml so it doesn't
// clobber comments in the main config (gopkg.in/yaml.v3 round-trips lose
// comments on re-encode).
type FiltersFile struct {
	SavedFilters []SavedFilter `yaml:"saved_filters"`
}

// FiltersPath returns the standard side-file path: same directory as the
// config file, named filters.yaml.
func FiltersPath(configPath string) string {
	return filepath.Join(filepath.Dir(configPath), "filters.yaml")
}

// LoadFilters reads the side-file. Missing file → empty slice + nil error;
// any other read/parse error is surfaced so a typo gets the user's attention.
func LoadFilters(path string) ([]SavedFilter, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read filters %s: %w", path, err)
	}
	var ff FiltersFile
	if err := yaml.Unmarshal(b, &ff); err != nil {
		return nil, fmt.Errorf("parse filters %s: %w", path, err)
	}
	return ff.SavedFilters, nil
}

// SaveFilter writes a single saved filter to the side-file. Adding a name that
// already exists overwrites the prior query. The directory is created if
// missing. File is rewritten atomically via tmp + rename.
func SaveFilter(path string, sf SavedFilter) error {
	if sf.Name == "" {
		return fmt.Errorf("filter name is empty")
	}
	if sf.Query == "" {
		return fmt.Errorf("filter query is empty")
	}
	existing, err := LoadFilters(path)
	if err != nil {
		return err
	}
	replaced := false
	for i, f := range existing {
		if f.Name == sf.Name {
			existing[i] = sf
			replaced = true
			break
		}
	}
	if !replaced {
		existing = append(existing, sf)
	}
	return writeFiltersFile(path, existing)
}

// DeleteFilter removes the named filter from the side-file. No-op when the
// filter is missing or the file doesn't exist.
func DeleteFilter(path, name string) error {
	existing, err := LoadFilters(path)
	if err != nil {
		return err
	}
	out := existing[:0]
	for _, f := range existing {
		if f.Name != name {
			out = append(out, f)
		}
	}
	return writeFiltersFile(path, out)
}

// MergeFilters returns inline + side-file filters, with side-file entries
// winning on name collision (the side-file is what the app rewrites, so it
// reflects the latest user intent).
func MergeFilters(inline, side []SavedFilter) []SavedFilter {
	idx := make(map[string]int, len(inline)+len(side))
	out := make([]SavedFilter, 0, len(inline)+len(side))
	for _, f := range inline {
		idx[f.Name] = len(out)
		out = append(out, f)
	}
	for _, f := range side {
		if i, ok := idx[f.Name]; ok {
			out[i] = f
			continue
		}
		idx[f.Name] = len(out)
		out = append(out, f)
	}
	return out
}

func writeFiltersFile(path string, sfs []SavedFilter) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	b, err := yaml.Marshal(FiltersFile{SavedFilters: sfs})
	if err != nil {
		return fmt.Errorf("encode filters: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s -> %s: %w", tmp, path, err)
	}
	return nil
}
