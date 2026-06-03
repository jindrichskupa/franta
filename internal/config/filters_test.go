package config

import (
	"path/filepath"
	"testing"
)

func TestSavedFilterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "filters.yaml")
	if err := SaveFilter(path, SavedFilter{Name: "errors", Query: `header['severity'] == "ERROR"`}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := SaveFilter(path, SavedFilter{Name: "paid", Query: `value.status == "PAID"`}); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := LoadFilters(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 2 || got[0].Name != "errors" || got[1].Name != "paid" {
		t.Fatalf("got = %+v", got)
	}
	// overwrite by name
	if err := SaveFilter(path, SavedFilter{Name: "errors", Query: "value.error contains \"FAIL\""}); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, _ = LoadFilters(path)
	if len(got) != 2 || got[0].Name != "errors" || got[0].Query != `value.error contains "FAIL"` {
		t.Fatalf("overwrite failed: %+v", got)
	}
	// delete
	if err := DeleteFilter(path, "paid"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, _ = LoadFilters(path)
	if len(got) != 1 || got[0].Name != "errors" {
		t.Fatalf("delete failed: %+v", got)
	}
}

func TestLoadFiltersMissingFileIsEmpty(t *testing.T) {
	got, err := LoadFilters(filepath.Join(t.TempDir(), "absent.yaml"))
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got = %+v, want empty", got)
	}
}

func TestMergeFiltersSideWins(t *testing.T) {
	inline := []SavedFilter{{Name: "a", Query: "1"}, {Name: "b", Query: "2"}}
	side := []SavedFilter{{Name: "b", Query: "side-2"}, {Name: "c", Query: "3"}}
	got := MergeFilters(inline, side)
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	for _, f := range got {
		if f.Name == "b" && f.Query != "side-2" {
			t.Fatalf("side did not override inline: %+v", got)
		}
	}
}
