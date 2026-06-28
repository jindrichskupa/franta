package tui

import (
	"strings"
	"testing"
)

func sampleSchemas() []SchemaEntry {
	return []SchemaEntry{
		// out of order: subjects and versions both unsorted
		{Subject: "users-value", Version: 2, ID: 9, Type: "JSON", Text: `{"b":2}`},
		{Subject: "orders-value", Version: 1, ID: 4, Type: "AVRO", Text: `{"type":"record"}`},
		{Subject: "orders-value", Version: 3, ID: 7, Type: "AVRO", Text: `{"type":"record","v":3}`},
		{Subject: "orders-value", Version: 2, ID: 5, Type: "AVRO", Text: `{"type":"record","v":2}`},
		{Subject: "users-value", Version: 1, ID: 8, Type: "JSON", Text: `{"b":1}`},
	}
}

func TestGroupSubjects(t *testing.T) {
	groups := groupSubjects(sampleSchemas())
	if len(groups) != 2 {
		t.Fatalf("groups=%d want 2", len(groups))
	}
	// sorted by name: orders-value before users-value
	if groups[0].Name != "orders-value" || groups[1].Name != "users-value" {
		t.Fatalf("subjects not sorted: %s, %s", groups[0].Name, groups[1].Name)
	}
	// orders-value versions sorted asc 1,2,3
	ov := groups[0]
	if len(ov.Versions) != 3 {
		t.Fatalf("orders versions=%d want 3", len(ov.Versions))
	}
	for i, want := range []int{1, 2, 3} {
		if ov.Versions[i].Version != want {
			t.Errorf("orders version[%d]=%d want %d", i, ov.Versions[i].Version, want)
		}
	}
	if ov.Type != "AVRO" {
		t.Errorf("orders type=%q want AVRO", ov.Type)
	}
}

func TestSchemaTextIndentsJSON(t *testing.T) {
	out := schemaText(`{"a":1,"b":2}`)
	if !strings.Contains(out, "\n") {
		t.Errorf("valid JSON not indented:\n%s", out)
	}
	if !strings.Contains(out, "\"a\": 1") {
		t.Errorf("indent missing key spacing:\n%s", out)
	}
}

func TestSchemaTextRawPassthrough(t *testing.T) {
	proto := "syntax = \"proto3\";\nmessage Order { int64 id = 1; }"
	if got := schemaText(proto); got != proto {
		t.Errorf("non-JSON should pass through unchanged:\ngot:  %q\nwant: %q", got, proto)
	}
}

func TestRenderSubjectList(t *testing.T) {
	groups := groupSubjects(sampleSchemas())
	sel := map[string]int{"orders-value": 3, "users-value": 1}
	out := renderSubjectList(groups, []int{0, 1}, 0, true, sel, 20)
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d:\n%s", len(lines), out)
	}
	if !strings.HasPrefix(strings.TrimSpace(lines[0]), ">") {
		t.Errorf("cursor not on row 0: %q", lines[0])
	}
	if !strings.Contains(lines[0], "A") { // AVRO initial
		t.Errorf("type initial missing: %q", lines[0])
	}
	if !strings.Contains(lines[0], "orders-value") {
		t.Errorf("subject name missing: %q", lines[0])
	}
}
