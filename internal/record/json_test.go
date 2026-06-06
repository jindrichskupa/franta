package record

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestToWire(t *testing.T) {
	r := Record{
		Topic:     "orders",
		Partition: 3,
		Offset:    14829113,
		Timestamp: time.Date(2026, 6, 1, 9, 13, 57, 0, time.UTC),
		Key:       []byte("ORD-1"),
		Value:     []byte(`{"a":1}`),
		Headers:   []Header{{Key: "source", Value: []byte("web")}},
	}
	w := ToWire(r)
	if w.Partition != 3 || w.Offset != 14829113 {
		t.Fatalf("part/offset: %+v", w)
	}
	if w.Timestamp != "2026-06-01T09:13:57Z" {
		t.Fatalf("timestamp: %q", w.Timestamp)
	}
	if w.Key != "ORD-1" {
		t.Fatalf("key: %q", w.Key)
	}
	if w.Value != r.ValueDisplay() {
		t.Fatalf("value: %q want %q", w.Value, r.ValueDisplay())
	}
	if w.Headers["source"] != "web" {
		t.Fatalf("headers: %+v", w.Headers)
	}
}

func TestJSON(t *testing.T) {
	r := Record{Partition: 1, Offset: 2, Timestamp: time.Unix(0, 0).UTC(), Key: []byte("k"), Value: []byte("v")}
	b, err := JSON(r)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("not valid JSON: %v\n%s", err, b)
	}
	if got["key"] != "k" || got["value"] != "v" {
		t.Fatalf("round-trip: %+v", got)
	}
}

func sampleRecs() []Record {
	return []Record{
		{Partition: 0, Offset: 1, Timestamp: time.Unix(0, 0).UTC(), Key: []byte("k1"), Value: []byte(`{"a":1}`), Headers: []Header{{Key: "h", Value: []byte("v")}}},
		{Partition: 2, Offset: 9, Timestamp: time.Unix(0, 0).UTC(), Key: []byte("k2"), Value: []byte("plain")},
	}
}

func TestWriteJSONL(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteJSONL(&buf, sampleRecs()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d: %q", len(lines), buf.String())
	}
	var w0 Wire
	if err := json.Unmarshal([]byte(lines[0]), &w0); err != nil {
		t.Fatalf("line0 not JSON: %v", err)
	}
	if w0.Key != "k1" || w0.Headers["h"] != "v" {
		t.Fatalf("line0: %+v", w0)
	}
}

func TestWriteJSONArray(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteJSONArray(&buf, sampleRecs()); err != nil {
		t.Fatal(err)
	}
	var ws []Wire
	if err := json.Unmarshal(buf.Bytes(), &ws); err != nil {
		t.Fatalf("not a JSON array: %v\n%s", err, buf.String())
	}
	if len(ws) != 2 || ws[1].Offset != 9 {
		t.Fatalf("array: %+v", ws)
	}
}

func TestWriteCSV(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteCSV(&buf, sampleRecs()); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(&buf).ReadAll()
	if err != nil {
		t.Fatalf("csv parse: %v", err)
	}
	if len(rows) != 3 { // header + 2
		t.Fatalf("want 3 rows, got %d", len(rows))
	}
	if rows[0][0] != "partition" || rows[0][5] != "headers" {
		t.Fatalf("header row: %v", rows[0])
	}
	if rows[1][0] != "0" || rows[1][3] != "k1" {
		t.Fatalf("row1: %v", rows[1])
	}
}
