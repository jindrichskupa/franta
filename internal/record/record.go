// Package record defines the Kafka record domain type and a bounded buffer.
package record

import (
	"bytes"
	"encoding/json"
	"time"
)

// Header is a single Kafka record header.
type Header struct {
	Key   string
	Value []byte
}

// Record is a decoded Kafka record, independent of any client library.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header

	// KeyText / ValueText are an optional decoded display rendering of Key /
	// Value (e.g. a Schema-Registry decoder filled these in). Empty means "no
	// decoded form available; fall back to Display(raw)". Raw Key/Value bytes
	// are unchanged by the decoder.
	KeyText   string
	ValueText string
}

// Display renders bytes for human viewing: pretty-printed when valid JSON,
// otherwise the raw text. Nil/empty yields "".
func Display(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if json.Valid(b) {
		if err := json.Indent(&buf, b, "", "  "); err == nil {
			return buf.String()
		}
	}
	return string(b)
}

// KeyString and ValueString are convenience decoders for the record fields.
func (r Record) KeyString() string   { return Display(r.Key) }
func (r Record) ValueString() string { return Display(r.Value) }

// KeyDisplay returns the decoded display text for the key when set, else the
// raw JSON-aware rendering of Key.
func (r Record) KeyDisplay() string {
	if r.KeyText != "" {
		return r.KeyText
	}
	return Display(r.Key)
}

// ValueDisplay returns the decoded display text for the value when set, else
// the raw JSON-aware rendering of Value.
func (r Record) ValueDisplay() string {
	if r.ValueText != "" {
		return r.ValueText
	}
	return Display(r.Value)
}
