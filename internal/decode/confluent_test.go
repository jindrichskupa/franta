package decode

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
)

func TestConfluentDecodeNonMagicFallsBackToRaw(t *testing.T) {
	r := &Registry{cache: map[int]cachedSchema{}}
	if got := r.Decode([]byte("plain")); got != "plain" {
		t.Fatalf("non-magic fallback = %q, want plain", got)
	}
	if got := r.Decode([]byte{}); got != "" {
		t.Fatalf("empty = %q, want empty", got)
	}
}

func TestConfluentDecodeAvroWithSeededCache(t *testing.T) {
	r := &Registry{cache: map[int]cachedSchema{}}
	r.seed(7, kindAvro, personSchema)
	sch, _ := avro.Parse(personSchema)
	body, err := avro.Marshal(sch, map[string]any{"name": "bob", "age": 41})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	hdr := make([]byte, 5)
	hdr[0] = 0x00
	binary.BigEndian.PutUint32(hdr[1:5], 7)
	wire := append(hdr, body...)
	got := r.Decode(wire)
	if !strings.Contains(got, `"name": "bob"`) || !strings.Contains(got, `"age": 41`) {
		t.Fatalf("decoded = %q", got)
	}
}

func TestConfluentDecodeUnknownSchemaIDShowsPlaceholder(t *testing.T) {
	r := &Registry{cache: map[int]cachedSchema{}}
	hdr := make([]byte, 5)
	hdr[0] = 0x00
	binary.BigEndian.PutUint32(hdr[1:5], 999)
	wire := append(hdr, []byte("blob")...)
	// No client and no cache entry -> deterministic placeholder (not the raw
	// wire bytes, which would render the NUL magic byte + binary id as
	// control characters in the UI).
	got := r.Decode(wire)
	if !strings.Contains(got, "id=999") {
		t.Fatalf("placeholder = %q, want it to mention id=999", got)
	}
	if strings.ContainsRune(got, 0) {
		t.Fatalf("placeholder %q contains a NUL byte; should not include raw wire bytes", got)
	}
}

func TestConfluentNegativeCacheSuppressesRefetch(t *testing.T) {
	// A negative cache entry inserted manually should be respected without a
	// network call.
	r := &Registry{cache: map[int]cachedSchema{
		42: {ok: false, expiresAt: time.Now().Add(time.Hour)},
	}}
	hdr := make([]byte, 5)
	hdr[0] = 0x00
	binary.BigEndian.PutUint32(hdr[1:5], 42)
	wire := append(hdr, []byte("body")...)
	got := r.Decode(wire)
	if !strings.Contains(got, "id=42") {
		t.Fatalf("placeholder = %q, want id=42", got)
	}
}
