package decode

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/hex"
	"regexp"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
)

func TestUUIDFromBytes(t *testing.T) {
	b := []byte{
		0x12, 0x34, 0x56, 0x78,
		0x9a, 0xbc,
		0xde, 0xf0,
		0x12, 0x34,
		0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
	}
	got := uuidFromBytes(b)
	want := "12345678-9abc-def0-1234-56789abcdef0"
	if got != want {
		t.Fatalf("uuidFromBytes = %q, want %q", got, want)
	}
}

func TestParseGlueHeaderValid(t *testing.T) {
	header := []byte{0x03, 0x00}
	uuid := []byte{
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
	}
	body := []byte("payload")
	wire := append(append(append([]byte{}, header...), uuid...), body...)
	comp, uid, gotBody, err := parseGlueHeader(wire)
	if err != nil {
		t.Fatalf("parseGlueHeader: %v", err)
	}
	if comp != 0x00 {
		t.Fatalf("compression = %d, want 0", comp)
	}
	if uid != "12345678-9abc-def0-1234-56789abcdef0" {
		t.Fatalf("uuid = %q", uid)
	}
	if string(gotBody) != "payload" {
		t.Fatalf("body = %q", gotBody)
	}
}

func TestParseGlueHeaderRejectsShortOrWrongVersion(t *testing.T) {
	if _, _, _, err := parseGlueHeader([]byte{0x03, 0x00}); err == nil {
		t.Fatal("expected error for short payload (< 18 bytes)")
	}
	short := bytes.Repeat([]byte{0xaa}, 17)
	if _, _, _, err := parseGlueHeader(short); err == nil {
		t.Fatal("expected error for 17-byte payload")
	}
	// Right length, wrong version byte
	wrong := append([]byte{0x04}, bytes.Repeat([]byte{0x00}, 17)...)
	if _, _, _, err := parseGlueHeader(wrong); err == nil {
		t.Fatal("expected error for non-0x03 version byte")
	}
}

func TestDecompressIfZlibPlain(t *testing.T) {
	got, err := decompressIfZlib(0x00, []byte("hello"))
	if err != nil || string(got) != "hello" {
		t.Fatalf("plain = %q, %v", got, err)
	}
}

func TestDecompressIfZlibZlibRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	_, _ = w.Write([]byte("world"))
	_ = w.Close()
	got, err := decompressIfZlib(0x05, buf.Bytes())
	if err != nil || string(got) != "world" {
		t.Fatalf("zlib = %q, %v", got, err)
	}
}

func TestDecompressIfZlibBadCompressionByte(t *testing.T) {
	if _, err := decompressIfZlib(0x09, []byte("x")); err == nil {
		t.Fatal("expected error for unknown compression byte")
	}
}

// ensure strings is used (later tests may add string assertions)
var _ = strings.Contains

const glueAvroSchema = `{"type":"record","name":"Person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

func TestGlueDecodeNonHeaderFallsBackToRaw(t *testing.T) {
	r := &GlueRegistry{cache: map[string]cachedSchema{}}
	if got := r.Decode([]byte("plain")); got != "plain" {
		t.Fatalf("non-header fallback = %q, want plain", got)
	}
	if got := r.Decode([]byte{}); got != "" {
		t.Fatalf("empty = %q, want empty", got)
	}
}

func TestGlueDecodeAvroWithSeededCachePlain(t *testing.T) {
	const uuid = "12345678-9abc-def0-1234-56789abcdef0"
	r := &GlueRegistry{cache: map[string]cachedSchema{
		uuid: {ok: true, kind: kindAvro, text: glueAvroSchema},
	}}
	sch, _ := avro.Parse(glueAvroSchema)
	body, err := avro.Marshal(sch, map[string]any{"name": "alice", "age": 30})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// header: 0x03, 0x00, uuid-bytes, then body
	wire := buildGlueWire(t, uuid, 0x00, body)
	got := r.Decode(wire)
	if !strings.Contains(got, `"name": "alice"`) || !strings.Contains(got, `"age": 30`) {
		t.Fatalf("decoded = %q", got)
	}
}

func TestGlueDecodeUnknownUUIDShowsPlaceholder(t *testing.T) {
	r := &GlueRegistry{cache: map[string]cachedSchema{}} // no client, no entries
	uuid := "00000000-0000-0000-0000-000000000000"
	wire := buildGlueWire(t, uuid, 0x00, []byte("blob"))
	got := r.Decode(wire)
	if !strings.Contains(got, "uuid="+uuid) {
		t.Fatalf("placeholder = %q, want it to mention uuid=%s", got, uuid)
	}
	if strings.ContainsRune(got, 0) {
		t.Fatalf("placeholder %q contains a NUL byte", got)
	}
}

// buildGlueWire assembles a Glue wire-format payload from a UUID, compression
// byte, and an already-(un)compressed body. Mirrors the production wire format.
func buildGlueWire(t *testing.T, uuid string, compression byte, body []byte) []byte {
	t.Helper()
	clean := strings.ReplaceAll(uuid, "-", "")
	raw, err := hex.DecodeString(clean)
	if err != nil || len(raw) != 16 {
		t.Fatalf("bad test uuid %q: %v", uuid, err)
	}
	wire := make([]byte, 2+16+len(body))
	wire[0] = 0x03
	wire[1] = compression
	copy(wire[2:18], raw)
	copy(wire[18:], body)
	// silence "imported and not used" for binary if it's no longer referenced
	_ = binary.BigEndian
	return wire
}

func TestGlueDecodeProtobufWithSeededCache(t *testing.T) {
	const uuid = "22222222-3333-4444-5555-666666666666"
	r := &GlueRegistry{cache: map[string]cachedSchema{
		uuid: {ok: true, kind: kindProtobuf, text: personProto},
	}}
	body := buildGlueProtoBody(t, personProto, "fr.test.Person", "alice", 30)
	wire := buildGlueWire(t, uuid, 0x00, body)
	got := r.Decode(wire)
	if !regexp.MustCompile(`"name":\s*"alice"`).MatchString(got) ||
		!regexp.MustCompile(`"age":\s*30`).MatchString(got) {
		t.Fatalf("decoded = %q", got)
	}
}
