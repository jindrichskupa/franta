package decode

import (
	"regexp"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
)

const personSchema = `{"type":"record","name":"Person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

func TestDecodeWithAvro(t *testing.T) {
	sch, err := avro.Parse(personSchema)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	payload, err := avro.Marshal(sch, map[string]any{"name": "alice", "age": 30})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := decodeWith(kindAvro, personSchema, payload)
	if err != nil {
		t.Fatalf("decodeWith: %v", err)
	}
	if !strings.Contains(got, `"name": "alice"`) || !strings.Contains(got, `"age": 30`) {
		t.Fatalf("decoded = %q", got)
	}
}

func TestDecodeWithJSON(t *testing.T) {
	got, err := decodeWith(kindJSON, "", []byte(`{"x":1}`))
	if err != nil || got != "{\n  \"x\": 1\n}" {
		t.Fatalf("decodeWith json = %q, %v", got, err)
	}
}

func TestDecodeWithAvroBadPayload(t *testing.T) {
	// hamba/avro/v2 does not error on short/truncated payloads; use a payload
	// that encodes a string length larger than Config.MaxByteSliceSize to
	// force a decode error. Zig-zag encoding 0xfe,0xff,0xff,0xff,0x0f = 2^31-1.
	bigLenPayload := []byte{0xfe, 0xff, 0xff, 0xff, 0x0f}
	if _, err := decodeWith(kindAvro, personSchema, bigLenPayload); err == nil {
		t.Fatal("expected an error for malformed avro payload")
	}
}

func TestDecodeWithProtobufRoundTrip(t *testing.T) {
	payload := encodePerson(t, personProto, "carl", 22)
	got, err := decodeWith(kindProtobuf, personProto, payload)
	if err != nil {
		t.Fatalf("decodeWith protobuf: %v", err)
	}
	// protojson injects randomized whitespace after `:` — match with regex.
	if !regexp.MustCompile(`"name":\s*"carl"`).MatchString(got) ||
		!regexp.MustCompile(`"age":\s*22`).MatchString(got) {
		t.Fatalf("decoded = %q", got)
	}
}
