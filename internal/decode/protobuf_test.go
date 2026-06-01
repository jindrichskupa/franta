package decode

import (
	"regexp"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestReadMessageIndexShortFormSingleByte(t *testing.T) {
	path, body, err := readMessageIndex([]byte{0x00, 0xde, 0xad})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(path) != 1 || path[0] != 0 {
		t.Fatalf("path = %v, want [0]", path)
	}
	if string(body) != "\xde\xad" {
		t.Fatalf("body = % x, want de ad", body)
	}
}

func TestReadMessageIndexExplicitMulti(t *testing.T) {
	// count=2 (zigzag 2 -> varint 4 = 0x04); then indices 0 (0x00) and 1 (zigzag -> 0x02)
	prefix := []byte{0x04, 0x00, 0x02}
	body := []byte("body")
	path, gotBody, err := readMessageIndex(append(prefix, body...))
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(path) != 2 || path[0] != 0 || path[1] != 1 {
		t.Fatalf("path = %v, want [0 1]", path)
	}
	if string(gotBody) != "body" {
		t.Fatalf("body = %q, want body", gotBody)
	}
}

func TestReadMessageIndexExplicitEmptyCountIsZero(t *testing.T) {
	// count = 0 long form is still [0]
	path, _, err := readMessageIndex([]byte{0x00, 0xff})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if len(path) != 1 || path[0] != 0 {
		t.Fatalf("path = %v, want [0]", path)
	}
}

func TestReadMessageIndexTruncated(t *testing.T) {
	// count claims 1 entry but body is empty after it
	if _, _, err := readMessageIndex([]byte{0x02}); err == nil {
		t.Fatal("expected error for truncated index")
	}
	if _, _, err := readMessageIndex(nil); err == nil {
		t.Fatal("expected error for empty payload")
	}
	// A continuation byte with no terminator
	if _, _, err := readMessageIndex([]byte{0xff}); err == nil {
		t.Fatal("expected error for truncated varint")
	}
}

func TestReadMessageIndexRejectsHugeCount(t *testing.T) {
	// Varint encoding of unsigned 2^32 (5 bytes). Zigzag-decoded it becomes 2^31.
	// A naive make([]int, count) would allocate ~16 GiB. The bound must reject.
	huge := []byte{0x80, 0x80, 0x80, 0x80, 0x10}
	if _, _, err := readMessageIndex(huge); err == nil {
		t.Fatal("expected error for over-large index count")
	}
}

const personProto = `syntax = "proto3";
package fr.test;
message Person {
  string name = 1;
  int32 age = 2;
}`

// encodePerson builds a Confluent-wire-format protobuf payload (without the
// magic byte / schema id — just the index prefix + protobuf bytes) for a
// Person{name, age} using the given schema.
func encodePerson(t *testing.T, schema, name string, age int32) []byte {
	t.Helper()
	fd, err := compileProto(schema)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	desc := fd.Messages().Get(0)
	msg := dynamicpb.NewMessage(desc)
	msg.Set(desc.Fields().ByName("name"), protoreflect.ValueOfString(name))
	msg.Set(desc.Fields().ByName("age"), protoreflect.ValueOfInt32(age))
	body, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// single top-level message -> 0x00 prefix
	return append([]byte{0x00}, body...)
}

func TestDecodeProtoRoundTrip(t *testing.T) {
	payload := encodePerson(t, personProto, "alice", 30)
	got, err := decodeProto(personProto, payload)
	if err != nil {
		t.Fatalf("decodeProto: %v", err)
	}
	// protojson deliberately injects randomized whitespace after `:` (see
	// google.golang.org/protobuf/encoding/protojson docs). Match with regex.
	if !regexp.MustCompile(`"name":\s*"alice"`).MatchString(got) ||
		!regexp.MustCompile(`"age":\s*30`).MatchString(got) {
		t.Fatalf("decoded = %q", got)
	}
}

func TestDecodeProtoUnknownNestedIndex(t *testing.T) {
	// Payload claims a nested path that doesn't exist in Person.
	// count=1 (zigzag 1 -> varint 2 = 0x02); index 5 (zigzag -> 0x0a)
	payload := []byte{0x02, 0x0a, 0x00}
	if _, err := decodeProto(personProto, payload); err == nil {
		t.Fatal("expected error for out-of-range message index")
	}
}

// appendUnsignedVarint writes v as an unsigned varint, returning b extended.
func appendUnsignedVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// glueProtoIndex returns the position of fullName in fd's lex-sorted
// descriptor list (the same order the Glue serializer uses).
func glueProtoIndex(t *testing.T, schemaText, fullName string) int {
	t.Helper()
	fd, err := compileProto(schemaText)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	descs := glueDescriptorList(fd)
	for i, d := range descs {
		if string(d.FullName()) == fullName {
			return i
		}
	}
	t.Fatalf("descriptor %q not in glueDescriptorList(%v)", fullName, descNames(descs))
	return -1
}

func descNames(descs []protoreflect.MessageDescriptor) []string {
	out := make([]string, len(descs))
	for i, d := range descs {
		out[i] = string(d.FullName())
	}
	return out
}

// buildGlueProtoBody constructs a Glue-protocol protobuf payload (post-header,
// post-decompress): varint(messageIndex) + protobuf-bytes for a Person record
// with name + age. fullName must be in schemaText's lex-sorted descriptor list.
func buildGlueProtoBody(t *testing.T, schemaText, fullName, name string, age int32) []byte {
	t.Helper()
	fd, err := compileProto(schemaText)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	descs := glueDescriptorList(fd)
	idx := glueProtoIndex(t, schemaText, fullName)
	desc := descs[idx]
	msg := dynamicpb.NewMessage(desc)
	msg.Set(desc.Fields().ByName("name"), protoreflect.ValueOfString(name))
	msg.Set(desc.Fields().ByName("age"), protoreflect.ValueOfInt32(age))
	body, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto marshal: %v", err)
	}
	out := appendUnsignedVarint(nil, uint64(idx))
	out = append(out, body...)
	return out
}

func TestGlueDescriptorListLexSort(t *testing.T) {
	// AWS Glue's encoder BFS-collects every top-level + nested message, then
	// sorts by FullName. Verify our list against a schema with multiple
	// messages at different nesting depths.
	const multiProto = `syntax = "proto3";
package fr.test;
message B {
  message C {}
  message A {
    message D {}
  }
}
message Z {}`
	fd, err := compileProto(multiProto)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got := descNames(glueDescriptorList(fd))
	want := []string{"fr.test.B", "fr.test.B.A", "fr.test.B.A.D", "fr.test.B.C", "fr.test.Z"}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("descs[%d] = %q, want %q (full = %v)", i, got[i], want[i], got)
		}
	}
}

func TestDecodeProtoGlueRoundTrip(t *testing.T) {
	payload := buildGlueProtoBody(t, personProto, "fr.test.Person", "alice", 30)
	got, err := decodeProtoGlue(personProto, payload)
	if err != nil {
		t.Fatalf("decodeProtoGlue: %v", err)
	}
	if !regexp.MustCompile(`"name":\s*"alice"`).MatchString(got) ||
		!regexp.MustCompile(`"age":\s*30`).MatchString(got) {
		t.Fatalf("decoded = %q", got)
	}
}

func TestDecodeProtoGlueOutOfRangeIndex(t *testing.T) {
	// personProto has 1 message -> only index 0 is valid. Index 200 encoded as
	// two-byte varint 0xc8 0x01 (also exercises multi-byte varint reading).
	payload := []byte{0xc8, 0x01, 0x08, 0x00}
	if _, err := decodeProtoGlue(personProto, payload); err == nil {
		t.Fatal("expected error for out-of-range message index")
	}
}

func TestDecodeProtoGlueTruncatedVarint(t *testing.T) {
	// A varint continuation byte with no terminator.
	if _, err := decodeProtoGlue(personProto, []byte{0xff}); err == nil {
		t.Fatal("expected error for truncated varint")
	}
}

func TestDecodeProtoGlueBadProtoPayload(t *testing.T) {
	// Valid index (0), but the protobuf body is malformed.
	wire := append([]byte{0x00}, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}...)
	if _, err := decodeProtoGlue(personProto, wire); err == nil {
		t.Fatal("expected error for malformed protobuf body")
	}
}
