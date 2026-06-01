package decode

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// readMessageIndex parses Confluent's protobuf message-index prefix. It returns
// the index path (e.g., [0] for the first top-level message), the remaining
// payload bytes (the actual protobuf message), and an error for truncated or
// malformed input.
//
// Wire format: either the single byte 0x00 (short form for [0] — the common
// single-message case), or a leading zigzag varint count followed by `count`
// zigzag varint indices. A count of 0 in the long form is also [0].
func readMessageIndex(payload []byte) ([]int, []byte, error) {
	if len(payload) == 0 {
		return nil, nil, fmt.Errorf("empty payload (no message-index prefix)")
	}
	if payload[0] == 0x00 {
		return []int{0}, payload[1:], nil
	}
	v, n := readVarint(payload)
	if n == 0 {
		return nil, nil, fmt.Errorf("truncated message-index count")
	}
	count := zigzagDecode(v)
	// Bound the count so a crafted varint (e.g. 5 bytes encoding ~2^31) cannot
	// trigger a multi-GB allocation in make([]int, count) on the consumer
	// goroutine. Real Confluent paths are 1–a-handful deep; 1024 is generous.
	const maxIndexDepth = 1024
	if count < 0 || count > maxIndexDepth {
		return nil, nil, fmt.Errorf("invalid message-index count %d", count)
	}
	if count == 0 {
		return []int{0}, payload[n:], nil
	}
	pos := n
	path := make([]int, count)
	for i := int64(0); i < count; i++ {
		v, n := readVarint(payload[pos:])
		if n == 0 {
			return nil, nil, fmt.Errorf("truncated message-index path at %d", i)
		}
		path[i] = int(zigzagDecode(v))
		pos += n
	}
	return path, payload[pos:], nil
}

// readVarint decodes an unsigned varint from b. Returns (value, bytesRead);
// bytesRead == 0 on truncated or overlong input.
func readVarint(b []byte) (uint64, int) {
	var v uint64
	var s uint
	for i, c := range b {
		if c < 0x80 {
			return v | uint64(c)<<s, i + 1
		}
		v |= uint64(c&0x7f) << s
		s += 7
		if s > 63 {
			return 0, 0
		}
	}
	return 0, 0
}

// zigzagDecode reverses zigzag encoding: (n << 1) ^ (n >> 63).
func zigzagDecode(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}

// protoCache holds parsed FileDescriptors keyed by raw schema text so each
// .proto schema is compiled at most once per process.
var protoCache sync.Map // map[string]protoreflect.FileDescriptor

// compileProto parses a single-file .proto schema and returns its descriptor.
// Well-known types are resolved by protocompile's bundled descriptors.
func compileProto(schemaText string) (protoreflect.FileDescriptor, error) {
	if v, ok := protoCache.Load(schemaText); ok {
		return v.(protoreflect.FileDescriptor), nil
	}
	const fname = "schema.proto"
	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(map[string]string{
				fname: schemaText,
			}),
		},
	}
	files, err := compiler.Compile(context.Background(), fname)
	if err != nil {
		return nil, fmt.Errorf("compile proto: %w", err)
	}
	fd := files.FindFileByPath(fname)
	if fd == nil {
		return nil, fmt.Errorf("compiled file %q not found in result", fname)
	}
	protoCache.Store(schemaText, fd)
	return fd, nil
}

// decodeProto decodes a Confluent-wire-format protobuf payload (message-index
// prefix + protobuf bytes) using the given schema text.
func decodeProto(schemaText string, payload []byte) (string, error) {
	fd, err := compileProto(schemaText)
	if err != nil {
		return "", err
	}
	path, body, err := readMessageIndex(payload)
	if err != nil {
		return "", err
	}
	if len(path) == 0 {
		return "", fmt.Errorf("empty message-index path")
	}
	if path[0] < 0 || path[0] >= fd.Messages().Len() {
		return "", fmt.Errorf("top-level message index %d out of range (have %d)", path[0], fd.Messages().Len())
	}
	desc := fd.Messages().Get(path[0])
	for i := 1; i < len(path); i++ {
		if path[i] < 0 || path[i] >= desc.Messages().Len() {
			return "", fmt.Errorf("nested message index %d out of range at depth %d", path[i], i)
		}
		desc = desc.Messages().Get(path[i])
	}
	msg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(body, msg); err != nil {
		return "", fmt.Errorf("proto unmarshal: %w", err)
	}
	opts := protojson.MarshalOptions{Multiline: true, Indent: "  "}
	out, err := opts.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("protojson: %w", err)
	}
	return string(out), nil
}

// glueDescriptorCache memoises the flat, lex-sorted descriptor list per parsed
// FileDescriptor so we don't re-walk + re-sort on every record.
var glueDescriptorCache sync.Map // protoreflect.FileDescriptor -> []protoreflect.MessageDescriptor

// glueDescriptorList returns every message in fd (top-level and nested) in
// lexicographic order of FullName. This matches AWS Glue's serializer, which
// indexes into exactly this list (see aws-glue-schema-registry's
// MessageIndexFinder.getAll + ProtobufWireFormatEncoder).
func glueDescriptorList(fd protoreflect.FileDescriptor) []protoreflect.MessageDescriptor {
	if v, ok := glueDescriptorCache.Load(fd); ok {
		return v.([]protoreflect.MessageDescriptor)
	}
	var out []protoreflect.MessageDescriptor
	var collect func(msgs protoreflect.MessageDescriptors)
	collect = func(msgs protoreflect.MessageDescriptors) {
		for i := 0; i < msgs.Len(); i++ {
			m := msgs.Get(i)
			out = append(out, m)
			collect(m.Messages())
		}
	}
	collect(fd.Messages())
	sort.Slice(out, func(i, j int) bool {
		return string(out[i].FullName()) < string(out[j].FullName())
	})
	glueDescriptorCache.Store(fd, out)
	return out
}

// decodeProtoGlue decodes a Glue-wire-format protobuf payload (varint message
// index + protobuf bytes) using the given schema text. The index points into
// the lex-sorted descriptor list (see glueDescriptorList).
func decodeProtoGlue(schemaText string, payload []byte) (string, error) {
	fd, err := compileProto(schemaText)
	if err != nil {
		return "", err
	}
	descs := glueDescriptorList(fd)
	if len(descs) == 0 {
		return "", fmt.Errorf("schema has no messages")
	}
	v, n := readVarint(payload)
	if n == 0 {
		return "", fmt.Errorf("truncated message-index varint")
	}
	if v > uint64(len(descs)-1) {
		return "", fmt.Errorf("message index %d out of range (have %d)", v, len(descs))
	}
	desc := descs[int(v)]
	msg := dynamicpb.NewMessage(desc)
	if err := proto.Unmarshal(payload[n:], msg); err != nil {
		return "", fmt.Errorf("proto unmarshal into %q: %w", desc.FullName(), err)
	}
	opts := protojson.MarshalOptions{Multiline: true, Indent: "  "}
	out, err := opts.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("protojson: %w", err)
	}
	return string(out), nil
}
