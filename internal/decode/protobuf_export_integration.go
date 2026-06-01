//go:build integration

package decode

import "google.golang.org/protobuf/reflect/protoreflect"

// CompileProtoForTest exposes compileProto to the external decode_test package
// for building wire-format payloads in integration tests. It is build-tagged
// `integration` so the symbol is absent from production binaries.
func CompileProtoForTest(schemaText string) (protoreflect.FileDescriptor, error) {
	return compileProto(schemaText)
}
