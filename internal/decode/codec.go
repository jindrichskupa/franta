package decode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hamba/avro/v2"
)

// schemaKind categorizes a registry schema's wire format.
type schemaKind int

const (
	kindOther schemaKind = iota
	kindAvro
	kindJSON
	kindProtobuf
)

// decodeWith renders a payload to a pretty-printed JSON display string using
// the given schema text. Protobuf is intentionally unsupported in D1.
func decodeWith(kind schemaKind, schemaText string, payload []byte) (string, error) {
	switch kind {
	case kindAvro:
		sch, err := avro.Parse(schemaText)
		if err != nil {
			return "", fmt.Errorf("parse avro schema: %w", err)
		}
		var v any
		if err := avro.Unmarshal(sch, payload, &v); err != nil {
			return "", fmt.Errorf("avro decode: %w", err)
		}
		var buf bytes.Buffer
		enc := json.NewEncoder(&buf)
		enc.SetIndent("", "  ")
		if err := enc.Encode(v); err != nil {
			return "", err
		}
		// Encode appends a trailing newline; strip it for cleaner display.
		return string(bytes.TrimRight(buf.Bytes(), "\n")), nil
	case kindJSON:
		var buf bytes.Buffer
		if err := json.Indent(&buf, payload, "", "  "); err != nil {
			return "", fmt.Errorf("json pretty-print: %w", err)
		}
		return buf.String(), nil
	case kindProtobuf:
		return decodeProto(schemaText, payload)
	}
	return "", errors.New("unknown schema kind")
}
