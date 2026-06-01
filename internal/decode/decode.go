// Package decode renders raw record bytes for display, optionally using a
// Schema Registry to decode wire-format payloads.
package decode

// Decoder renders raw record bytes to a display string. It never returns an
// error: undecodable input falls back to a raw / JSON rendering.
type Decoder interface {
	Decode(b []byte) string
}
