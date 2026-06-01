package decode

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/hamba/avro/v2"
)

// TestGlueDecodeAgainstFakeServer exercises the full Glue decode pipeline
// (NewGlue → GetRegistry ping → Decode → GetSchemaVersion fetch → decodeWith)
// against an httptest-fake Glue endpoint. Replaces the LocalStack integration
// test because LocalStack Community does not implement the Glue Schema
// Registry API (it's a LocalStack Pro feature).
func TestGlueDecodeAgainstFakeServer(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")

	const schemaText = `{"type":"record","name":"Person","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`
	const uuid = "11111111-2222-3333-4444-555555555555"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Amz-Target")
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		switch target {
		case "AWSGlue.GetRegistry":
			fmt.Fprint(w, `{"RegistryName":"default","RegistryArn":"arn:aws:glue:us-east-1:000000000000:registry/default","Status":"AVAILABLE"}`)
		case "AWSGlue.GetSchemaVersion":
			esc, _ := json.Marshal(schemaText)
			fmt.Fprintf(w, `{"SchemaVersionId":%q,"SchemaDefinition":%s,"DataFormat":"AVRO","Status":"AVAILABLE"}`, uuid, esc)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	dec, err := NewGlue("us-east-1", "", "default", srv.URL)
	if err != nil {
		t.Fatalf("NewGlue: %v", err)
	}

	sch, _ := avro.Parse(schemaText)
	body, err := avro.Marshal(sch, map[string]any{"name": "alice", "age": 30})
	if err != nil {
		t.Fatalf("avro marshal: %v", err)
	}
	wire := buildGlueWire(t, uuid, 0x00, body)

	got := dec.Decode(wire)
	if !regexp.MustCompile(`"name":\s*"alice"`).MatchString(got) ||
		!regexp.MustCompile(`"age":\s*30`).MatchString(got) {
		t.Fatalf("decoded = %q", got)
	}
}
