package decode

import (
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
)

func TestSchemaTypeName(t *testing.T) {
	cases := []struct {
		in   sr.SchemaType
		want string
	}{
		{sr.TypeAvro, "AVRO"},
		{sr.TypeJSON, "JSON"},
		{sr.TypeProtobuf, "PROTOBUF"},
	}
	for _, c := range cases {
		if got := schemaTypeName(c.in); got != c.want {
			t.Errorf("schemaTypeName(%v)=%q want %q", c.in, got, c.want)
		}
	}
}

func TestSrEntriesToSchemaEntries(t *testing.T) {
	in := []sr.SubjectSchema{
		{Subject: "orders-value", Version: 3, ID: 12, Schema: sr.Schema{Schema: `{"type":"record"}`, Type: sr.TypeAvro}},
		{Subject: "users-value", Version: 1, ID: 5, Schema: sr.Schema{Schema: `{"x":1}`, Type: sr.TypeJSON}},
	}
	got := srEntriesToSchemaEntries(in)
	if len(got) != 2 {
		t.Fatalf("len=%d want 2", len(got))
	}
	if got[0].Subject != "orders-value" || got[0].Version != 3 || got[0].ID != 12 {
		t.Errorf("entry0 fields wrong: %+v", got[0])
	}
	if got[0].Type != "AVRO" {
		t.Errorf("entry0 type=%q want AVRO", got[0].Type)
	}
	if got[0].Text != `{"type":"record"}` {
		t.Errorf("entry0 text=%q", got[0].Text)
	}
	if got[1].Type != "JSON" {
		t.Errorf("entry1 type=%q want JSON", got[1].Type)
	}
}
