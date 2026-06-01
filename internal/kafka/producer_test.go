package kafka

import (
	"testing"

	"franta/internal/record"
)

func TestToKgoRecordCopiesFields(t *testing.T) {
	r := record.Record{
		Topic:   "t",
		Key:     []byte("k"),
		Value:   []byte("v"),
		Headers: []record.Header{{Key: "h", Value: []byte("hv")}},
	}
	kr := toKgoRecord(r)
	if kr.Topic != "t" || string(kr.Key) != "k" || string(kr.Value) != "v" {
		t.Fatalf("mismatch: %+v", kr)
	}
	if len(kr.Headers) != 1 || kr.Headers[0].Key != "h" || string(kr.Headers[0].Value) != "hv" {
		t.Fatalf("headers mismatch: %+v", kr.Headers)
	}
}
