package kafka

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestToRecordCopiesFields(t *testing.T) {
	ts := time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)
	kr := &kgo.Record{
		Topic:     "t",
		Partition: 3,
		Offset:    42,
		Timestamp: ts,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers:   []kgo.RecordHeader{{Key: "h", Value: []byte("hv")}},
	}
	r := toRecord(kr)
	if r.Topic != "t" || r.Partition != 3 || r.Offset != 42 || !r.Timestamp.Equal(ts) {
		t.Fatalf("meta mismatch: %+v", r)
	}
	if string(r.Key) != "k" || string(r.Value) != "v" {
		t.Fatalf("payload mismatch: %+v", r)
	}
	if len(r.Headers) != 1 || r.Headers[0].Key != "h" || string(r.Headers[0].Value) != "hv" {
		t.Fatalf("headers mismatch: %+v", r.Headers)
	}
}

// fakeDecoder returns a fixed display string regardless of input. Used to
// verify the consumer threads the decoder through into the record.
type fakeDecoder struct{ out string }

func (f fakeDecoder) Decode([]byte) string { return f.out }

func TestUseDecoderSetsField(t *testing.T) {
	c := NewConsumer(nil, "t")
	c.UseDecoder(fakeDecoder{out: "x"})
	d := c.decoder()
	if d == nil {
		t.Fatal("decoder not set")
	}
	if got := d.Decode(nil); got != "x" {
		t.Fatalf("Decode = %q", got)
	}
	// UseDecoder(nil) clears it.
	c.UseDecoder(nil)
	if c.decoder() != nil {
		t.Fatal("expected nil after UseDecoder(nil)")
	}
}
