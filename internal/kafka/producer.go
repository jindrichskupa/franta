package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	"franta/internal/record"
)

// Producer sends records to Kafka using the default partitioner.
type Producer struct {
	cl *kgo.Client
}

// NewProducer wraps a client for producing.
func NewProducer(cl *kgo.Client) *Producer { return &Producer{cl: cl} }

// Produce sends r synchronously and returns the first delivery error, if any.
func (p *Producer) Produce(ctx context.Context, r record.Record) error {
	return p.cl.ProduceSync(ctx, toKgoRecord(r)).FirstErr()
}

// toKgoRecord converts a domain record into a franz-go record for producing.
func toKgoRecord(r record.Record) *kgo.Record {
	hs := make([]kgo.RecordHeader, len(r.Headers))
	for i, h := range r.Headers {
		hs[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}
	return &kgo.Record{
		Topic:   r.Topic,
		Key:     r.Key,
		Value:   r.Value,
		Headers: hs,
	}
}
