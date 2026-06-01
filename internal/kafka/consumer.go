package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"

	"franta/internal/decode"
	"franta/internal/record"
)

// Fetched is a consumed record tagged with the seek generation that was active
// when it was read. Consumers of the stream use Gen to detect a re-seek and
// drop records from a superseded generation.
type Fetched struct {
	Record record.Record
	Gen    int64
}

// decoderHolder wraps the decoder interface so we can store it in an
// atomic.Pointer (an interface value itself is two words and not atomic).
type decoderHolder struct{ d decode.Decoder }

// Consumer tails topics on a franz-go client and emits decoded records.
type Consumer struct {
	cl  *kgo.Client
	gen atomic.Int64 // incremented by Seek/SwitchTopic after each reposition

	mu    sync.Mutex // guards topic (Seek and SwitchTopic run on separate cmd goroutines)
	topic string

	// dec is set by UseDecoder, read on the consumer goroutine each record.
	// atomic so a caller setting the decoder before Run starts cannot race
	// with the poll loop reading it (and so future callers could swap at runtime).
	dec atomic.Pointer[decoderHolder]
}

// NewConsumer wraps a client (built via NewClient with topic) for consuming the
// given topic.
func NewConsumer(cl *kgo.Client, topic string) *Consumer {
	return &Consumer{cl: cl, topic: topic}
}

// Topic returns the topic currently being consumed.
func (c *Consumer) Topic() string { return c.currentTopic() }

func (c *Consumer) currentTopic() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.topic
}

func (c *Consumer) setTopic(t string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topic = t
}

// Gen returns the current seek generation (0 before any seek/switch).
func (c *Consumer) Gen() int64 { return c.gen.Load() }

// UseDecoder sets the decoder used to fill each record's KeyText/ValueText
// display fields. Pass nil to disable decoding. Safe to call before or after
// Run starts.
func (c *Consumer) UseDecoder(d decode.Decoder) {
	if d == nil {
		c.dec.Store(nil)
		return
	}
	c.dec.Store(&decoderHolder{d: d})
}

// decoder returns the current decoder, or nil if none has been set.
func (c *Consumer) decoder() decode.Decoder {
	h := c.dec.Load()
	if h == nil {
		return nil
	}
	return h.d
}

// Run polls until ctx is cancelled (or the client is closed), sending each
// record on out tagged with the current seek generation. Per-partition fetch
// errors are non-fatal: franz-go retries internally, so they are reported on
// errs (best-effort, dropped if no reader) and polling continues. Run returns
// nil on clean shutdown, or ctx.Err() on cancellation. A nil errs channel
// disables error reporting.
func (c *Consumer) Run(ctx context.Context, out chan<- Fetched, errs chan<- error) error {
	for {
		fs := c.cl.PollFetches(ctx)
		if fs.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fs.EachError(func(topic string, p int32, err error) {
			if errs == nil {
				return
			}
			select {
			case errs <- fmt.Errorf("fetch %s[%d]: %w", topic, p, err):
			default: // drop rather than block the poll loop
			}
		})
		var sendErr error
		dec := c.decoder()
		fs.EachRecord(func(kr *kgo.Record) {
			rec := toRecord(kr)
			if dec != nil {
				rec.KeyText = dec.Decode(rec.Key)
				rec.ValueText = dec.Decode(rec.Value)
			}
			select {
			case out <- Fetched{Record: rec, Gen: c.gen.Load()}:
			case <-ctx.Done():
				sendErr = ctx.Err()
			}
		})
		if sendErr != nil {
			return sendErr
		}
	}
}

// toRecord converts a franz-go record into the domain record type.
func toRecord(kr *kgo.Record) record.Record {
	hs := make([]record.Header, len(kr.Headers))
	for i, h := range kr.Headers {
		hs[i] = record.Header{Key: h.Key, Value: h.Value}
	}
	return record.Record{
		Topic:     kr.Topic,
		Partition: kr.Partition,
		Offset:    kr.Offset,
		Timestamp: kr.Timestamp,
		Key:       kr.Key,
		Value:     kr.Value,
		Headers:   hs,
	}
}
