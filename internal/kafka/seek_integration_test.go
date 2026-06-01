//go:build integration

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"franta/internal/config"
	"franta/internal/record"
)

// firstOffset seeks a fresh consumer to spec and returns the offset of the
// first record it delivers, or fails on timeout.
func firstOffset(t *testing.T, cl config.Cluster, topic string, spec StartSpec) int64 {
	t.Helper()
	client, err := NewClient(cl, topic)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	t.Cleanup(client.Close)
	cons := NewConsumer(client, topic)
	if err := cons.Seek(context.Background(), spec); err != nil {
		t.Fatalf("seek %+v: %v", spec, err)
	}
	out := make(chan Fetched, 16)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = cons.Run(ctx, out, nil) }()
	select {
	case f := <-out:
		return f.Record.Offset
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for a record after seek %+v", spec)
		return -1
	}
}

func TestSeekStartPositions(t *testing.T) {
	ctx := context.Background()
	rp, err := redpanda.Run(ctx, "redpandadata/redpanda:latest")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	defer func() { _ = rp.Terminate(ctx) }()

	broker, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}
	cl := config.Cluster{Brokers: []string{broker}, Auth: config.Auth{Type: "plaintext"}}

	const topic = "franta-seek-it"

	// Single-partition topic so offsets are deterministic.
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	adm := kadm.NewClient(adminClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	adminClient.Close()

	// Produce offsets 0..9.
	prodClient, err := NewClient(cl)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	prod := NewProducer(prodClient)
	for i := 0; i < 10; i++ {
		if err := prod.Produce(ctx, record.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("%d", i)),
			Value: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	prodClient.Close()

	if off := firstOffset(t, cl, topic, StartSpec{Kind: StartBeginning}); off != 0 {
		t.Fatalf("beginning: first offset = %d, want 0", off)
	}
	if off := firstOffset(t, cl, topic, StartSpec{Kind: StartLastN, N: 3}); off != 7 {
		t.Fatalf("last:3: first offset = %d, want 7", off)
	}
}
