//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"franta/internal/config"
	"franta/internal/record"
)

func TestProduceConsumeRoundTrip(t *testing.T) {
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

	const topic = "franta-it"

	// Create the topic explicitly before consumer/producer connect.
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("admin kgo client: %v", err)
	}
	adm := kadm.NewClient(adminClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	adminClient.Close()

	consClient, err := NewClient(cl, topic)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consClient.Close()

	prodClient, err := NewClient(cl)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer prodClient.Close()

	out := make(chan Fetched, 4)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = NewConsumer(consClient, topic).Run(cctx, out, nil) }()

	// Give the consumer a moment to position at the end before producing.
	time.Sleep(2 * time.Second)
	if err := NewProducer(prodClient).Produce(ctx, record.Record{
		Topic: topic, Key: []byte("k"), Value: []byte(`{"hello":"world"}`),
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	select {
	case f := <-out:
		if string(f.Record.Key) != "k" {
			t.Fatalf("key = %q, want k", f.Record.Key)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for record")
	}
}
