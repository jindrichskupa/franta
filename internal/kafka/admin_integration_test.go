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

func TestListTopicsAndSwitch(t *testing.T) {
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

	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	adm := kadm.NewClient(admClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, "alpha", "beta"); err != nil {
		t.Fatalf("create topics: %v", err)
	}
	admClient.Close()

	// Produce 1 record to alpha, 2 to beta.
	prodClient, err := NewClient(cl)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	prod := NewProducer(prodClient)
	mustProduce(t, ctx, prod, "alpha", 1)
	mustProduce(t, ctx, prod, "beta", 2)
	prodClient.Close()

	listClient, err := NewClient(cl)
	if err != nil {
		t.Fatalf("list client: %v", err)
	}
	defer listClient.Close()
	topics, err := ListTopics(ctx, listClient, false)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	byName := map[string]TopicInfo{}
	for _, ti := range topics {
		byName[ti.Name] = ti
	}
	if byName["alpha"].Messages != 1 || byName["beta"].Messages != 2 {
		t.Fatalf("message counts = alpha:%d beta:%d, want 1/2", byName["alpha"].Messages, byName["beta"].Messages)
	}

	// Start consuming alpha, then switch to beta and confirm beta's records flow.
	consClient, err := NewClient(cl, "alpha")
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer consClient.Close()
	cons := NewConsumer(consClient, "alpha")
	if err := cons.Seek(ctx, StartSpec{Kind: StartBeginning}); err != nil {
		t.Fatalf("seek alpha: %v", err)
	}
	out := make(chan Fetched, 16)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = cons.Run(cctx, out, nil) }()

	// drain alpha's record
	select {
	case f := <-out:
		if f.Record.Topic != "alpha" {
			t.Fatalf("first record topic = %q, want alpha", f.Record.Topic)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out reading alpha")
	}

	if err := cons.SwitchTopic(ctx, "beta"); err != nil {
		t.Fatalf("switch to beta: %v", err)
	}
	// Need beta from beginning to see pre-existing records; SwitchTopic tails
	// from end, so produce a fresh beta record to observe post-switch delivery.
	prodClient2, _ := NewClient(cl)
	mustProduce(t, ctx, NewProducer(prodClient2), "beta", 1)
	prodClient2.Close()

	deadline := time.After(15 * time.Second)
	for {
		select {
		case f := <-out:
			if f.Record.Topic == "beta" {
				return // success
			}
		case <-deadline:
			t.Fatal("timed out waiting for a beta record after switch")
		}
	}
}

func mustProduce(t *testing.T, ctx context.Context, p *Producer, topic string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := p.Produce(ctx, record.Record{Topic: topic, Value: []byte(fmt.Sprintf(`{"i":%d}`, i))}); err != nil {
			t.Fatalf("produce %s: %v", topic, err)
		}
	}
}

func TestCreateDeleteTopic(t *testing.T) {
	ctx := context.Background()
	cl, adm := newIntegrationClient(t)
	name := "admin-cd"
	if err := CreateTopic(ctx, cl, name, 3, 1, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	td, err := adm.ListTopics(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if d, ok := td[name]; !ok || len(d.Partitions) != 3 {
		t.Fatalf("topic not created with 3 partitions: %+v", td[name])
	}
	if err := DeleteTopic(ctx, cl, name); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestAddPartitions(t *testing.T) {
	ctx := context.Background()
	cl, adm := newIntegrationClient(t)
	name := "admin-ap"
	if err := CreateTopic(ctx, cl, name, 2, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := AddPartitions(ctx, cl, name, 4); err != nil {
		t.Fatalf("add partitions: %v", err)
	}
	td, _ := adm.ListTopics(ctx, name)
	if len(td[name].Partitions) != 4 {
		t.Fatalf("want 4 partitions, got %d", len(td[name].Partitions))
	}
	// Reducing must fail.
	if err := AddPartitions(ctx, cl, name, 1); err == nil {
		t.Fatal("reducing partitions should error")
	}
}

func TestGetSetTopicConfig(t *testing.T) {
	ctx := context.Background()
	cl, _ := newIntegrationClient(t)
	name := "admin-cfg"
	if err := CreateTopic(ctx, cl, name, 1, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := SetTopicConfig(ctx, cl, name, map[string]string{"retention.ms": "1234000"}); err != nil {
		t.Fatalf("set: %v", err)
	}
	entries, err := GetTopicConfig(ctx, cl, name)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range entries {
		if e.Key == "retention.ms" {
			found = true
			if e.Value != "1234000" {
				t.Fatalf("retention.ms = %q", e.Value)
			}
			if !e.Editable {
				t.Fatal("dynamic config should be Editable")
			}
		}
	}
	if !found {
		t.Fatal("retention.ms not returned")
	}
}
