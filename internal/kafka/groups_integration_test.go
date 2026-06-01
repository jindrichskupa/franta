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

func TestListAndDescribeGroup(t *testing.T) {
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

	const topic = "grp-it"
	const group = "grp-it-consumers"

	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	adm := kadm.NewClient(admClient)
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Produce 5 records (offsets 0..4).
	prodClient, err := NewClient(cl)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	prod := NewProducer(prodClient)
	for i := 0; i < 5; i++ {
		if err := prod.Produce(ctx, record.Record{Topic: topic, Value: []byte(fmt.Sprintf(`{"i":%d}`, i))}); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	prodClient.Close()

	// Commit offset 2 for the group on partition 0 -> lag should be 5 - 2 = 3.
	var commit kadm.Offsets
	commit.Add(kadm.Offset{Topic: topic, Partition: 0, At: 2})
	if _, err := adm.CommitOffsets(ctx, group, commit); err != nil {
		t.Fatalf("commit offsets: %v", err)
	}
	admClient.Close()

	listClient, err := NewClient(cl)
	if err != nil {
		t.Fatalf("list client: %v", err)
	}
	defer listClient.Close()

	// The group may take a moment to be listable; poll briefly.
	var found bool
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		groups, err := ListGroups(ctx, listClient)
		if err != nil {
			t.Fatalf("ListGroups: %v", err)
		}
		for _, g := range groups {
			if g.Name == group {
				found = true
			}
		}
		if found {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !found {
		t.Fatalf("group %q not listed", group)
	}

	d, err := DescribeGroup(ctx, listClient, group)
	if err != nil {
		t.Fatalf("DescribeGroup: %v", err)
	}
	if d.TotalLag != 3 {
		t.Fatalf("total lag = %d, want 3", d.TotalLag)
	}
}
