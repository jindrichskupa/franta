//go:build integration

package kafka

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDescribeCluster(t *testing.T) {
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

	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cl.Close()

	const topic = "cluster-it"
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopics(ctx, 3, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	meta, err := DescribeCluster(ctx, cl, topic)
	if err != nil {
		t.Fatalf("DescribeCluster: %v", err)
	}

	if len(meta.Brokers) < 1 {
		t.Fatalf("want at least 1 broker, got %d", len(meta.Brokers))
	}
	if meta.Topic != topic {
		t.Errorf("Topic=%q want %q", meta.Topic, topic)
	}
	if len(meta.Partitions) != 3 {
		t.Fatalf("partitions=%d want 3", len(meta.Partitions))
	}
	for i, p := range meta.Partitions {
		if int(p.Partition) != i {
			t.Errorf("partitions out of order: [%d].Partition=%d", i, p.Partition)
		}
		if p.Leader < 0 {
			t.Errorf("partition %d has no leader", p.Partition)
		}
		if len(p.Replicas) != 1 {
			t.Errorf("partition %d replicas=%v want 1 (rf=1)", p.Partition, p.Replicas)
		}
		if len(p.ISR) != 1 {
			t.Errorf("partition %d isr=%v want 1", p.Partition, p.ISR)
		}
		if p.UnderReplicated() {
			t.Errorf("partition %d should be in-sync at rf=1", p.Partition)
		}
	}

	// Empty topic → brokers only, no partitions.
	bare, err := DescribeCluster(ctx, cl, "")
	if err != nil {
		t.Fatalf("DescribeCluster(no topic): %v", err)
	}
	if len(bare.Partitions) != 0 {
		t.Errorf("no-topic partitions=%d want 0", len(bare.Partitions))
	}
	if len(bare.Brokers) < 1 {
		t.Errorf("no-topic brokers=%d want >=1", len(bare.Brokers))
	}
}
