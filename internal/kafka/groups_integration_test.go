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

// newIntegrationClient starts a redpanda container and returns a connected
// *kgo.Client plus a *kadm.Client wrapping it. The container is terminated when
// the test finishes.
func newIntegrationClient(t *testing.T) (*kgo.Client, *kadm.Client) {
	t.Helper()
	ctx := context.Background()
	rp, err := redpanda.Run(ctx, "redpandadata/redpanda:latest")
	if err != nil {
		t.Fatalf("start redpanda: %v", err)
	}
	t.Cleanup(func() { _ = rp.Terminate(ctx) })

	broker, err := rp.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("seed broker: %v", err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl, kadm.NewClient(cl)
}

func TestResetGroupOffsetsBeginningEnd(t *testing.T) {
	ctx := context.Background()
	cl, adm := newIntegrationClient(t)
	topic := "reset-be"
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	group := "g-reset-be"
	// Commit an arbitrary offset so the group has a committed topic-partition.
	var os kadm.Offsets
	os.AddOffset(topic, 0, 5, -1)
	if _, err := adm.CommitOffsets(ctx, group, os); err != nil {
		t.Fatal(err)
	}

	if err := ResetGroupOffsets(ctx, cl, group, ResetSpec{Kind: ResetBeginning}); err != nil {
		t.Fatalf("reset beginning: %v", err)
	}
	got, _ := adm.FetchOffsets(ctx, group)
	if o, ok := got.Lookup(topic, 0); !ok || o.At != 0 {
		t.Fatalf("after beginning: %+v ok=%v", o, ok)
	}

	if err := ResetGroupOffsets(ctx, cl, group, ResetSpec{Kind: ResetEnd}); err != nil {
		t.Fatalf("reset end: %v", err)
	}
	end, _ := adm.ListEndOffsets(ctx, topic)
	eo, _ := end.Lookup(topic, 0)
	got, _ = adm.FetchOffsets(ctx, group)
	if o, _ := got.Lookup(topic, 0); o.At != eo.Offset {
		t.Fatalf("after end: committed=%d end=%d", o.At, eo.Offset)
	}
}

func TestResetGroupOffsetsExplicit(t *testing.T) {
	ctx := context.Background()
	cl, adm := newIntegrationClient(t)
	topic := "reset-x"
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	group := "g-reset-x"
	var os kadm.Offsets
	os.AddOffset(topic, 0, 1, -1)
	if _, err := adm.CommitOffsets(ctx, group, os); err != nil {
		t.Fatal(err)
	}
	spec := ResetSpec{Kind: ResetExplicit, Offsets: map[string]map[int32]int64{topic: {0: 3}}}
	if err := ResetGroupOffsets(ctx, cl, group, spec); err != nil {
		t.Fatal(err)
	}
	got, _ := adm.FetchOffsets(ctx, group)
	if o, _ := got.Lookup(topic, 0); o.At != 3 {
		t.Fatalf("explicit: %d", o.At)
	}
}

func TestDeleteGroup(t *testing.T) {
	ctx := context.Background()
	cl, adm := newIntegrationClient(t)
	topic := "del-grp"
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	group := "g-del"
	var os kadm.Offsets
	os.AddOffset(topic, 0, 1, -1)
	if _, err := adm.CommitOffsets(ctx, group, os); err != nil {
		t.Fatal(err)
	}
	if err := DeleteGroup(ctx, cl, group); err != nil {
		t.Fatalf("delete: %v", err)
	}
	listed, _ := adm.ListGroups(ctx)
	if _, ok := listed[group]; ok {
		t.Fatal("group should be gone")
	}
}
