package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestUnderReplicated(t *testing.T) {
	cases := []struct {
		name     string
		replicas []int32
		isr      []int32
		want     bool
	}{
		{"in sync", []int32{1, 2, 3}, []int32{1, 2, 3}, false},
		{"one out", []int32{1, 2, 3}, []int32{1, 2}, true},
		{"all out", []int32{1, 2, 3}, []int32{}, true},
		{"single replica in sync", []int32{1}, []int32{1}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := PartitionInfo{Replicas: c.replicas, ISR: c.isr}
			if got := p.UnderReplicated(); got != c.want {
				t.Fatalf("UnderReplicated(%v,%v)=%v want %v", c.replicas, c.isr, got, c.want)
			}
		})
	}
}

func strptr(s string) *string { return &s }

func TestMetaToCluster(t *testing.T) {
	rackA := strptr("a")
	md := kadm.Metadata{
		Cluster:    "test-cluster",
		Controller: 3,
		Brokers: kadm.BrokerDetails{
			// Intentionally out of order to verify sort by ID.
			{NodeID: 2, Host: "h2", Port: 9092, Rack: rackA},
			{NodeID: 1, Host: "h1", Port: 9092, Rack: nil}, // no rack
			{NodeID: 3, Host: "h3", Port: 9092, Rack: rackA},
		},
		Topics: kadm.TopicDetails{
			"orders": kadm.TopicDetail{
				Topic: "orders",
				Partitions: kadm.PartitionDetails{
					1: {Partition: 1, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2}},
					0: {Partition: 0, Leader: 3, Replicas: []int32{3, 1, 2}, ISR: []int32{3, 1, 2}},
				},
			},
		},
	}

	got := metaToCluster(md, "orders")

	if got.ClusterID != "test-cluster" {
		t.Errorf("ClusterID=%q want test-cluster", got.ClusterID)
	}
	if got.Controller != 3 {
		t.Errorf("Controller=%d want 3", got.Controller)
	}
	if got.Topic != "orders" {
		t.Errorf("Topic=%q want orders", got.Topic)
	}
	// Brokers sorted by ID.
	if len(got.Brokers) != 3 {
		t.Fatalf("brokers len=%d want 3", len(got.Brokers))
	}
	wantIDs := []int32{1, 2, 3}
	for i, b := range got.Brokers {
		if b.ID != wantIDs[i] {
			t.Errorf("broker[%d].ID=%d want %d", i, b.ID, wantIDs[i])
		}
	}
	if got.Brokers[0].Rack != "" {
		t.Errorf("broker 1 rack=%q want empty", got.Brokers[0].Rack)
	}
	if got.Brokers[1].Rack != "a" {
		t.Errorf("broker 2 rack=%q want a", got.Brokers[1].Rack)
	}
	// Partitions sorted by Partition.
	if len(got.Partitions) != 2 {
		t.Fatalf("partitions len=%d want 2", len(got.Partitions))
	}
	if got.Partitions[0].Partition != 0 || got.Partitions[1].Partition != 1 {
		t.Errorf("partitions not sorted: %d,%d", got.Partitions[0].Partition, got.Partitions[1].Partition)
	}
	if got.Partitions[1].Leader != 1 {
		t.Errorf("p1 leader=%d want 1", got.Partitions[1].Leader)
	}
	if !got.Partitions[1].UnderReplicated() {
		t.Errorf("p1 should be under-replicated (isr 1,2 < repl 1,2,3)")
	}
	if got.Partitions[0].UnderReplicated() {
		t.Errorf("p0 should be fully replicated")
	}
}

func TestMetaToClusterNoTopic(t *testing.T) {
	md := kadm.Metadata{
		Cluster:    "c",
		Controller: 1,
		Brokers:    kadm.BrokerDetails{{NodeID: 1, Host: "h", Port: 9092}},
		Topics:     kadm.TopicDetails{},
	}
	got := metaToCluster(md, "")
	if got.Topic != "" {
		t.Errorf("Topic=%q want empty", got.Topic)
	}
	if len(got.Partitions) != 0 {
		t.Errorf("partitions=%d want 0", len(got.Partitions))
	}
	if len(got.Brokers) != 1 {
		t.Errorf("brokers=%d want 1", len(got.Brokers))
	}
}

// ensure kgo import is used (BrokerMetadata is the element type of BrokerDetails)
var _ = kgo.BrokerMetadata{}
