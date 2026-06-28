package tui

import (
	"strings"
	"testing"

	"franta/internal/kafka"
)

func sampleMeta() kafka.ClusterMeta {
	return kafka.ClusterMeta{
		ClusterID:  "abc",
		Controller: 3,
		Brokers: []kafka.BrokerInfo{
			{ID: 1, Host: "10.0.4.1", Port: 9092, Rack: "a"},
			{ID: 2, Host: "10.0.4.2", Port: 9092, Rack: "a"},
			{ID: 3, Host: "10.0.4.3", Port: 9092, Rack: "b"},
		},
		Topic: "orders",
		Partitions: []kafka.PartitionInfo{
			{Partition: 0, Leader: 3, Replicas: []int32{3, 1, 2}, ISR: []int32{3, 1, 2}},
			{Partition: 1, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2}}, // under-replicated
		},
	}
}

func TestRenderBrokerListControllerMark(t *testing.T) {
	m := sampleMeta()
	out := renderBrokerList(m.Brokers, m.Controller, 0, true)
	lines := strings.Split(out, "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 broker lines, got %d:\n%s", len(lines), out)
	}
	// Controller (id 3) is the third row → must carry the ● mark; others must not.
	if !strings.Contains(lines[2], "●") {
		t.Errorf("controller row missing ●: %q", lines[2])
	}
	if strings.Contains(lines[0], "●") || strings.Contains(lines[1], "●") {
		t.Errorf("non-controller row has ●:\n%s", out)
	}
	if !strings.Contains(lines[0], "10.0.4.1") {
		t.Errorf("broker host missing: %q", lines[0])
	}
}

func TestRenderBrokerListCursor(t *testing.T) {
	m := sampleMeta()
	out := renderBrokerList(m.Brokers, m.Controller, 1, true)
	lines := strings.Split(out, "\n")
	if !strings.HasPrefix(strings.TrimSpace(lines[1]), ">") {
		t.Errorf("cursor not on row 1: %q", lines[1])
	}
	if strings.HasPrefix(strings.TrimSpace(lines[0]), ">") {
		t.Errorf("cursor wrongly on row 0: %q", lines[0])
	}
}

func TestRenderPartitionTableWarn(t *testing.T) {
	m := sampleMeta()
	out := renderPartitionTable(m.Partitions)
	if !strings.Contains(out, "⚠") {
		t.Errorf("under-replicated partition missing ⚠:\n%s", out)
	}
	// Exactly one warned row.
	if strings.Count(out, "⚠") != 1 {
		t.Errorf("want exactly 1 ⚠, got %d:\n%s", strings.Count(out, "⚠"), out)
	}
	// Replica/ISR lists rendered as comma-joined ids.
	if !strings.Contains(out, "3,1,2") {
		t.Errorf("replica list not comma-joined:\n%s", out)
	}
}

func TestReplicationFactor(t *testing.T) {
	m := sampleMeta()
	if rf := replicationFactor(m.Partitions); rf != 3 {
		t.Errorf("rf=%d want 3", rf)
	}
	if rf := replicationFactor(nil); rf != 0 {
		t.Errorf("rf(empty)=%d want 0", rf)
	}
}
