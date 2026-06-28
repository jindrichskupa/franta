package kafka

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BrokerInfo is one broker in the cluster.
type BrokerInfo struct {
	ID   int32
	Host string
	Port int32
	Rack string // "" when the broker reports no rack
}

// PartitionInfo is the replication state of one topic partition.
type PartitionInfo struct {
	Partition int32
	Leader    int32 // -1 when offline / no leader
	Replicas  []int32
	ISR       []int32
}

// UnderReplicated reports whether fewer replicas are in-sync than assigned.
func (p PartitionInfo) UnderReplicated() bool { return len(p.ISR) < len(p.Replicas) }

// ClusterMeta is a snapshot of cluster-wide metadata plus, optionally, the
// per-partition detail of a single topic.
type ClusterMeta struct {
	ClusterID  string
	Controller int32
	Brokers    []BrokerInfo
	Topic      string          // echo of the requested topic ("" → no partitions)
	Partitions []PartitionInfo // sorted by Partition asc
}

// DescribeCluster fetches broker + controller metadata and, when topic is
// non-empty, that topic's partition replication detail — in a single Metadata
// round-trip.
func DescribeCluster(ctx context.Context, cl *kgo.Client, topic string) (ClusterMeta, error) {
	adm := kadm.NewClient(cl)
	var md kadm.Metadata
	var err error
	if topic == "" {
		md, err = adm.Metadata(ctx)
	} else {
		md, err = adm.Metadata(ctx, topic)
	}
	if err != nil {
		return ClusterMeta{}, err
	}
	return metaToCluster(md, topic), nil
}

// metaToCluster maps a kadm.Metadata into ClusterMeta, sorting brokers by ID
// and partitions by number. Kept separate from DescribeCluster so it is
// testable without a live broker.
func metaToCluster(md kadm.Metadata, topic string) ClusterMeta {
	out := ClusterMeta{
		ClusterID:  md.Cluster,
		Controller: md.Controller,
		Topic:      topic,
	}
	brokers := append(kadm.BrokerDetails(nil), md.Brokers...)
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].NodeID < brokers[j].NodeID })
	for _, b := range brokers {
		rack := ""
		if b.Rack != nil {
			rack = *b.Rack
		}
		out.Brokers = append(out.Brokers, BrokerInfo{
			ID:   b.NodeID,
			Host: b.Host,
			Port: b.Port,
			Rack: rack,
		})
	}
	if topic != "" {
		if td, ok := md.Topics[topic]; ok {
			for _, p := range td.Partitions.Sorted() {
				out.Partitions = append(out.Partitions, PartitionInfo{
					Partition: p.Partition,
					Leader:    p.Leader,
					Replicas:  p.Replicas,
					ISR:       p.ISR,
				})
			}
		}
	}
	return out
}
