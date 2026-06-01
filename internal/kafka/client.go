// Package kafka wraps franz-go for franta's consume/produce needs.
package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"franta/internal/config"
	"franta/internal/kafka/auth"
)

// NewClient builds a franz-go client for the cluster. If topics are given the
// client is configured to consume them from the latest offset (live tail).
// The client connects lazily on first use.
func NewClient(c config.Cluster, topics ...string) (*kgo.Client, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...)}
	authOpts, err := auth.Build(c)
	if err != nil {
		return nil, err
	}
	opts = append(opts, authOpts...)
	if len(topics) > 0 {
		opts = append(opts,
			kgo.ConsumeTopics(topics...),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		)
	}
	return kgo.NewClient(opts...)
}
