// Package session owns per-cluster Kafka wiring and live cluster switching.
package session

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"franta/internal/config"
	"franta/internal/decode"
	"franta/internal/kafka"
)

// Session is the Kafka wiring for one cluster.
type Session struct {
	Name       string
	Client     *kgo.Client // consumer/admin client
	ProdClient *kgo.Client // producer client
	Cons       *kafka.Consumer
	Producer   *kafka.Producer
	Dec        decode.Decoder // nil when the cluster has no schema registry

	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{} // closed when the consumer goroutine returns; nil until started
	stopped bool
}

// buildDecoder constructs a Decoder from a cluster's schema_registry config, or
// (nil, nil) when none is configured.
func buildDecoder(cl config.Cluster) (decode.Decoder, error) {
	if cl.SchemaRegistry == nil {
		return nil, nil
	}
	switch cl.SchemaRegistry.Type {
	case "", "confluent":
		pw, err := cl.SchemaRegistry.ResolvePassword()
		if err != nil {
			return nil, err
		}
		return decode.NewConfluent(cl.SchemaRegistry.URL, cl.SchemaRegistry.Username, pw)
	case "glue":
		return decode.NewGlue(
			cl.SchemaRegistry.Region,
			cl.SchemaRegistry.Profile,
			cl.SchemaRegistry.RegistryName,
			cl.SchemaRegistry.Endpoint,
		)
	default:
		return nil, fmt.Errorf("schema_registry: unknown type %q", cl.SchemaRegistry.Type)
	}
}

// Build constructs (without starting) a Session for a cluster, parented to ctx.
// topic may be "" (no initial consume topic → land in the topic picker).
func Build(ctx context.Context, cl config.Cluster, name, topic string) (*Session, error) {
	var topics []string
	if topic != "" {
		topics = []string{topic}
	}
	client, err := kafka.NewClient(cl, topics...)
	if err != nil {
		return nil, err
	}
	prod, err := kafka.NewClient(cl)
	if err != nil {
		client.Close()
		return nil, err
	}
	dec, err := buildDecoder(cl)
	if err != nil {
		client.Close()
		prod.Close()
		return nil, fmt.Errorf("schema_registry: %w", err)
	}
	cons := kafka.NewConsumer(client, topic)
	if dec != nil {
		cons.UseDecoder(dec)
	}
	sctx, cancel := context.WithCancel(ctx)
	return &Session{
		Name:       name,
		Client:     client,
		ProdClient: prod,
		Cons:       cons,
		Producer:   kafka.NewProducer(prod),
		Dec:        dec,
		ctx:        sctx,
		cancel:     cancel,
	}, nil
}

// Context returns the session's cancellable context (used by callbacks for
// per-session RPCs and by Seek/Switch).
func (s *Session) Context() context.Context { return s.ctx }

// start launches the consumer goroutine feeding records/errs. It does NOT close
// the channels — main owns them across switches; the goroutine closes done.
func (s *Session) start(records chan<- kafka.Fetched, errs chan<- error) {
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		rerr := s.Cons.Run(s.ctx, records, errs)
		if rerr != nil && s.ctx.Err() == nil {
			select {
			case errs <- fmt.Errorf("consumer stopped: %w", rerr):
			default:
			}
		}
	}()
}

// Start is the exported entry point for the initial session built in main.
func (s *Session) Start(records chan<- kafka.Fetched, errs chan<- error) {
	s.start(records, errs)
}

// Gen returns the consumer's current generation (0 before any consume starts).
func (s *Session) Gen() int64 {
	if s.Cons != nil {
		return s.Cons.Gen()
	}
	return 0
}

// Stop cancels the session and waits for its consumer goroutine (if started),
// then closes the clients. Idempotent and nil-safe.
func (s *Session) Stop() {
	if s == nil || s.stopped {
		return
	}
	s.stopped = true
	if s.cancel != nil {
		s.cancel()
	}
	if s.done != nil {
		<-s.done
	}
	if s.Client != nil {
		s.Client.Close()
	}
	if s.ProdClient != nil {
		s.ProdClient.Close()
	}
}
