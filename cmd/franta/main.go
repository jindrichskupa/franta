package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/atotto/clipboard"

	"franta/internal/config"
	"franta/internal/decode"
	"franta/internal/kafka"
	"franta/internal/query"
	"franta/internal/record"
	"franta/internal/tui"
)

// version, commit, date are injected at build time via ldflags by goreleaser.
// Defaults make `go run` / `go build` produce something sensible.
var (
	version = "0.0.0-dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "version":
			fmt.Printf("franta %s (commit %s, built %s)\n", version, commit, date)
			return
		case "consume":
			if err := runConsume(os.Args[2:]); err != nil {
				fmt.Fprintln(os.Stderr, "franta:", err)
				os.Exit(1)
			}
			return
		}
	}
	if err := runTUI(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "franta:", err)
		os.Exit(1)
	}
}

// resolveCluster loads config and resolves a cluster (prompting with the picker
// when no name is given and there is no default), returning its name and config.
func resolveCluster(cfgPath, name string) (string, config.Cluster, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return "", config.Cluster{}, err
	}
	if err := cfg.Validate(); err != nil {
		return "", config.Cluster{}, err
	}
	if name == "" && cfg.DefaultCluster == "" {
		names := make([]string, 0, len(cfg.Clusters))
		for n := range cfg.Clusters {
			names = append(names, n)
		}
		chosen, err := tui.PickCluster(names)
		if err != nil {
			return "", config.Cluster{}, err
		}
		if chosen == "" {
			return "", config.Cluster{}, fmt.Errorf("no cluster selected")
		}
		name = chosen
	}
	resolvedName, cl, err := cfg.Resolve(name)
	return resolvedName, cl, err
}

// resolveTopic returns the given topic, or runs a startup picker over the
// cluster's topics when topic is empty.
func resolveTopic(ctx context.Context, clusterName string, cl config.Cluster, topic string) (string, error) {
	if topic != "" {
		return topic, nil
	}
	adminClient, err := kafka.NewClient(cl)
	if err != nil {
		return "", err
	}
	defer adminClient.Close()
	topics, err := kafka.ListTopics(ctx, adminClient, false)
	if err != nil {
		return "", err
	}
	if len(topics) == 0 {
		return "", fmt.Errorf("cluster %q has no topics", clusterName)
	}
	chosen, err := tui.PickTopic(clusterName, topics)
	if err != nil {
		return "", err
	}
	if chosen == "" {
		return "", fmt.Errorf("no topic selected")
	}
	return chosen, nil
}

func runTUI(args []string) error {
	fs := flag.NewFlagSet("franta", flag.ExitOnError)
	cfgPath := fs.String("config", config.DefaultPath(), "path to config file")
	cluster := fs.String("cluster", "", "cluster name")
	// Default empty so we can tell "user didn't set it" apart from "user picked
	// end explicitly"; cluster.default_seek fills it in when unset.
	from := fs.String("from", "", "start position: end | beginning | last:N | <duration> | RFC3339 (defaults to cluster.default_seek or end)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("usage: franta [--config PATH] [--cluster NAME] [--from POS] [TOPIC]")
	}
	argTopic := fs.Arg(0) // may be empty -> startup picker

	name, cl, err := resolveCluster(*cfgPath, *cluster)
	if err != nil {
		return err
	}

	fromValue := *from
	if fromValue == "" {
		fromValue = cl.DefaultSeek
	}
	if fromValue == "" {
		fromValue = "end"
	}
	spec, err := kafka.ParseStart(fromValue)
	if err != nil {
		return err
	}

	// Load saved filters: inline (config.yaml) + side-file (filters.yaml) so
	// the user can edit by hand or save from the TUI.
	sideFilters, _ := config.LoadFilters(config.FiltersPath(*cfgPath))
	cfg, _ := config.Load(*cfgPath)
	var inlineFilters []config.SavedFilter
	if cfg != nil {
		inlineFilters = cfg.SavedFilters
	}
	savedFilters := config.MergeFilters(inlineFilters, sideFilters)
	filtersPath := config.FiltersPath(*cfgPath)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Don't run a separate startup picker when TOPIC is omitted — the 3-pane
	// TUI's left pane is the topic picker. Start with no consume topic; the
	// user picks one via the topics pane (auto-loaded on Init), and the
	// existing SwitchTopic flow then begins consumption.
	topic := argTopic

	var topics []string
	if topic != "" {
		topics = []string{topic}
	}
	consClient, err := kafka.NewClient(cl, topics...)
	if err != nil {
		return err
	}
	defer consClient.Close()
	prodClient, err := kafka.NewClient(cl)
	if err != nil {
		return err
	}
	defer prodClient.Close()

	cons := kafka.NewConsumer(consClient, topic)
	if cl.SchemaRegistry != nil {
		var (
			dec decode.Decoder
			err error
		)
		switch cl.SchemaRegistry.Type {
		case "", "confluent":
			pw, perr := cl.SchemaRegistry.ResolvePassword()
			if perr != nil {
				return perr
			}
			dec, err = decode.NewConfluent(cl.SchemaRegistry.URL, cl.SchemaRegistry.Username, pw)
		case "glue":
			dec, err = decode.NewGlue(
				cl.SchemaRegistry.Region,
				cl.SchemaRegistry.Profile,
				cl.SchemaRegistry.RegistryName,
				cl.SchemaRegistry.Endpoint,
			)
		default:
			return fmt.Errorf("schema_registry: unknown type %q", cl.SchemaRegistry.Type)
		}
		if err != nil {
			return fmt.Errorf("schema_registry: %w", err)
		}
		cons.UseDecoder(dec)
	}
	if spec.Kind != kafka.StartEnd {
		if err := cons.Seek(ctx, spec); err != nil {
			return fmt.Errorf("seek: %w", err)
		}
	}

	records := make(chan kafka.Fetched, 128)
	errs := make(chan error, 16)
	go func() {
		rerr := cons.Run(ctx, records, errs)
		if rerr != nil && ctx.Err() == nil {
			select {
			case errs <- fmt.Errorf("consumer stopped: %w", rerr):
			default:
			}
		}
		close(records)
		close(errs)
	}()

	producer := kafka.NewProducer(prodClient)
	return tui.Run(records, errs, tui.Callbacks{
		Produce: func(r record.Record) error { return producer.Produce(ctx, r) },
		Seek:    func(s kafka.StartSpec) (int64, error) { return seekGen(ctx, cons, s) },
		ListTopics: func(internal bool) ([]kafka.TopicInfo, error) {
			return kafka.ListTopicsBasic(ctx, consClient, internal)
		},
		TopicOffsets: func(names []string) (map[string]int64, error) {
			return kafka.FetchTopicOffsets(ctx, consClient, names)
		},
		Switch: func(t string) (int64, error) {
			if err := cons.SwitchTopic(ctx, t); err != nil {
				return 0, err
			}
			// Honour the launch --from for switches too: a user who started
			// with --from beginning expects each freshly-picked topic to start
			// from the beginning. StartEnd is the kgo default, no extra seek.
			if spec.Kind != kafka.StartEnd {
				if err := cons.Seek(ctx, spec); err != nil {
					return 0, err
				}
			}
			return cons.Gen(), nil
		},
		Groups: func() ([]kafka.GroupInfo, error) {
			return kafka.ListGroupsBasic(ctx, consClient)
		},
		GroupLags: func(names []string) (map[string]int64, error) {
			return kafka.FetchGroupLags(ctx, consClient, names)
		},
		DescribeGroup: func(n string) (kafka.GroupDetail, error) {
			return kafka.DescribeGroup(ctx, consClient, n)
		},
		ResetOffsets: func(g string, spec kafka.ResetSpec) error {
			return kafka.ResetGroupOffsets(ctx, consClient, g, spec)
		},
		DeleteGroup: func(g string) error {
			return kafka.DeleteGroup(ctx, consClient, g)
		},
		CreateTopic: func(name string, p int32, rf int16, cfg map[string]string) error {
			return kafka.CreateTopic(ctx, consClient, name, p, rf, cfg)
		},
		DeleteTopic: func(name string) error { return kafka.DeleteTopic(ctx, consClient, name) },
		AddPartitions: func(name string, total int) error {
			return kafka.AddPartitions(ctx, consClient, name, total)
		},
		GetTopicConfig: func(name string) ([]kafka.TopicConfigEntry, error) {
			return kafka.GetTopicConfig(ctx, consClient, name)
		},
		SetTopicConfig: func(name string, set map[string]string) error {
			return kafka.SetTopicConfig(ctx, consClient, name, set)
		},
		Cluster: name,
		Topic:   topic,
		SavedFilters: func() []tui.SavedFilter {
			out := make([]tui.SavedFilter, len(savedFilters))
			for i, f := range savedFilters {
				out[i] = tui.SavedFilter{Name: f.Name, Query: f.Query}
			}
			return out
		}(),
		SaveFilter: func(n, q string) error {
			return config.SaveFilter(filtersPath, config.SavedFilter{Name: n, Query: q})
		},
		DeleteFilter: func(n string) error {
			return config.DeleteFilter(filtersPath, n)
		},
		Copy: func(s string) error { return clipboard.WriteAll(s) },
	})
}

// seekGen seeks and returns the new generation.
func seekGen(ctx context.Context, cons *kafka.Consumer, s kafka.StartSpec) (int64, error) {
	if err := cons.Seek(ctx, s); err != nil {
		return 0, err
	}
	return cons.Gen(), nil
}

func runConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	cfgPath := fs.String("config", config.DefaultPath(), "path to config file")
	cluster := fs.String("cluster", "", "cluster name (default: config default_cluster)")
	filter := fs.String("filter", "", "DSL filter query")
	from := fs.String("from", "end", "start position: end | beginning | last:N | <duration> | RFC3339")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("consume takes at most one TOPIC argument")
	}
	topic := fs.Arg(0)

	name, cl, err := resolveCluster(*cfgPath, *cluster)
	if err != nil {
		return err
	}
	pred, err := query.Parse(*filter)
	if err != nil {
		return fmt.Errorf("filter: %w", err)
	}

	spec, err := kafka.ParseStart(*from)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	topic, err = resolveTopic(ctx, name, cl, topic)
	if err != nil {
		return err
	}

	client, err := kafka.NewClient(cl, topic)
	if err != nil {
		return err
	}
	defer client.Close()
	fmt.Fprintf(os.Stderr, "franta: tailing %q (Ctrl-C to stop)\n", topic)

	cons := kafka.NewConsumer(client, topic)
	if cl.SchemaRegistry != nil {
		var (
			dec decode.Decoder
			err error
		)
		switch cl.SchemaRegistry.Type {
		case "", "confluent":
			pw, perr := cl.SchemaRegistry.ResolvePassword()
			if perr != nil {
				return perr
			}
			dec, err = decode.NewConfluent(cl.SchemaRegistry.URL, cl.SchemaRegistry.Username, pw)
		case "glue":
			dec, err = decode.NewGlue(
				cl.SchemaRegistry.Region,
				cl.SchemaRegistry.Profile,
				cl.SchemaRegistry.RegistryName,
				cl.SchemaRegistry.Endpoint,
			)
		default:
			return fmt.Errorf("schema_registry: unknown type %q", cl.SchemaRegistry.Type)
		}
		if err != nil {
			return fmt.Errorf("schema_registry: %w", err)
		}
		cons.UseDecoder(dec)
	}
	if spec.Kind != kafka.StartEnd {
		if err := cons.Seek(ctx, spec); err != nil {
			return fmt.Errorf("seek: %w", err)
		}
	}

	out := make(chan kafka.Fetched, 128)
	errs := make(chan error, 16)
	go func() {
		rerr := cons.Run(ctx, out, errs)
		if rerr != nil && ctx.Err() == nil {
			select {
			case errs <- fmt.Errorf("consumer stopped: %w", rerr):
			default:
			}
		}
		close(out)
		close(errs)
	}()
	go func() {
		for e := range errs {
			fmt.Fprintln(os.Stderr, "franta:", e)
		}
	}()

	enc := json.NewEncoder(os.Stdout)
	for f := range out {
		r := f.Record
		if !pred(r) {
			continue
		}
		_ = enc.Encode(record.ToWire(r))
	}
	return nil
}
