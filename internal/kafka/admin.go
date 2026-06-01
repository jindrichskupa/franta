package kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TopicInfo summarizes a topic for the picker.
type TopicInfo struct {
	Name       string
	Partitions int
	Messages   int64 // approx: sum(endOffset - startOffset) across partitions
}

// buildTopicInfos aggregates partition counts and start/end offsets into sorted
// TopicInfos. Internal topics (name prefixed "_") are excluded unless
// includeInternal. A topic with no end-offset entries gets Messages = -1
// (unknown) so the UI can distinguish "no data fetched" from "genuinely 0".
func buildTopicInfos(partitions map[string]int, starts, ends map[string]map[int32]int64, includeInternal bool) []TopicInfo {
	out := make([]TopicInfo, 0, len(partitions))
	for name, np := range partitions {
		if !includeInternal && strings.HasPrefix(name, "_") {
			continue
		}
		endPartitions, hasEnd := ends[name]
		info := TopicInfo{Name: name, Partitions: np}
		if !hasEnd || len(endPartitions) == 0 {
			info.Messages = -1
		} else {
			var msgs int64
			for p, e := range endPartitions {
				s := starts[name][p]
				if d := e - s; d > 0 {
					msgs += d
				}
			}
			info.Messages = msgs
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ListTopicsBasic returns the cluster's topics with names and partition counts
// only (Messages=-1 → "unknown"). Single Metadata round-trip; the costly
// per-partition offset fetch lives in FetchTopicOffsets so the UI can paint
// the list immediately and fill counts in a second phase.
func ListTopicsBasic(ctx context.Context, cl *kgo.Client, includeInternal bool) ([]TopicInfo, error) {
	adm := kadm.NewClient(cl)
	td, err := adm.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]TopicInfo, 0, len(td))
	for name, d := range td {
		if d.Err != nil {
			continue
		}
		if !includeInternal && strings.HasPrefix(name, "_") {
			continue
		}
		out = append(out, TopicInfo{Name: name, Partitions: len(d.Partitions), Messages: -1})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// FetchTopicOffsets returns the approximate message count per topic (sum of
// endOffset - startOffset across partitions). Start and end offsets are
// fetched in parallel — each is a broker fan-out, so concurrent fetch
// ~halves wall-clock time. Per-partition errors are surfaced as a non-fatal
// warning (returned alongside the partial result).
func FetchTopicOffsets(ctx context.Context, cl *kgo.Client, names []string) (map[string]int64, error) {
	if len(names) == 0 {
		return map[string]int64{}, nil
	}
	adm := kadm.NewClient(cl)
	var (
		starts, ends kadm.ListedOffsets
		serr, eerr   error
		wg           sync.WaitGroup
	)
	wg.Add(2)
	go func() { defer wg.Done(); starts, serr = adm.ListStartOffsets(ctx, names...) }()
	go func() { defer wg.Done(); ends, eerr = adm.ListEndOffsets(ctx, names...) }()
	wg.Wait()
	if serr != nil {
		return nil, serr
	}
	if eerr != nil {
		return nil, eerr
	}
	startMap, startErr := offsetsToMap(starts)
	endMap, endErr := offsetsToMap(ends)
	counts := make(map[string]int64, len(endMap))
	for topic, parts := range endMap {
		var msgs int64
		for p, e := range parts {
			s := startMap[topic][p]
			if d := e - s; d > 0 {
				msgs += d
			}
		}
		counts[topic] = msgs
	}
	var warn error
	if endErr != nil {
		warn = fmt.Errorf("some end offsets failed: %w", endErr)
	} else if startErr != nil {
		warn = fmt.Errorf("some start offsets failed: %w", startErr)
	}
	return counts, warn
}

// ListTopics is the legacy single-call API: basic list + per-topic offset
// counts in one shot. Retained for tests and callers that don't need
// progressive load. Internal topics are excluded unless includeInternal.
func ListTopics(ctx context.Context, cl *kgo.Client, includeInternal bool) ([]TopicInfo, error) {
	basics, err := ListTopicsBasic(ctx, cl, includeInternal)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(basics))
	for i, t := range basics {
		names[i] = t.Name
	}
	counts, warn := FetchTopicOffsets(ctx, cl, names)
	for i, t := range basics {
		if c, ok := counts[t.Name]; ok {
			basics[i].Messages = c
		}
	}
	return basics, warn
}

// offsetsToMap flattens kadm listed offsets into topic->partition->offset,
// skipping errored or negative entries. The returned error is the first
// per-partition error encountered (best-effort diagnostic; callers may show
// it as a non-fatal warning).
func offsetsToMap(lo kadm.ListedOffsets) (map[string]map[int32]int64, error) {
	out := make(map[string]map[int32]int64, len(lo))
	var firstErr error
	for topic, parts := range lo {
		m := make(map[int32]int64, len(parts))
		for p, l := range parts {
			if l.Err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("%s[%d]: %w", topic, p, l.Err)
				}
				continue
			}
			if l.Offset < 0 {
				continue
			}
			m[p] = l.Offset
		}
		out[topic] = m
	}
	return out, firstErr
}
