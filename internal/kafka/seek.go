package kafka

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// StartKind is a category of consume start position.
type StartKind int

const (
	StartEnd       StartKind = iota // live edge (default)
	StartBeginning                  // earliest retained offset
	StartTimestamp                  // first record at/after Time
	StartLastN                      // N records back from end, per partition
)

// StartSpec describes where to begin consuming.
type StartSpec struct {
	Kind StartKind
	Time time.Time // for StartTimestamp (UTC)
	N    int64     // for StartLastN
}

// ParseStart parses the --from mini-syntax:
//
//	end | latest | ""   -> StartEnd
//	beginning | start   -> StartBeginning
//	last:N              -> StartLastN (N > 0)
//	<duration>          -> StartTimestamp at now-duration (Go durations, plus Nd days)
//	<RFC3339>           -> StartTimestamp at that absolute time
func ParseStart(s string) (StartSpec, error) {
	raw := strings.TrimSpace(s)
	low := strings.ToLower(raw)
	switch low {
	case "", "end", "latest":
		return StartSpec{Kind: StartEnd}, nil
	case "beginning", "start":
		return StartSpec{Kind: StartBeginning}, nil
	}
	if strings.HasPrefix(low, "last:") {
		n, err := strconv.ParseInt(low[len("last:"):], 10, 64)
		if err != nil || n <= 0 {
			return StartSpec{}, fmt.Errorf("invalid last:N %q (want a positive integer, e.g. last:500)", raw)
		}
		return StartSpec{Kind: StartLastN, N: n}, nil
	}
	if d, err := parseDuration(low); err == nil {
		if d <= 0 {
			return StartSpec{}, fmt.Errorf("invalid --from %q: duration must be positive (a past offset like 1h, 30m, 2d)", raw)
		}
		return StartSpec{Kind: StartTimestamp, Time: time.Now().Add(-d).UTC()}, nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return StartSpec{Kind: StartTimestamp, Time: t.UTC()}, nil
	}
	return StartSpec{}, fmt.Errorf("invalid --from %q: want end | beginning | last:N | <duration like 1h/2d> | RFC3339", raw)
}

// parseDuration extends time.ParseDuration with a bare "<n>d" days form. It
// rejects non-finite values (NaN/Inf) that ParseFloat would otherwise accept.
func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		n, err := strconv.ParseFloat(s[:len(s)-1], 64)
		if err != nil {
			return 0, err
		}
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return 0, fmt.Errorf("non-finite duration %q", s)
		}
		return time.Duration(n * 24 * float64(time.Hour)), nil
	}
	return time.ParseDuration(s)
}

// lastNTargets returns, per partition, the offset N records back from the end,
// floored at the partition's earliest retained offset (and never negative).
func lastNTargets(start, end map[int32]int64, n int64) map[int32]int64 {
	out := make(map[int32]int64, len(end))
	for p, e := range end {
		t := e - n
		if s, ok := start[p]; ok && t < s {
			t = s
		}
		if t < 0 {
			t = 0
		}
		out[p] = t
	}
	return out
}

// Seek repositions the consumer to the given start position by resolving
// offsets via kadm and applying them with the client's SetOffsets. It takes
// effect on the next poll. StartEnd is also achievable by the client's default
// reset, but Seek handles it explicitly so it works for live re-seeks.
func (c *Consumer) Seek(ctx context.Context, spec StartSpec) error {
	topic := c.currentTopic()
	if topic == "" {
		return fmt.Errorf("seek: no topic selected — pick one in the topics pane (enter) before seeking")
	}
	adm := kadm.NewClient(c.cl)
	var set map[string]map[int32]kgo.EpochOffset
	switch spec.Kind {
	case StartEnd:
		lo, err := adm.ListEndOffsets(ctx, topic)
		if err != nil {
			return err
		}
		set, err = toSetOffsets(lo)
		if err != nil {
			return err
		}
	case StartBeginning:
		lo, err := adm.ListStartOffsets(ctx, topic)
		if err != nil {
			return err
		}
		set, err = toSetOffsets(lo)
		if err != nil {
			return err
		}
	case StartTimestamp:
		lo, err := adm.ListOffsetsAfterMilli(ctx, spec.Time.UnixMilli(), topic)
		if err != nil {
			return err
		}
		set, err = toSetOffsets(lo)
		if err != nil {
			return err
		}
	case StartLastN:
		startLO, err := adm.ListStartOffsets(ctx, topic)
		if err != nil {
			return err
		}
		endLO, err := adm.ListEndOffsets(ctx, topic)
		if err != nil {
			return err
		}
		startMap, err := offsetMap(startLO, topic)
		if err != nil {
			return err
		}
		endMap, err := offsetMap(endLO, topic)
		if err != nil {
			return err
		}
		set = map[string]map[int32]kgo.EpochOffset{topic: {}}
		for p, off := range lastNTargets(startMap, endMap, spec.N) {
			set[topic][p] = kgo.EpochOffset{Epoch: -1, Offset: off}
		}
	default:
		return fmt.Errorf("unknown start kind %d", spec.Kind)
	}
	c.cl.SetOffsets(set)
	// Bump the generation only after the new position is live, so records read
	// before this point keep the prior generation and are cleared when the
	// first new-generation record (or the seek-done signal) arrives.
	c.gen.Add(1)
	return nil
}

// SwitchTopic repoints the consumer at a new topic, tailing it from the end.
// It reuses the seek generation so in-flight records from the old topic are
// dropped and the displayed buffer is cleared.
//
// Ordering matters and is the opposite of Seek: the generation is bumped BEFORE
// the new topic is added so every record read from the new topic is tagged with
// the new generation (and kept). Bumping after AddConsumeTopics would let the
// new topic's first records be tagged with the old generation and then wiped by
// the buffer clear. The old topic is purged first, so no old-topic records are
// read after the bump.
//
// Note: AddConsumeTopics in kgo v1.21.2 does not accept an offset argument;
// the new topic starts at the end because the client was built with
// ConsumeResetOffset(kgo.NewOffset().AtEnd()) in NewClient.
func (c *Consumer) SwitchTopic(ctx context.Context, topic string) error {
	if old := c.currentTopic(); old != "" {
		c.cl.PurgeTopicsFromConsuming(old)
	}
	c.gen.Add(1)
	c.setTopic(topic)
	c.cl.AddConsumeTopics(topic)
	return nil
}

// offsetMap flattens kadm offsets for one topic into partition->offset.
func offsetMap(lo kadm.ListedOffsets, topic string) (map[int32]int64, error) {
	out := make(map[int32]int64)
	for p, l := range lo[topic] {
		if l.Err != nil {
			return nil, l.Err
		}
		out[p] = l.Offset
	}
	return out, nil
}

// toSetOffsets converts kadm listed offsets into the map SetOffsets expects.
// Negative offsets (no offset for a timestamp past the log) are skipped so the
// client keeps its default position for those partitions.
func toSetOffsets(lo kadm.ListedOffsets) (map[string]map[int32]kgo.EpochOffset, error) {
	set := make(map[string]map[int32]kgo.EpochOffset)
	for topic, parts := range lo {
		m := make(map[int32]kgo.EpochOffset)
		for p, l := range parts {
			if l.Err != nil {
				return nil, l.Err
			}
			if l.Offset < 0 {
				continue
			}
			m[p] = kgo.EpochOffset{Epoch: -1, Offset: l.Offset}
		}
		set[topic] = m
	}
	return set, nil
}
