package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// GroupInfo summarizes a consumer group for the list view. TotalLag is -1 when
// the bulk lag fetch failed for this group (rendered as "?" in the UI).
type GroupInfo struct {
	Name     string
	State    string
	Members  int
	TotalLag int64
}

// GroupLagRow is one topic-partition's offsets and lag for a group.
type GroupLagRow struct {
	Topic     string
	Partition int32
	Committed int64
	End       int64
	Lag       int64
}

// GroupMember is one member of a consumer group.
type GroupMember struct {
	MemberID    string
	ClientID    string
	Host        string
	Assignments []string // "topic:partition"
}

// GroupDetail is the full read-only view of one group.
type GroupDetail struct {
	Name     string
	State    string
	TotalLag int64
	Lag      []GroupLagRow
	Members  []GroupMember
}

// buildGroupDetail assembles a GroupDetail: lag rows sorted by topic then
// partition, total lag summed (negative per-partition lag floored at 0),
// members sorted by id with their assignments sorted.
func buildGroupDetail(name, state string, rows []GroupLagRow, members []GroupMember) GroupDetail {
	sorted := append([]GroupLagRow(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Topic != sorted[j].Topic {
			return sorted[i].Topic < sorted[j].Topic
		}
		return sorted[i].Partition < sorted[j].Partition
	})
	var total int64
	for _, r := range sorted {
		if r.Lag > 0 {
			total += r.Lag
		}
	}
	ms := make([]GroupMember, len(members))
	copy(ms, members)
	for i := range ms {
		a := append([]string(nil), ms[i].Assignments...)
		sort.Strings(a)
		ms[i].Assignments = a
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].MemberID < ms[j].MemberID })
	return GroupDetail{Name: name, State: state, TotalLag: total, Lag: sorted, Members: ms}
}

// ListGroupsBasic lists groups with state and member count, TotalLag=-1
// ("unknown" — fill in via FetchGroupLags). Two RPCs: ListGroups + batch
// DescribeGroups. Lag fetch lives in FetchGroupLags so the UI can paint the
// list immediately and fill lag in a second phase.
func ListGroupsBasic(ctx context.Context, cl *kgo.Client) ([]GroupInfo, error) {
	adm := kadm.NewClient(cl)
	listed, err := adm.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for name := range listed {
		names = append(names, name)
	}
	if len(names) == 0 {
		return nil, nil
	}
	described, err := adm.DescribeGroups(ctx, names...)
	if err != nil {
		return nil, err
	}
	out := make([]GroupInfo, 0, len(described))
	for name, d := range described {
		if d.Err != nil {
			continue
		}
		out = append(out, GroupInfo{Name: name, State: d.State, Members: len(d.Members), TotalLag: -1})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// FetchGroupLags returns total lag per group name in a single batch Lag call.
// Groups whose lag fetch errored are absent from the map.
func FetchGroupLags(ctx context.Context, cl *kgo.Client, names []string) (map[string]int64, error) {
	if len(names) == 0 {
		return map[string]int64{}, nil
	}
	adm := kadm.NewClient(cl)
	lags, err := adm.Lag(ctx, names...)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(lags))
	for name, dgl := range lags {
		if dgl.Error() != nil {
			continue
		}
		var total int64
		for _, parts := range dgl.Lag {
			for _, gml := range parts {
				if gml.Err == nil && gml.Lag > 0 {
					total += gml.Lag
				}
			}
		}
		out[name] = total
	}
	return out, nil
}

// ListGroups is the legacy single-call API: basic list + lag in one shot.
// Retained for tests and callers that don't need progressive load.
func ListGroups(ctx context.Context, cl *kgo.Client) ([]GroupInfo, error) {
	basics, err := ListGroupsBasic(ctx, cl)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(basics))
	for i, g := range basics {
		names[i] = g.Name
	}
	lags, _ := FetchGroupLags(ctx, cl, names)
	for i, g := range basics {
		if v, ok := lags[g.Name]; ok {
			basics[i].TotalLag = v
		}
	}
	return basics, nil
}

// DescribeGroup returns lag and members for one group.
func DescribeGroup(ctx context.Context, cl *kgo.Client, name string) (GroupDetail, error) {
	adm := kadm.NewClient(cl)
	lags, err := adm.Lag(ctx, name)
	if err != nil {
		return GroupDetail{}, err
	}
	dgl, ok := lags[name]
	if !ok {
		return GroupDetail{}, fmt.Errorf("group %q not found", name)
	}
	// dgl.Error() combines DescribeErr and FetchErr; checking only FetchErr would
	// silently present a describe-failed group as healthy-and-empty.
	if err := dgl.Error(); err != nil {
		return GroupDetail{}, err
	}

	var rows []GroupLagRow
	for topic, parts := range dgl.Lag {
		for p, gml := range parts {
			if gml.Err != nil {
				continue // partition with a commit/offset error; skip the -1 row
			}
			rows = append(rows, GroupLagRow{
				Topic:     topic,
				Partition: p,
				Committed: gml.Commit.At,
				End:       gml.End.Offset,
				Lag:       gml.Lag,
			})
		}
	}
	var members []GroupMember
	for _, m := range dgl.Members {
		members = append(members, GroupMember{
			MemberID:    m.MemberID,
			ClientID:    m.ClientID,
			Host:        m.ClientHost,
			Assignments: memberAssignments(m),
		})
	}
	return buildGroupDetail(name, dgl.State, rows, members), nil
}

// memberAssignments flattens a member's assigned topic-partitions to
// "topic:partition" strings.
//
// NOTE vs plan: m.Assigned is kadm.GroupMemberAssignment (not directly
// iterable). We call .AsConsumer() to get *kmsg.ConsumerMemberAssignment
// whose .Topics field has .Topic string and .Partitions []int32. If the
// group is not of "consumer" protocol type, we return an empty slice.
func memberAssignments(m kadm.DescribedGroupMember) []string {
	c, ok := m.Assigned.AsConsumer()
	if !ok {
		return nil
	}
	var out []string
	for _, t := range c.Topics {
		for _, p := range t.Partitions {
			out = append(out, fmt.Sprintf("%s:%d", t.Topic, p))
		}
	}
	return out
}
