package kafka

import "testing"

func TestBuildGroupDetailSortsAndTotals(t *testing.T) {
	rows := []GroupLagRow{
		{Topic: "b", Partition: 0, Committed: 5, End: 10, Lag: 5},
		{Topic: "a", Partition: 1, Committed: 0, End: 3, Lag: 3},
		{Topic: "a", Partition: 0, Committed: 2, End: 2, Lag: 0},
		{Topic: "a", Partition: 2, Committed: 0, End: 0, Lag: -1}, // negative -> floored at 0
	}
	members := []GroupMember{
		{MemberID: "m2", Assignments: []string{"b:0", "a:0"}},
		{MemberID: "m1", Assignments: []string{"a:2", "a:1"}},
	}

	d := buildGroupDetail("g", "Stable", rows, members)

	if d.Name != "g" || d.State != "Stable" {
		t.Fatalf("meta = %q/%q", d.Name, d.State)
	}
	// total lag: 5 + 3 + 0 + 0 = 8 (negative floored)
	if d.TotalLag != 8 {
		t.Fatalf("TotalLag = %d, want 8", d.TotalLag)
	}
	// lag rows sorted by topic then partition: a0, a1, a2, b0
	wantOrder := []struct {
		topic string
		part  int32
	}{{"a", 0}, {"a", 1}, {"a", 2}, {"b", 0}}
	if len(d.Lag) != 4 {
		t.Fatalf("lag rows = %d, want 4", len(d.Lag))
	}
	for i, w := range wantOrder {
		if d.Lag[i].Topic != w.topic || d.Lag[i].Partition != w.part {
			t.Fatalf("row %d = %s[%d], want %s[%d]", i, d.Lag[i].Topic, d.Lag[i].Partition, w.topic, w.part)
		}
	}
	// members sorted by MemberID; assignments sorted within
	if d.Members[0].MemberID != "m1" || d.Members[1].MemberID != "m2" {
		t.Fatalf("members order = %q,%q", d.Members[0].MemberID, d.Members[1].MemberID)
	}
	if d.Members[0].Assignments[0] != "a:1" || d.Members[0].Assignments[1] != "a:2" {
		t.Fatalf("m1 assignments = %v, want [a:1 a:2]", d.Members[0].Assignments)
	}
}
