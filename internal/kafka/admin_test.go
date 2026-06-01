package kafka

import "testing"

func TestBuildTopicInfos(t *testing.T) {
	partitions := map[string]int{"orders": 2, "_internal": 1, "users": 1}
	starts := map[string]map[int32]int64{
		"orders":    {0: 0, 1: 5},
		"_internal": {0: 0},
		"users":     {0: 10},
	}
	ends := map[string]map[int32]int64{
		"orders":    {0: 10, 1: 7}, // 10 + 2 = 12
		"_internal": {0: 100},
		"users":     {0: 10}, // 0 messages
	}

	got := buildTopicInfos(partitions, starts, ends, false)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (internal excluded)", len(got))
	}
	// sorted by name: orders, users
	if got[0].Name != "orders" || got[1].Name != "users" {
		t.Fatalf("order = %q,%q want orders,users", got[0].Name, got[1].Name)
	}
	if got[0].Partitions != 2 || got[0].Messages != 12 {
		t.Fatalf("orders = %+v, want parts 2 msgs 12", got[0])
	}
	if got[1].Messages != 0 {
		t.Fatalf("users messages = %d, want 0", got[1].Messages)
	}

	withInternal := buildTopicInfos(partitions, starts, ends, true)
	if len(withInternal) != 3 {
		t.Fatalf("len = %d, want 3 (internal included)", len(withInternal))
	}
	if withInternal[0].Name != "_internal" {
		t.Fatalf("first = %q, want _internal (sorts first)", withInternal[0].Name)
	}
}

func TestBuildTopicInfosMissingStartCountsFromZero(t *testing.T) {
	got := buildTopicInfos(
		map[string]int{"t": 1},
		map[string]map[int32]int64{}, // no start offsets
		map[string]map[int32]int64{"t": {0: 5}},
		false,
	)
	if len(got) != 1 || got[0].Messages != 5 {
		t.Fatalf("got %+v, want msgs 5", got)
	}
}
