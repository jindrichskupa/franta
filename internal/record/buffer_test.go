package record

import "testing"

func mk(off int64) Record { return Record{Offset: off} }

func offsets(rs []Record) []int64 {
	out := make([]int64, len(rs))
	for i, r := range rs {
		out[i] = r.Offset
	}
	return out
}

func TestBufferKeepsOrderUnderCap(t *testing.T) {
	b := NewBuffer(3)
	b.Add(mk(1))
	b.Add(mk(2))
	if got := offsets(b.Records()); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("got %v, want [1 2]", got)
	}
}

func TestBufferEvictsOldestAtCap(t *testing.T) {
	b := NewBuffer(2)
	b.Add(mk(1))
	b.Add(mk(2))
	b.Add(mk(3))
	got := offsets(b.Records())
	if len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("got %v, want [2 3]", got)
	}
}

func TestBufferRecordsIsACopy(t *testing.T) {
	b := NewBuffer(2)
	b.Add(mk(1))
	snap := b.Records()
	b.Add(mk(2))
	if len(snap) != 1 {
		t.Fatalf("snapshot mutated: len=%d, want 1", len(snap))
	}
}
