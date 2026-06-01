package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
)

func groupModel(t *testing.T) Model {
	t.Helper()
	m := sized(New(nil))
	m.listGroupsFn = func() ([]kafka.GroupInfo, error) {
		return []kafka.GroupInfo{
			{Name: "g1", State: "Stable", Members: 2},
			{Name: "g2", State: "Empty", Members: 0},
		}, nil
	}
	m.describeGroupFn = func(name string) (kafka.GroupDetail, error) {
		return kafka.GroupDetail{Name: name, State: "Stable", TotalLag: 7}, nil
	}
	nm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'g'}})
	m = nm.(Model)
	if m.mode != modeGroups {
		t.Fatal("expected modeGroups after 'g'")
	}
	if cmd == nil {
		t.Fatal("expected a groups-load command")
	}
	nm, _ = m.Update(cmd())
	return nm.(Model)
}

func TestGroupsLoad(t *testing.T) {
	m := groupModel(t)
	if len(m.groups) != 2 || m.groups[0].Name != "g1" {
		t.Fatalf("groups = %+v", m.groups)
	}
}

func TestGroupCursorAutoLoadsDetail(t *testing.T) {
	m := groupModel(t)
	// groupsLoadedMsg already fired refreshSelectedGroupDetail; consume the
	// describe command it queued.
	nm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = nm.(Model)
	if m.groupCursor != 1 {
		t.Fatalf("cursor = %d, want 1", m.groupCursor)
	}
	if cmd == nil {
		t.Fatal("expected describe cmd on cursor move")
	}
	msg := cmd()
	if _, ok := msg.(groupDetailMsg); !ok {
		t.Fatalf("expected groupDetailMsg, got %T", msg)
	}
}

func TestGroupEscBackToNormal(t *testing.T) {
	m := groupModel(t)
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = nm.(Model)
	if m.mode != modeNormal {
		t.Fatal("esc from groups view should return to record view")
	}
}

func TestGroupsLoadErrorKeepsList(t *testing.T) {
	m := groupModel(t)
	// a failing reload must not blank the existing list. Tag with the current
	// load generation so the message isn't dropped as stale.
	nm, _ := m.Update(groupsLoadedMsg{err: errFake, gen: m.groupLoadGen})
	m = nm.(Model)
	if len(m.groups) != 2 {
		t.Fatalf("groups blanked on error: %d", len(m.groups))
	}
	if m.groupsErr == "" {
		t.Fatal("expected groupsErr set")
	}
}

var errFake = fakeErr("boom")

type fakeErr string

func (e fakeErr) Error() string { return string(e) }
