package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/record"
)

func sized(m Model) Model {
	nm, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 30})
	return nm.(Model)
}

func TestRecordMsgAppendsAndIsVisible(t *testing.T) {
	m := sized(New(nil))
	nm, _ := m.Update(RecordMsg{Record: record.Record{Partition: 1, Offset: 5, Value: []byte("hi")}})
	m = nm.(Model)
	if got := len(m.visible()); got != 1 {
		t.Fatalf("visible = %d, want 1", got)
	}
}

func TestVisibleAppliesPredicate(t *testing.T) {
	m := sized(New(nil))
	for _, off := range []int64{1, 2, 3} {
		nm, _ := m.Update(RecordMsg{Record: record.Record{Offset: off}})
		m = nm.(Model)
	}
	// Apply a predicate directly (query bar wiring is a later task).
	m.pred = func(r record.Record) bool { return r.Offset >= 2 }
	if got := len(m.visible()); got != 2 {
		t.Fatalf("visible = %d, want 2", got)
	}
}

func TestQuitKey(t *testing.T) {
	m := sized(New(nil))
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	if cmd == nil {
		t.Fatal("ctrl+c returned nil cmd, want tea.Quit")
	}
}

func typeRunes(m Model, s string) Model {
	for _, r := range s {
		nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = nm.(Model)
	}
	return m
}

func TestQueryBarFiltersOnEnter(t *testing.T) {
	m := sized(New(nil))
	for _, off := range []int64{1, 2, 3} {
		nm, _ := m.Update(RecordMsg{Record: record.Record{Offset: off}})
		m = nm.(Model)
	}
	// Focus filter, type a query, submit.
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = nm.(Model)
	if !m.filtering {
		t.Fatal("expected filtering mode after '/'")
	}
	m = typeRunes(m, "offset >= 2")
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = nm.(Model)
	if m.filtering {
		t.Fatal("still filtering after enter")
	}
	if got := len(m.visible()); got != 2 {
		t.Fatalf("visible = %d, want 2", got)
	}
}

func TestQueryBarShowsParseError(t *testing.T) {
	m := sized(New(nil))
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = nm.(Model)
	m = typeRunes(m, "partition ==")
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = nm.(Model)
	if m.status == "" {
		t.Fatal("expected a filter error status")
	}
}

func TestDetailAutoSyncOnCursorMove(t *testing.T) {
	m := sized(New(nil))
	for _, off := range []int64{1, 2, 3} {
		nm, _ := m.Update(RecordMsg{Record: recordWithOffset(off), Gen: 0})
		m = nm.(Model)
	}
	// Focus pane 2 (default), press Down twice; detail content must change.
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = nm.(Model)
	view1 := m.detail.View()
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = nm.(Model)
	view2 := m.detail.View()
	if view1 == view2 {
		t.Fatal("detail viewport should change as the cursor moves")
	}
}

func TestPaneFocusKeys(t *testing.T) {
	m := sized(New(nil))
	if m.paneFocus != paneMessages {
		t.Fatalf("default paneFocus = %v, want paneMessages", m.paneFocus)
	}
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'1'}})
	m = nm.(Model)
	if m.paneFocus != paneTopics {
		t.Fatal("'1' did not focus topics")
	}
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'3'}})
	m = nm.(Model)
	if m.paneFocus != paneDetail {
		t.Fatal("'3' did not focus detail")
	}
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	m = nm.(Model)
	if m.paneFocus != paneTopics {
		t.Fatalf("tab from detail = %v, want wrap to topics", m.paneFocus)
	}
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyShiftTab})
	m = nm.(Model)
	if m.paneFocus != paneDetail {
		t.Fatalf("shift+tab from topics = %v, want wrap to detail", m.paneFocus)
	}
}

func TestViewContainsThreeBordersAtNormalSize(t *testing.T) {
	m := sized(New(nil))
	v := m.View()
	// lipgloss rounded border uses '╭' for top-left. Three panes -> at least 3 occurrences.
	if got := strings.Count(v, "╭"); got < 3 {
		t.Fatalf("View has %d '╭'; want >= 3 (one per pane)", got)
	}
}

func TestViewNarrowFallbackShowsOnlyFocused(t *testing.T) {
	m := New(nil)
	nm, _ := m.Update(tea.WindowSizeMsg{Width: 40, Height: 24})
	m = nm.(Model)
	v := m.View()
	if !strings.Contains(v, "[narrow]") {
		t.Fatalf("expected [narrow] tag in footer; got %q", v)
	}
}
