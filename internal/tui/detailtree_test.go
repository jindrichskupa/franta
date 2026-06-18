package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/record"
)

// treeModel returns a detail-focused model showing one JSON record's tree.
func treeModel(t *testing.T, value string) Model {
	t.Helper()
	m := New(nil)
	nm, _ := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	m = nm.(Model)
	nm, _ = m.Update(RecordMsg{Record: record.Record{
		Topic: "t", Partition: 0, Offset: 1, Value: []byte(value),
	}, Gen: 0})
	m = nm.(Model)
	// Focus the detail pane.
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'3'}})
	m = nm.(Model)
	return m
}

func key(m Model, t *testing.T, k tea.KeyMsg) Model {
	t.Helper()
	nm, _ := m.Update(k)
	return nm.(Model)
}

func TestTreeIsDefaultForJSON(t *testing.T) {
	m := treeModel(t, `{"a":1,"b":2}`)
	if m.detailRaw {
		t.Fatal("detailRaw should default to false (tree)")
	}
	if m.detailTree == nil {
		t.Fatal("expected a parsed tree for a JSON value")
	}
	// root, a, b => 3 rows.
	if len(m.detailRows) != 3 {
		t.Fatalf("rows %d want 3", len(m.detailRows))
	}
}

func TestVToggleRawTree(t *testing.T) {
	m := treeModel(t, `{"a":1}`)
	m = key(m, t, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	if !m.detailRaw || m.detailTree != nil {
		t.Fatalf("after v: raw=%v tree!=nil=%v, want raw=true tree=nil", m.detailRaw, m.detailTree != nil)
	}
	m = key(m, t, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	if m.detailRaw || m.detailTree == nil {
		t.Fatalf("after v v: raw=%v tree==nil=%v, want raw=false tree set", m.detailRaw, m.detailTree == nil)
	}
}

func TestTreeCursorMoves(t *testing.T) {
	m := treeModel(t, `{"a":1,"b":2}`)
	if m.detailTreeCursor != 0 {
		t.Fatalf("initial cursor %d want 0", m.detailTreeCursor)
	}
	m = key(m, t, tea.KeyMsg{Type: tea.KeyDown})
	if m.detailTreeCursor != 1 {
		t.Fatalf("after down cursor %d want 1", m.detailTreeCursor)
	}
	// End jumps to last row.
	m = key(m, t, tea.KeyMsg{Type: tea.KeyEnd})
	if m.detailTreeCursor != len(m.detailRows)-1 {
		t.Fatalf("after end cursor %d want %d", m.detailTreeCursor, len(m.detailRows)-1)
	}
	m = key(m, t, tea.KeyMsg{Type: tea.KeyHome})
	if m.detailTreeCursor != 0 {
		t.Fatalf("after home cursor %d want 0", m.detailTreeCursor)
	}
}

func TestTreeFoldUnfoldChangesRowCount(t *testing.T) {
	m := treeModel(t, `{"a":{"x":1,"y":2}}`)
	full := len(m.detailRows) // root, a, x, y => 4
	if full != 4 {
		t.Fatalf("full rows %d want 4", full)
	}
	// Cursor on root (row 0): fold it → only root remains.
	m = key(m, t, tea.KeyMsg{Type: tea.KeyEnter})
	if len(m.detailRows) != 1 {
		t.Fatalf("after fold root rows %d want 1", len(m.detailRows))
	}
	// Unfold.
	m = key(m, t, tea.KeyMsg{Type: tea.KeyEnter})
	if len(m.detailRows) != full {
		t.Fatalf("after unfold rows %d want %d", len(m.detailRows), full)
	}
}

func TestTreeLeftRightCollapseExpand(t *testing.T) {
	m := treeModel(t, `{"a":{"x":1}}`)
	// Move to "a" (row 1), collapse with left.
	m = key(m, t, tea.KeyMsg{Type: tea.KeyDown})
	m = key(m, t, tea.KeyMsg{Type: tea.KeyLeft})
	if len(m.detailRows) != 2 { // root, a(collapsed)
		t.Fatalf("after collapse rows %d want 2", len(m.detailRows))
	}
	m = key(m, t, tea.KeyMsg{Type: tea.KeyRight})
	if len(m.detailRows) != 3 { // root, a, x
		t.Fatalf("after expand rows %d want 3", len(m.detailRows))
	}
}

func TestNonJSONStaysRaw(t *testing.T) {
	m := treeModel(t, `plain text`)
	if m.detailTree != nil {
		t.Fatal("non-JSON should not produce a tree")
	}
	// Pressing v to request tree reports it isn't JSON and stays raw.
	m = key(m, t, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	if !m.detailRaw {
		t.Fatalf("after v on non-JSON detailRaw=%v want true", m.detailRaw)
	}
	if m.status != "value is not JSON" {
		t.Fatalf("status %q want 'value is not JSON'", m.status)
	}
}

func TestTreeShortPaneScrollsMetaOff(t *testing.T) {
	// A value with many keys + a short pane: navigating down must scroll the
	// metadata block off so the whole panel (not just a few value rows) scrolls.
	m := treeModel(t, `{"k0":0,"k1":1,"k2":2,"k3":3,"k4":4,"k5":5,"k6":6,"k7":7,"k8":8,"k9":9}`)
	const innerH = 6

	// At the top the last key isn't reachable yet (panel hasn't scrolled).
	top := m.detailTreeView(innerH)
	if strings.Contains(top, "k9") {
		t.Fatalf("at top, last key k9 should not be visible yet:\n%s", top)
	}

	// Jump to the last row → the whole panel scrolls: the meta block goes off
	// the top and the last key comes into view.
	m = key(m, t, tea.KeyMsg{Type: tea.KeyEnd})
	bottom := m.detailTreeView(innerH)
	if strings.Contains(bottom, "topic:") {
		t.Fatalf("at bottom, meta block should have scrolled off:\n%s", bottom)
	}
	if !strings.Contains(bottom, "k9") {
		t.Fatalf("at bottom, last key k9 should be visible:\n%s", bottom)
	}
}

func TestRecordChangeResetsTreeCursor(t *testing.T) {
	m := treeModel(t, `{"a":{"x":1,"y":2}}`)
	m = key(m, t, tea.KeyMsg{Type: tea.KeyDown}) // cursor → 1
	if m.detailTreeCursor == 0 {
		t.Fatal("precondition: cursor should have moved")
	}
	// A new record arrives and the table cursor lands on it (newest first).
	nm, _ := m.Update(RecordMsg{Record: record.Record{
		Topic: "t", Partition: 0, Offset: 2, Value: []byte(`{"b":9}`),
	}, Gen: 0})
	m = nm.(Model)
	if m.detailTreeCursor != 0 {
		t.Fatalf("cursor %d want 0 after record change", m.detailTreeCursor)
	}
	if len(m.detailRows) != 2 { // root, b
		t.Fatalf("rows %d want 2 for new record", len(m.detailRows))
	}
}
