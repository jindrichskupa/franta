package tui

import (
	"errors"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/record"
)

func keyRunes(s string) tea.KeyMsg { return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(s)} }

func newModelWithRecords(recs ...record.Record) Model {
	m := New(nil)
	m.width, m.height = 120, 40
	for _, r := range recs {
		m.buf.Add(r)
	}
	m.refreshTable()
	return m
}

func TestSelectedRecord(t *testing.T) {
	r := record.Record{Partition: 1, Offset: 9, Timestamp: time.Unix(0, 0), Key: []byte("k"), Value: []byte("v")}
	m := newModelWithRecords(r)
	got, ok := m.selectedRecord()
	if !ok {
		t.Fatal("expected a selected record")
	}
	if got.Offset != 9 {
		t.Fatalf("offset: %d", got.Offset)
	}
	empty := New(nil)
	if _, ok := empty.selectedRecord(); ok {
		t.Fatal("empty buffer should have no selection")
	}
}

func TestYankKey(t *testing.T) {
	var got string
	m := newModelWithRecords(record.Record{Partition: 1, Offset: 9, Key: []byte("ORD-1"), Value: []byte("v")})
	m.copyFn = func(s string) error { got = s; return nil }
	m.paneFocus = paneMessages
	nm, _ := m.handleKey(keyRunes("y"))
	m2 := nm.(Model)
	if got != "ORD-1" {
		t.Fatalf("clipboard got %q", got)
	}
	if m2.status != "copied key" {
		t.Fatalf("status %q", m2.status)
	}
}

func TestYankValue(t *testing.T) {
	var got string
	r := record.Record{Offset: 1, Key: []byte("k"), Value: []byte(`{"a":1}`)}
	m := newModelWithRecords(r)
	m.copyFn = func(s string) error { got = s; return nil }
	m.paneFocus = paneMessages
	nm, _ := m.handleKey(keyRunes("Y"))
	m2 := nm.(Model)
	if got != r.ValueDisplay() {
		t.Fatalf("got=%q want=%q", got, r.ValueDisplay())
	}
	if m2.status != "copied value" {
		t.Fatalf("status %q", m2.status)
	}
}

func TestYankRecord(t *testing.T) {
	var got string
	m := newModelWithRecords(record.Record{Partition: 2, Offset: 5, Key: []byte("k"), Value: []byte("v")})
	m.copyFn = func(s string) error { got = s; return nil }
	m.paneFocus = paneMessages
	nm, _ := m.handleKey(tea.KeyMsg{Type: tea.KeyCtrlY})
	m2 := nm.(Model)
	if !strings.Contains(got, `"offset": 5`) || m2.status != "copied record" {
		t.Fatalf("got=%q status=%q", got, m2.status)
	}
}

func TestYankClipboardError(t *testing.T) {
	m := newModelWithRecords(record.Record{Offset: 1, Key: []byte("k")})
	m.copyFn = func(string) error { return errors.New("no clipboard") }
	m.paneFocus = paneMessages
	nm, _ := m.handleKey(keyRunes("y"))
	if !strings.Contains(nm.(Model).errDialog, "clipboard: no clipboard") {
		t.Fatalf("errDialog %q", nm.(Model).errDialog)
	}
}

func TestYankNothingToCopy(t *testing.T) {
	called := false
	m := New(nil)
	m.width, m.height = 120, 40
	m.copyFn = func(string) error { called = true; return nil }
	m.paneFocus = paneMessages
	nm, _ := m.handleKey(keyRunes("y"))
	m2 := nm.(Model)
	if called {
		t.Fatal("copyFn should not be called with empty buffer")
	}
	if m2.status != "nothing to copy" {
		t.Fatalf("status %q", m2.status)
	}
}
