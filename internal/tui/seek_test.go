package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
	"franta/internal/record"
)

func TestSeekPromptCallsSeek(t *testing.T) {
	var got kafka.StartSpec
	m := sized(New(nil))
	m.seek = func(s kafka.StartSpec) (int64, error) { got = s; return 1, nil }

	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	m = nm.(Model)
	if !m.seeking {
		t.Fatal("expected seeking mode after 's'")
	}
	m = typeRunes(m, "last:3")
	nm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = nm.(Model)
	if cmd == nil {
		t.Fatal("expected a seek command")
	}
	msg := cmd()
	if _, ok := msg.(seekDoneMsg); !ok {
		t.Fatalf("expected seekDoneMsg, got %T", msg)
	}
	if got.Kind != kafka.StartLastN || got.N != 3 {
		t.Fatalf("seek spec = %+v, want last:3", got)
	}
}

func TestSeekDoneClearsBuffer(t *testing.T) {
	m := sized(New(nil))
	for _, off := range []int64{1, 2, 3} {
		nm, _ := m.Update(RecordMsg{Record: record.Record{Offset: off}})
		m = nm.(Model)
	}
	// A new generation (1 > current 0) clears the prior position's view.
	nm, _ := m.Update(seekDoneMsg{gen: 1})
	m = nm.(Model)
	if got := len(m.visible()); got != 0 {
		t.Fatalf("visible after seek = %d, want 0 (buffer cleared)", got)
	}
}

func TestStaleGenerationRecordsDropped(t *testing.T) {
	m := sized(New(nil))
	// Advance to generation 1 via a seek-done signal.
	nm, _ := m.Update(seekDoneMsg{gen: 1})
	m = nm.(Model)
	// A record tagged with the old generation 0 must be ignored.
	nm, _ = m.Update(RecordMsg{Record: record.Record{Offset: 9}, Gen: 0})
	m = nm.(Model)
	if got := len(m.visible()); got != 0 {
		t.Fatalf("visible = %d, want 0 (stale-generation record dropped)", got)
	}
	// A record at the current generation is kept.
	nm, _ = m.Update(RecordMsg{Record: record.Record{Offset: 10}, Gen: 1})
	m = nm.(Model)
	if got := len(m.visible()); got != 1 {
		t.Fatalf("visible = %d, want 1", got)
	}
}

func TestSeekDoneAfterRecordDoesNotWipe(t *testing.T) {
	m := sized(New(nil))
	// A freshly-sought record (gen 1) arrives before the seekDoneMsg.
	nm, _ := m.Update(RecordMsg{Record: record.Record{Offset: 0}, Gen: 1})
	m = nm.(Model)
	// The later seekDoneMsg for the same generation must NOT clear it.
	nm, _ = m.Update(seekDoneMsg{gen: 1})
	m = nm.(Model)
	if got := len(m.visible()); got != 1 {
		t.Fatalf("visible = %d, want 1 (record must survive the seekDoneMsg)", got)
	}
}

func TestSeekParseErrorKeepsPrompt(t *testing.T) {
	m := sized(New(nil))
	m.seek = func(kafka.StartSpec) (int64, error) { return 1, nil }
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'s'}})
	m = nm.(Model)
	m = typeRunes(m, "garbage")
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = nm.(Model)
	if m.status == "" {
		t.Fatal("expected a seek error status")
	}
	if !m.seeking {
		t.Fatal("expected to stay in seeking mode after a parse error")
	}
}
