package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/record"
)

func TestProducerSubmitCallsProduce(t *testing.T) {
	var got record.Record
	produce := func(r record.Record) error { got = r; return nil }
	m := sized(New(produce))

	// open producer
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	m = nm.(Model)
	if m.mode != modeProducer {
		t.Fatal("expected producer mode")
	}

	// Fill topic, key, headers, value via tab-cycling through 4 fields.
	m = typeRunes(m, "orders")
	m = tab(m) // -> key
	m = typeRunes(m, "k1")
	m = tab(m) // -> headers
	m = typeRunes(m, "x=1, y=2")
	m = tab(m) // -> value (textarea)
	m = typeRunes(m, "hello")

	// Submit with ctrl+s (enter on the textarea would insert a newline).
	nm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlS})
	m = nm.(Model)
	if cmd == nil {
		t.Fatal("expected a command from submit")
	}
	msg := cmd()
	if _, ok := msg.(producedMsg); !ok {
		t.Fatalf("expected producedMsg, got %T", msg)
	}
	if got.Topic != "orders" || string(got.Key) != "k1" || string(got.Value) != "hello" {
		t.Fatalf("produced record = %+v", got)
	}
	if len(got.Headers) != 2 || got.Headers[0].Key != "x" || string(got.Headers[0].Value) != "1" {
		t.Fatalf("headers = %+v", got.Headers)
	}
}

func TestProducerTemplatePrefillsFromRecord(t *testing.T) {
	produce := func(record.Record) error { return nil }
	m := sized(New(produce))
	// Seed one record so 'P' has something to template from.
	nm, _ := m.Update(RecordMsg{Record: record.Record{
		Topic: "orders", Key: []byte("k9"),
		Value:   []byte(`{"x":1}`),
		Headers: []record.Header{{Key: "src", Value: []byte("web")}},
	}, Gen: 0})
	m = nm.(Model)
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'P'}})
	m = nm.(Model)
	if m.mode != modeProducer {
		t.Fatal("'P' did not open producer")
	}
	if m.prodInputs[prodFieldTopic].Value() != "orders" {
		t.Fatalf("topic prefill = %q", m.prodInputs[prodFieldTopic].Value())
	}
	if m.prodInputs[prodFieldKey].Value() != "k9" {
		t.Fatalf("key prefill = %q", m.prodInputs[prodFieldKey].Value())
	}
	if m.prodHeadersTA.Value() != "src=web" {
		t.Fatalf("headers prefill = %q", m.prodHeadersTA.Value())
	}
}

func TestParseHeaders(t *testing.T) {
	got := parseHeaders("a=1, b=2 ,  c=hello world")
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Key != "a" || string(got[0].Value) != "1" {
		t.Fatalf("got[0] = %+v", got[0])
	}
	if got[2].Key != "c" || string(got[2].Value) != "hello world" {
		t.Fatalf("got[2] = %+v", got[2])
	}
	if parseHeaders("") != nil {
		t.Fatal("empty should be nil")
	}
}

func tab(m Model) Model {
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyTab})
	return nm.(Model)
}

func TestProducerEscReturnsToList(t *testing.T) {
	m := sized(New(func(record.Record) error { return nil }))
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	m = nm.(Model)
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = nm.(Model)
	if m.mode != modeNormal {
		t.Fatal("esc did not return to list")
	}
}
