package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestPickerSelectsWithEnter(t *testing.T) {
	// newPicker sorts names -> [local, prod, staging]
	p := newPicker([]string{"local", "staging", "prod"})
	// move down once, then enter -> "prod" (index 1 in sorted order)
	np, _ := p.Update(tea.KeyMsg{Type: tea.KeyDown})
	p = np.(picker)
	np, cmd := p.Update(tea.KeyMsg{Type: tea.KeyEnter})
	p = np.(picker)
	if !p.done || p.Choice() != "prod" {
		t.Fatalf("choice = %q done=%v, want prod/true", p.Choice(), p.done)
	}
	if cmd == nil {
		t.Fatal("expected tea.Quit cmd on selection")
	}
}

func TestPickerCancel(t *testing.T) {
	p := newPicker([]string{"local"})
	np, _ := p.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	p = np.(picker)
	if p.Choice() != "" {
		t.Fatalf("choice = %q, want empty on cancel", p.Choice())
	}
}
