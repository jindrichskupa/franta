package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestConfirmYesNo(t *testing.T) {
	ran := false
	m := New(nil)
	m.width, m.height = 80, 24
	m = m.askConfirm("Delete X?", func() tea.Cmd { ran = true; return nil })
	if !m.confirmActive {
		t.Fatal("confirm should be active")
	}
	// 'n' cancels.
	nm, _ := m.updateConfirm(keyRunes("n"))
	if nm.(Model).confirmActive || ran {
		t.Fatal("n should cancel without running action")
	}
	// 'y' runs.
	m2, _ := m.updateConfirm(keyRunes("y"))
	if m2.(Model).confirmActive {
		t.Fatal("y should close the modal")
	}
	if !ran {
		t.Fatal("y should run the action")
	}
}

func TestConfirmTyped(t *testing.T) {
	ran := false
	m := New(nil)
	m.width, m.height = 80, 24
	m = m.askConfirmTyped("Type name", "orders", func() tea.Cmd { ran = true; return nil })

	// Wrong text + enter → does not run, stays open.
	m.confirmInput.SetValue("wrong")
	nm, _ := m.updateConfirm(tea.KeyMsg{Type: tea.KeyEnter})
	if !nm.(Model).confirmActive || ran {
		t.Fatal("mismatch should not confirm")
	}
	// Correct text + enter → runs.
	m.confirmInput.SetValue("orders")
	m2, _ := m.updateConfirm(tea.KeyMsg{Type: tea.KeyEnter})
	if m2.(Model).confirmActive || !ran {
		t.Fatal("matching name should confirm + run")
	}
}
