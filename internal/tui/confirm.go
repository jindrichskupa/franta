package tui

import (
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// askConfirm opens a yes/no confirm modal that runs action on confirm.
func (m Model) askConfirm(prompt string, action func() tea.Cmd) Model {
	m.confirmActive = true
	m.confirmPrompt = prompt
	m.confirmExpect = ""
	m.confirmAction = action
	m.status = ""
	return m
}

// askConfirmTyped opens a type-to-confirm modal; action runs only when the user
// types expect exactly.
func (m Model) askConfirmTyped(prompt, expect string, action func() tea.Cmd) Model {
	in := textinput.New()
	in.Prompt = "› "
	in.Focus()
	m.confirmActive = true
	m.confirmPrompt = prompt
	m.confirmExpect = expect
	m.confirmInput = in
	m.confirmAction = action
	m.status = ""
	return m
}

func (m *Model) closeConfirm() {
	m.confirmActive = false
	m.confirmPrompt = ""
	m.confirmExpect = ""
	m.confirmAction = nil
}

// updateConfirm handles keys while the confirm modal is open.
func (m Model) updateConfirm(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.closeConfirm()
		return m, nil
	}
	if m.confirmExpect == "" && msg.Type == tea.KeyEnter {
		action := m.confirmAction
		m.closeConfirm()
		if action != nil {
			return m, action()
		}
		return m, nil
	}
	if m.confirmExpect == "" {
		// y/n mode.
		switch msg.String() {
		case "y", "Y":
			action := m.confirmAction
			m.closeConfirm()
			if action != nil {
				return m, action()
			}
			return m, nil
		case "n", "N":
			m.closeConfirm()
			return m, nil
		}
		return m, nil
	}
	// type-to-confirm mode.
	if msg.Type == tea.KeyEnter {
		if m.confirmInput.Value() == m.confirmExpect {
			action := m.confirmAction
			m.closeConfirm()
			if action != nil {
				return m, action()
			}
			return m, nil
		}
		m.status = "name does not match; type it exactly or esc"
		return m, nil
	}
	var cmd tea.Cmd
	m.confirmInput, cmd = m.confirmInput.Update(msg)
	return m, cmd
}

// confirmView renders the centred confirm modal.
func (m Model) confirmView() string {
	w := m.width * 60 / 100
	if w < 50 {
		w = 50
	}
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("214"))
	var body string
	if m.confirmExpect == "" {
		body = titleStyle.Render(m.confirmPrompt) + "\n\n" +
			statusStyle.Render("y confirm  •  n / esc cancel")
	} else {
		body = titleStyle.Render(m.confirmPrompt) + "\n\n" +
			m.confirmInput.View() + "\n\n" +
			statusStyle.Render("enter confirm (must match)  •  esc cancel")
	}
	dialog := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("214")).
		Padding(1, 2).
		Width(w - 2).
		Render(body)
	if m.width > 0 && m.height > 0 {
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, dialog)
	}
	return dialog
}
