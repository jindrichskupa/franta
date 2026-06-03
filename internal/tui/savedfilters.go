package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"franta/internal/query"
)

// updateFilterPicker handles keys while the saved-filter picker modal is open.
// Enter applies the selected query, d deletes it, esc closes.
func (m Model) updateFilterPicker(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.pickingFilter = false
		return m, nil
	case tea.KeyUp:
		if m.filterPickCursor > 0 {
			m.filterPickCursor--
		}
		return m, nil
	case tea.KeyDown:
		if m.filterPickCursor < len(m.savedFilters)-1 {
			m.filterPickCursor++
		}
		return m, nil
	case tea.KeyHome:
		m.filterPickCursor = 0
		return m, nil
	case tea.KeyEnd:
		if n := len(m.savedFilters); n > 0 {
			m.filterPickCursor = n - 1
		}
		return m, nil
	case tea.KeyEnter:
		if m.filterPickCursor < 0 || m.filterPickCursor >= len(m.savedFilters) {
			return m, nil
		}
		sf := m.savedFilters[m.filterPickCursor]
		pred, err := query.Parse(sf.Query)
		if err != nil {
			m.errDialog = "Saved filter " + sf.Name + " did not parse\n\n" + err.Error()
			m.pickingFilter = false
			return m, nil
		}
		m.pred = pred
		m.queryIn.SetValue(sf.Query)
		m.pickingFilter = false
		m.refreshTable()
		m.refreshDetail()
		m.status = fmt.Sprintf("filter %q applied (%d matches)", sf.Name, len(m.visible()))
		return m, nil
	}
	// 'd' deletes the highlighted entry from the side-file.
	if msg.String() == "d" {
		if m.filterPickCursor < 0 || m.filterPickCursor >= len(m.savedFilters) {
			return m, nil
		}
		victim := m.savedFilters[m.filterPickCursor].Name
		if m.deleteFilterFn != nil {
			if err := m.deleteFilterFn(victim); err != nil {
				m.errDialog = "Delete filter failed\n\n" + err.Error()
				return m, nil
			}
		}
		// Drop from the in-memory list too so the picker reflects the change
		// without a reload.
		m.savedFilters = append(m.savedFilters[:m.filterPickCursor], m.savedFilters[m.filterPickCursor+1:]...)
		if m.filterPickCursor >= len(m.savedFilters) && len(m.savedFilters) > 0 {
			m.filterPickCursor = len(m.savedFilters) - 1
		}
		if len(m.savedFilters) == 0 {
			m.pickingFilter = false
			m.status = "deleted " + victim + " (no saved filters left)"
		} else {
			m.status = "deleted " + victim
		}
		return m, nil
	}
	return m, nil
}

// updateSaveFilterPrompt handles keys while the "save filter as…" input is
// focused. Enter writes through the SaveFilter callback; esc cancels and
// returns to the filter editor.
func (m Model) updateSaveFilterPrompt(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.savingFilter = false
		m.pendingSaveQry = ""
		return m, nil
	case tea.KeyEnter:
		name := strings.TrimSpace(m.saveFilterIn.Value())
		if name == "" {
			m.status = "filter name is empty"
			return m, nil
		}
		if m.saveFilterFn != nil {
			if err := m.saveFilterFn(name, m.pendingSaveQry); err != nil {
				m.errDialog = "Save filter failed\n\n" + err.Error()
				m.savingFilter = false
				return m, nil
			}
		}
		// Update or append in the in-memory list so the picker sees it next time.
		updated := false
		for i, f := range m.savedFilters {
			if f.Name == name {
				m.savedFilters[i].Query = m.pendingSaveQry
				updated = true
				break
			}
		}
		if !updated {
			m.savedFilters = append(m.savedFilters, SavedFilter{Name: name, Query: m.pendingSaveQry})
		}
		m.savingFilter = false
		m.pendingSaveQry = ""
		m.status = "saved filter " + name
		return m, nil
	}
	var cmd tea.Cmd
	m.saveFilterIn, cmd = m.saveFilterIn.Update(msg)
	return m, cmd
}

// filterPickerView renders the saved-filter picker as a centered modal.
func (m Model) filterPickerView() string {
	w := m.width * 60 / 100
	if w < 50 {
		w = 50
	}
	if w > 100 {
		w = 100
	}
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	var b strings.Builder
	b.WriteString(titleStyle.Render("Saved filters") + "\n\n")
	for i, sf := range m.savedFilters {
		marker := "  "
		if i == m.filterPickCursor {
			marker = "> "
		}
		line := fmt.Sprintf("%s%-20s  %s", marker, sf.Name, truncate(sf.Query, w-26))
		if i == m.filterPickCursor {
			line = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true).Render(line)
		}
		b.WriteString(line + "\n")
	}
	b.WriteString("\n")
	b.WriteString(statusStyle.Render("↑/↓ nav  •  enter apply  •  d delete  •  esc cancel"))
	dialog := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("205")).
		Padding(1, 2).
		Width(w).
		Render(b.String())
	if m.width > 0 && m.height > 0 {
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, dialog)
	}
	return dialog
}

// saveFilterPromptView renders the "save filter as…" inline prompt as a
// compact bottom-of-screen panel.
func (m Model) saveFilterPromptView() string {
	hint := statusStyle.Render("enter save  •  esc cancel")
	return m.saveFilterIn.View() + "\n" +
		statusStyle.Render("query: ") + m.pendingSaveQry + "\n" +
		hint
}

// Need to silence go's "imported and not used" when textinput is the only
// reason we import this — referenced via saveFilterIn fields elsewhere.
var _ = textinput.New
