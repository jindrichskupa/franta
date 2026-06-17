package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// clusterSwitchedMsg reports the result of a cluster switch.
type clusterSwitchedMsg struct {
	cluster string
	gen     int64
	err     error
}

// openClusterPicker opens the picker if more than one cluster is configured.
func (m Model) openClusterPicker() Model {
	if m.switchClusterFn == nil || len(m.clusters) <= 1 {
		m.status = "only one cluster configured"
		return m
	}
	m.pickingCluster = true
	m.clusterPickCursor = 0
	for i, c := range m.clusters {
		if c == m.cluster {
			m.clusterPickCursor = i
		}
	}
	return m
}

// switchClusterCmd invokes the switch callback for a cluster name.
func (m Model) switchClusterCmd(name string) tea.Cmd {
	fn := m.switchClusterFn
	return func() tea.Msg {
		if fn == nil {
			return clusterSwitchedMsg{cluster: name, err: fmt.Errorf("switch disabled")}
		}
		gen, err := fn(name)
		return clusterSwitchedMsg{cluster: name, gen: gen, err: err}
	}
}

// updateClusterPicker handles keys while the cluster picker is open.
func (m Model) updateClusterPicker(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.pickingCluster = false
		return m, nil
	case tea.KeyUp:
		if m.clusterPickCursor > 0 {
			m.clusterPickCursor--
		}
		return m, nil
	case tea.KeyDown:
		if m.clusterPickCursor < len(m.clusters)-1 {
			m.clusterPickCursor++
		}
		return m, nil
	case tea.KeyEnter:
		if m.clusterPickCursor < 0 || m.clusterPickCursor >= len(m.clusters) {
			return m, nil
		}
		name := m.clusters[m.clusterPickCursor]
		m.pickingCluster = false
		if name == m.cluster {
			m.status = "already on " + name
			return m, nil
		}
		m.status = "switching to " + name + "…"
		return m, m.switchClusterCmd(name)
	}
	return m, nil
}

// clusterPickerView renders the cluster picker as a centered modal.
func (m Model) clusterPickerView() string {
	w := m.width * 50 / 100
	if w < 40 {
		w = 40
	}
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	var b strings.Builder
	b.WriteString(titleStyle.Render("Switch cluster") + "\n\n")
	for i, c := range m.clusters {
		marker := "  "
		if i == m.clusterPickCursor {
			marker = "> "
		}
		active := ""
		if c == m.cluster {
			active = "  (current)"
		}
		line := marker + c + active
		if i == m.clusterPickCursor {
			line = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true).Render(line)
		}
		b.WriteString(line + "\n")
	}
	b.WriteString("\n" + statusStyle.Render("↑/↓ nav  •  enter switch  •  esc cancel"))
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
