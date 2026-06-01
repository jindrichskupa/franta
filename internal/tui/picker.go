package tui

import (
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

type picker struct {
	names  []string
	cursor int
	choice string
	done   bool
}

func newPicker(names []string) picker {
	cp := make([]string, len(names))
	copy(cp, names)
	sort.Strings(cp)
	return picker{names: cp}
}

func (p picker) Choice() string { return p.choice }

func (p picker) Init() tea.Cmd { return nil }

func (p picker) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if k, ok := msg.(tea.KeyMsg); ok {
		switch k.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			p.done = true
			return p, tea.Quit
		case tea.KeyUp:
			if p.cursor > 0 {
				p.cursor--
			}
		case tea.KeyDown:
			if p.cursor < len(p.names)-1 {
				p.cursor++
			}
		case tea.KeyEnter:
			if len(p.names) > 0 {
				p.choice = p.names[p.cursor]
			}
			p.done = true
			return p, tea.Quit
		}
	}
	return p, nil
}

func (p picker) View() string {
	var b strings.Builder
	b.WriteString("Select a cluster:\n\n")
	for i, n := range p.names {
		cursor := "  "
		if i == p.cursor {
			cursor = "> "
		}
		b.WriteString(cursor + n + "\n")
	}
	b.WriteString("\n↑/↓: move  enter: select  esc: cancel")
	return b.String()
}

// PickCluster runs an interactive picker and returns the chosen cluster name,
// or "" if cancelled.
func PickCluster(names []string) (string, error) {
	final, err := tea.NewProgram(newPicker(names)).Run()
	if err != nil {
		return "", err
	}
	return final.(picker).Choice(), nil
}
