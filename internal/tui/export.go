package tui

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"franta/internal/record"
)

// exportFormat enumerates the on-disk formats the export dialog can write.
type exportFormat int

const (
	fmtJSONL exportFormat = iota
	fmtJSONArray
	fmtCSV
	exportFormatCount
)

func (f exportFormat) label() string {
	switch f {
	case fmtJSONArray:
		return "JSON array"
	case fmtCSV:
		return "CSV"
	default:
		return "JSONL"
	}
}

func (f exportFormat) ext() string {
	switch f {
	case fmtJSONArray:
		return ".json"
	case fmtCSV:
		return ".csv"
	default:
		return ".jsonl"
	}
}

func (f exportFormat) write(w io.Writer, recs []record.Record) error {
	switch f {
	case fmtJSONArray:
		return record.WriteJSONArray(w, recs)
	case fmtCSV:
		return record.WriteCSV(w, recs)
	default:
		return record.WriteJSONL(w, recs)
	}
}

// exportDoneMsg reports the result of a file export.
type exportDoneMsg struct {
	n    int
	path string
	err  error
}

// homeDir is overridable in tests.
var homeDir = os.UserHomeDir

// defaultExportPath builds the suggested output filename for a topic + format.
func defaultExportPath(topic string, f exportFormat) string {
	name := topic
	if name == "" {
		name = "records"
	}
	ts := time.Now().UTC().Format("2006-01-02T15-04-05")
	return fmt.Sprintf("franta-%s-%s%s", name, ts, f.ext())
}

// expandPath expands a leading ~ to the user's home directory.
func expandPath(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		if home, err := homeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(p[1:], "/"))
		}
	}
	return p
}

// openExport captures the current filtered record set and opens the dialog. If
// nothing is visible it sets a status message and stays in normal mode.
func (m Model) openExport() Model {
	n := len(m.visible())
	if n == 0 {
		m.status = "nothing to export"
		return m
	}
	m.exportN = n
	m.exportFmt = fmtJSONL
	m.exportPathEdited = false
	in := textinput.New()
	in.Prompt = "path:    "
	in.SetValue(defaultExportPath(m.topic, m.exportFmt))
	in.CursorEnd()
	in.Focus()
	m.exportPath = in
	m.mode = modeExport
	return m
}

// updateExport handles keys while the export dialog is open.
func (m Model) updateExport(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.exportPath.Blur()
		m.mode = modeNormal
		return m, nil
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyCtrlS:
		return m.submitExport()
	case tea.KeyTab:
		m.exportFmt = (m.exportFmt + 1) % exportFormatCount
		if !m.exportPathEdited {
			m.exportPath.SetValue(defaultExportPath(m.topic, m.exportFmt))
			m.exportPath.CursorEnd()
		}
		return m, nil
	}
	// Any other key edits the path field.
	before := m.exportPath.Value()
	var cmd tea.Cmd
	m.exportPath, cmd = m.exportPath.Update(msg)
	if m.exportPath.Value() != before {
		m.exportPathEdited = true
	}
	return m, cmd
}

// submitExport writes the captured visible records to the chosen path/format.
func (m Model) submitExport() (tea.Model, tea.Cmd) {
	path := expandPath(strings.TrimSpace(m.exportPath.Value()))
	recs := m.visible()
	f := m.exportFmt
	open := m.openFn
	return m, func() tea.Msg {
		wc, err := open(path)
		if err != nil {
			return exportDoneMsg{path: path, err: err}
		}
		werr := f.write(wc, recs)
		cerr := wc.Close()
		if werr != nil {
			return exportDoneMsg{path: path, err: werr}
		}
		if cerr != nil {
			return exportDoneMsg{path: path, err: cerr}
		}
		return exportDoneMsg{n: len(recs), path: path}
	}
}

// exportView renders the centred export modal.
func (m Model) exportView() string {
	dlgW := m.width * 60 / 100
	if dlgW < 56 {
		dlgW = 56
	}
	innerW := dlgW - 6
	m.exportPath.Width = innerW - 10

	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	muted := lipgloss.NewStyle().Faint(true)

	var b strings.Builder
	b.WriteString(titleStyle.Render(fmt.Sprintf("Export %d records (filtered)", m.exportN)) + "\n\n")
	fmt.Fprintf(&b, "format:  %s\n", titleStyle.Render("‹ "+m.exportFmt.label()+" ›"))
	b.WriteString(m.exportPath.View() + "\n\n")
	b.WriteString(statusStyle.Render("tab cycle format  •  ctrl+s write  •  esc cancel"))
	if m.status != "" {
		b.WriteString("\n" + muted.Render(m.status))
	}

	dialog := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("205")).
		Padding(1, 2).
		Width(dlgW - 2).
		Render(b.String())
	if m.width > 0 && m.height > 0 {
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, dialog)
	}
	return dialog
}
