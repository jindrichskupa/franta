package tui

import (
	"fmt"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"franta/internal/jsontree"
	"franta/internal/record"
)

// Tree colour palette. Keys and scalar types are coloured by type so a nested
// document scans quickly; markers/punctuation are faint to recede.
var (
	treeKeyStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))  // key — blue
	treeStrStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("82"))  // string — green
	treeNumStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("220")) // number — yellow
	treeBoolStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("213")) // bool — magenta
	treeNullStyle   = lipgloss.NewStyle().Faint(true)                       // null — grey
	treePunctStyle  = lipgloss.NewStyle().Faint(true)                       // markers / preview / ':'
	treeCursorStyle = lipgloss.NewStyle().Background(lipgloss.Color("236")).Bold(true)
)

// recKey uniquely identifies a record for change-detection (so the tree is
// rebuilt — and its fold state reset — only when the selection actually moves,
// not on every streamed record for the same selection).
func recKey(r record.Record) string {
	return fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset)
}

// scalarStyle maps a scalar type to its colour.
func scalarStyle(t jsontree.SType) lipgloss.Style {
	switch t {
	case jsontree.SStr:
		return treeStrStyle
	case jsontree.SNum:
		return treeNumStyle
	case jsontree.SBool:
		return treeBoolStyle
	case jsontree.SNull:
		return treeNullStyle
	default:
		return lipgloss.NewStyle()
	}
}

// renderTreeRow renders one flattened tree row, indented by depth and coloured
// by type. The cursor row gets a highlight background.
func renderTreeRow(r jsontree.Row, cursor bool) string {
	var b strings.Builder
	b.WriteString(strings.Repeat("  ", r.Depth))
	if r.Marker != ' ' {
		b.WriteString(treePunctStyle.Render(string(r.Marker)) + " ")
	}
	if r.Node.HasKey {
		b.WriteString(treeKeyStyle.Render(r.Key) + treePunctStyle.Render(": "))
	}
	switch {
	case r.Preview != "": // folded container
		b.WriteString(treePunctStyle.Render(r.Preview))
	case r.Marker == ' ': // scalar leaf
		txt := r.Scalar
		if r.SType == jsontree.SStr {
			txt = strconv.Quote(r.Scalar)
		}
		b.WriteString(scalarStyle(r.SType).Render(txt))
	}
	line := b.String()
	if cursor {
		return treeCursorStyle.Render(line)
	}
	return line
}

// detailTreeView renders the fixed metadata block followed by the foldable JSON
// value tree, windowed so the cursor row stays visible within innerH lines.
func (m Model) detailTreeView(innerH int) string {
	r, ok := m.selectedRecord()
	if !ok {
		return ""
	}
	// Treat the metadata block + the tree rows as one scrollable list so the
	// whole panel scrolls together. On a short pane the meta block scrolls off
	// as the cursor moves down, instead of pinning it and leaving only a few
	// rows of value visible.
	metaLines := strings.Split(strings.TrimRight(detailMetaBlock(r), "\n")+"\nvalue:", "\n")
	nMeta := len(metaLines)
	if innerH < 1 {
		innerH = 1
	}
	total := nMeta + len(m.detailRows)
	cursor := nMeta + m.detailTreeCursor // tree cursor offset past the meta lines
	return windowedList(total, cursor, innerH, func(i int) string {
		if i < nMeta {
			return metaLines[i] + "\n"
		}
		j := i - nMeta
		return renderTreeRow(m.detailRows[j], j == m.detailTreeCursor) + "\n"
	})
}

// toggleDetailRaw flips between the raw viewport and the JSON tree. Toggling to
// tree on a non-JSON value is a no-op that reports why via the status line.
func (m Model) toggleDetailRaw() Model {
	if !m.detailRaw && m.detailTree != nil {
		// Currently showing a tree → switch to raw.
		m.detailRaw = true
		m.detailRecKey = ""
		m.refreshDetail()
		m.status = "raw view"
		return m
	}
	// Currently raw (explicit or non-JSON fallback) → attempt the tree.
	m.detailRaw = false
	m.detailRecKey = "" // force a rebuild on the next refresh
	m.refreshDetail()
	if m.detailTree == nil {
		m.detailRaw = true // value isn't JSON — stay on raw
		m.status = "value is not JSON"
	} else {
		m.status = "tree view"
	}
	return m
}

// updateDetailTree handles navigation + fold keys while the JSON tree is shown.
// (space/g are intercepted globally for pause/groups, so fold is enter-only and
// top/bottom use home/end — matching the topics pane.)
func (m Model) updateDetailTree(msg tea.KeyMsg) Model {
	switch msg.Type {
	case tea.KeyUp:
		if m.detailTreeCursor > 0 {
			m.detailTreeCursor--
		}
	case tea.KeyDown:
		if m.detailTreeCursor < len(m.detailRows)-1 {
			m.detailTreeCursor++
		}
	case tea.KeyPgUp:
		m.detailTreeCursor -= 10
		if m.detailTreeCursor < 0 {
			m.detailTreeCursor = 0
		}
	case tea.KeyPgDown:
		m.detailTreeCursor += 10
		if m.detailTreeCursor >= len(m.detailRows) {
			m.detailTreeCursor = len(m.detailRows) - 1
		}
		if m.detailTreeCursor < 0 {
			m.detailTreeCursor = 0
		}
	case tea.KeyHome:
		m.detailTreeCursor = 0
	case tea.KeyEnd:
		if n := len(m.detailRows); n > 0 {
			m.detailTreeCursor = n - 1
		}
	case tea.KeyEnter:
		m.foldAtCursor(toggle)
	case tea.KeyLeft:
		m.foldAtCursor(collapse)
	case tea.KeyRight:
		m.foldAtCursor(expand)
	}
	return m
}

type foldOp int

const (
	toggle foldOp = iota
	collapse
	expand
)

// foldAtCursor applies a fold operation to the container under the tree cursor
// and re-flattens. Scalars and no-op transitions are ignored.
func (m *Model) foldAtCursor(op foldOp) {
	if m.detailTreeCursor < 0 || m.detailTreeCursor >= len(m.detailRows) {
		return
	}
	n := m.detailRows[m.detailTreeCursor].Node
	if n.Kind == jsontree.Scalar {
		return
	}
	switch op {
	case toggle:
		n.Collapsed = !n.Collapsed
	case collapse:
		if n.Collapsed {
			return
		}
		n.Collapsed = true
	case expand:
		if !n.Collapsed {
			return
		}
		n.Collapsed = false
	}
	m.detailRows = m.detailTree.Rows()
	if m.detailTreeCursor >= len(m.detailRows) {
		m.detailTreeCursor = len(m.detailRows) - 1
	}
	if m.detailTreeCursor < 0 {
		m.detailTreeCursor = 0
	}
}
