package tui

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"franta/internal/kafka"
)

// groupMutatedMsg reports the result of a destructive group action.
type groupMutatedMsg struct {
	action string // "deleted" | "reset"
	group  string
	err    error
}

// capitalize upper-cases the first rune of s. Used for status/error prefixes in
// place of the deprecated strings.Title.
func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// currentGroup returns the group under the list cursor, and false if none.
func (m Model) currentGroup() (kafka.GroupInfo, bool) {
	if m.groupCursor < 0 || m.groupCursor >= len(m.filteredGroups) {
		return kafka.GroupInfo{}, false
	}
	return m.groups[m.filteredGroups[m.groupCursor]], true
}

// deletableState reports whether a group in this state may be deleted.
func deletableState(state string) bool {
	return state == "Empty" || state == "Dead"
}

// deleteGroupCmd runs the delete callback for a group.
func (m Model) deleteGroupCmd(group string) tea.Cmd {
	fn := m.deleteGroupFn
	return func() tea.Msg {
		if fn == nil {
			return groupMutatedMsg{action: "deleted", group: group, err: fmt.Errorf("delete disabled")}
		}
		return groupMutatedMsg{action: "deleted", group: group, err: fn(group)}
	}
}

// beginDeleteGroup opens the type-to-confirm modal for the selected group, with
// a state safety gate.
func (m Model) beginDeleteGroup() (Model, tea.Cmd) {
	g, ok := m.currentGroup()
	if !ok {
		return m, nil
	}
	if m.deleteGroupFn == nil {
		m.status = "delete disabled"
		return m, nil
	}
	if !deletableState(g.State) {
		m.errDialog = fmt.Sprintf("Cannot delete group\n\ngroup %q has active members (state: %s); only Empty/Dead groups can be deleted", g.Name, g.State)
		return m, nil
	}
	name := g.Name
	m = m.askConfirmTyped(
		fmt.Sprintf("Delete group %q? Type the name to confirm.", name),
		name,
		func() tea.Cmd { return m.deleteGroupCmd(name) },
	)
	return m, nil
}

var resetTargets = []string{"beginning", "end", "timestamp…", "specific offset…"}

// beginReset opens the reset target picker, gated on group state.
func (m Model) beginReset() (Model, tea.Cmd) {
	g, ok := m.currentGroup()
	if !ok {
		return m, nil
	}
	if m.resetOffsetsFn == nil {
		m.status = "reset disabled"
		return m, nil
	}
	if !deletableState(g.State) {
		m.errDialog = fmt.Sprintf("Cannot reset offsets\n\ngroup %q has active members (state: %s); the group must be Empty before resetting offsets", g.Name, g.State)
		return m, nil
	}
	m.resetActive = true
	m.resetCursor = 0
	m.resetGroup = g.Name
	return m, nil
}

// resetCmd performs a reset with the given spec for the active reset group.
func (m Model) resetCmd(spec kafka.ResetSpec) tea.Cmd {
	fn := m.resetOffsetsFn
	group := m.resetGroup
	return func() tea.Msg {
		if fn == nil {
			return groupMutatedMsg{action: "reset", group: group, err: fmt.Errorf("reset disabled")}
		}
		return groupMutatedMsg{action: "reset", group: group, err: fn(group, spec)}
	}
}

// updateReset handles keys while the reset target picker is open.
func (m Model) updateReset(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.resetActive = false
		return m, nil
	case tea.KeyUp:
		if m.resetCursor > 0 {
			m.resetCursor--
		}
		return m, nil
	case tea.KeyDown:
		if m.resetCursor < len(resetTargets)-1 {
			m.resetCursor++
		}
		return m, nil
	case tea.KeyEnter:
		return m.chooseResetTarget()
	}
	return m, nil
}

func (m Model) chooseResetTarget() (tea.Model, tea.Cmd) {
	switch m.resetCursor {
	case 0: // beginning
		m.resetActive = false
		grp := m.resetGroup
		m = m.askConfirm(fmt.Sprintf("Reset group %q to beginning?", grp),
			func() tea.Cmd { return m.resetCmd(kafka.ResetSpec{Kind: kafka.ResetBeginning}) })
		return m, nil
	case 1: // end
		m.resetActive = false
		grp := m.resetGroup
		m = m.askConfirm(fmt.Sprintf("Reset group %q to end?", grp),
			func() tea.Cmd { return m.resetCmd(kafka.ResetSpec{Kind: kafka.ResetEnd}) })
		return m, nil
	case 2: // timestamp
		m.resetActive = false
		in := textinput.New()
		in.Prompt = "at: "
		in.Placeholder = "1h | 2d | 2026-06-01T00:00:00Z"
		in.Focus()
		m.resetTSInput = in
		m.resetTSActive = true
		return m, nil
	default: // specific offset
		m.resetActive = false
		return m.openExplicitReset()
	}
}

// updateResetTS handles the timestamp input.
func (m Model) updateResetTS(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.resetTSActive = false
		return m, nil
	case tea.KeyEnter:
		spec, err := kafka.ParseStart(strings.TrimSpace(m.resetTSInput.Value()))
		if err != nil || spec.Kind != kafka.StartTimestamp {
			m.status = "enter a duration (1h/2d) or RFC3339 time"
			return m, nil
		}
		m.resetTSActive = false
		grp := m.resetGroup
		at := spec.Time
		m = m.askConfirm(fmt.Sprintf("Reset group %q to %s?", grp, at.Format("2006-01-02T15:04:05Z")),
			func() tea.Cmd { return m.resetCmd(kafka.ResetSpec{Kind: kafka.ResetTimestamp, At: at}) })
		return m, nil
	}
	var cmd tea.Cmd
	m.resetTSInput, cmd = m.resetTSInput.Update(msg)
	return m, cmd
}

// explicitRow is one editable partition offset.
type explicitRow struct {
	topic     string
	partition int32
	committed int64
	end       int64
	value     string // user-edited NEW offset (defaults to committed)
}

// openExplicitReset builds the editable table from the cached group detail. It
// requires a loaded detail (the user has been on the row, which auto-describes).
func (m Model) openExplicitReset() (tea.Model, tea.Cmd) {
	d := m.groupDetail
	if d == nil || len(d.Lag) == 0 || d.Name != m.resetGroup {
		m.errDialog = "Cannot edit offsets\n\nno per-partition data for this group yet; wait for the detail to load and retry"
		return m, nil
	}
	rows := make([]explicitRow, 0, len(d.Lag))
	for _, r := range d.Lag {
		rows = append(rows, explicitRow{
			topic:     r.Topic,
			partition: r.Partition,
			committed: r.Committed,
			end:       r.End,
			value:     strconv.FormatInt(r.Committed, 10),
		})
	}
	m.explicitRows = rows
	m.explicitCursor = 0
	in := textinput.New()
	in.Prompt = ""
	in.SetValue(rows[0].value)
	in.Focus()
	m.explicitInput = in
	m.explicitActive = true
	return m, nil
}

func (m *Model) syncExplicitInputToRow() {
	if m.explicitCursor >= 0 && m.explicitCursor < len(m.explicitRows) {
		m.explicitRows[m.explicitCursor].value = m.explicitInput.Value()
	}
}

func (m *Model) loadExplicitRowToInput() {
	if m.explicitCursor >= 0 && m.explicitCursor < len(m.explicitRows) {
		m.explicitInput.SetValue(m.explicitRows[m.explicitCursor].value)
		m.explicitInput.CursorEnd()
	}
}

// updateExplicit handles keys in the per-partition offset editor.
func (m Model) updateExplicit(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.explicitActive = false
		return m, nil
	case tea.KeyUp:
		m.syncExplicitInputToRow()
		if m.explicitCursor > 0 {
			m.explicitCursor--
		}
		m.loadExplicitRowToInput()
		return m, nil
	case tea.KeyDown, tea.KeyTab:
		m.syncExplicitInputToRow()
		if m.explicitCursor < len(m.explicitRows)-1 {
			m.explicitCursor++
		}
		m.loadExplicitRowToInput()
		return m, nil
	case tea.KeyCtrlS:
		return m.submitExplicit()
	}
	var cmd tea.Cmd
	m.explicitInput, cmd = m.explicitInput.Update(msg)
	return m, cmd
}

// submitExplicit validates all rows, builds a ResetSpec, and asks confirm.
func (m Model) submitExplicit() (tea.Model, tea.Cmd) {
	m.syncExplicitInputToRow()
	offsets := map[string]map[int32]int64{}
	changed := 0
	for _, r := range m.explicitRows {
		v, err := strconv.ParseInt(strings.TrimSpace(r.value), 10, 64)
		if err != nil || v < 0 {
			m.status = fmt.Sprintf("invalid offset for %s[%d]: %q", r.topic, r.partition, r.value)
			return m, nil
		}
		if r.end >= 0 && v > r.end {
			m.status = fmt.Sprintf("%s[%d]: offset %d exceeds end %d", r.topic, r.partition, v, r.end)
			return m, nil
		}
		if v == r.committed {
			continue // unchanged
		}
		if offsets[r.topic] == nil {
			offsets[r.topic] = map[int32]int64{}
		}
		offsets[r.topic][r.partition] = v
		changed++
	}
	if changed == 0 {
		m.status = "no offsets changed"
		m.explicitActive = false
		return m, nil
	}
	m.explicitActive = false
	grp := m.resetGroup
	m = m.askConfirm(fmt.Sprintf("Apply %d offset change(s) to group %q?", changed, grp),
		func() tea.Cmd {
			return m.resetCmd(kafka.ResetSpec{Kind: kafka.ResetExplicit, Offsets: offsets})
		})
	return m, nil
}

func (m Model) resetPickerView() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	var b strings.Builder
	b.WriteString(titleStyle.Render(fmt.Sprintf("Reset offsets — group %q", m.resetGroup)) + "\n\n")
	for i, t := range resetTargets {
		cursor := "  "
		if i == m.resetCursor {
			cursor = "› "
		}
		b.WriteString(cursor + t + "\n")
	}
	b.WriteString("\n" + statusStyle.Render("↑/↓ select  •  enter choose  •  esc cancel"))
	return centeredBox(m, b.String(), 60)
}

func (m Model) resetTSView() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	body := titleStyle.Render(fmt.Sprintf("Reset %q to timestamp", m.resetGroup)) + "\n\n" +
		m.resetTSInput.View() + "\n\n" +
		statusStyle.Render("enter apply  •  esc cancel")
	if m.status != "" {
		body += "\n" + statusStyle.Render(m.status)
	}
	return centeredBox(m, body, 60)
}

func (m Model) explicitView() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	var b strings.Builder
	b.WriteString(titleStyle.Render(fmt.Sprintf("Edit offsets — group %q", m.resetGroup)) + "\n\n")
	fmt.Fprintf(&b, "%-20s %-6s %-12s %s\n", "TOPIC", "PART", "COMMITTED", "NEW")
	for i, r := range m.explicitRows {
		cursor := "  "
		newVal := r.value
		if i == m.explicitCursor {
			cursor = "› "
			newVal = m.explicitInput.View()
		}
		fmt.Fprintf(&b, "%s%-18s %-6d %-12d %s\n", cursor, truncate(r.topic, 18), r.partition, r.committed, newVal)
	}
	b.WriteString("\n" + statusStyle.Render("↑/↓ row  •  edit NEW  •  ctrl+s apply  •  esc cancel"))
	if m.status != "" {
		b.WriteString("\n" + statusStyle.Render(m.status))
	}
	return centeredBox(m, b.String(), 70)
}

// centeredBox renders content in a rounded modal centred on screen.
func centeredBox(m Model, content string, minW int) string {
	w := m.width * 60 / 100
	if w < minW {
		w = minW
	}
	dialog := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("205")).
		Padding(1, 2).
		Width(w - 2).
		Render(content)
	if m.width > 0 && m.height > 0 {
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, dialog)
	}
	return dialog
}
