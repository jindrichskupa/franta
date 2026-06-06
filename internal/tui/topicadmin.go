package tui

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// topicMutatedMsg reports the result of a topic admin action.
type topicMutatedMsg struct {
	action string // "created" | "deleted" | "repartitioned" | "configured"
	topic  string
	err    error
}

// currentTopicName returns the selected topic name and false if none.
func (m Model) currentTopicName() (string, bool) {
	idx := m.topicCursor
	if idx < 0 || idx >= len(m.filteredTopics) {
		return "", false
	}
	return m.topics[m.filteredTopics[idx]].Name, true
}

// currentTopicPartitions returns the selected topic's partition count.
func (m Model) currentTopicPartitions() int {
	idx := m.topicCursor
	if idx < 0 || idx >= len(m.filteredTopics) {
		return 0
	}
	return m.topics[m.filteredTopics[idx]].Partitions
}

// reloadTopicsCmd bumps the load generation and refetches the topic list
// (mirrors the manual "r" reload).
func (m *Model) reloadTopicsCmd() tea.Cmd {
	if m.listTopics == nil {
		return nil
	}
	m.topicLoadGen++
	m.topicsLoading = true
	for i := range m.topics {
		m.topics[i].Messages = -1
	}
	return m.loadTopicsCmd()
}

// ---------------------------------------------------------------------------
// Task 3: delete topic (D)
// ---------------------------------------------------------------------------

// deleteTopicCmd runs the delete callback.
func (m Model) deleteTopicCmd(name string) tea.Cmd {
	fn := m.deleteTopicFn
	return func() tea.Msg {
		if fn == nil {
			return topicMutatedMsg{action: "deleted", topic: name, err: fmt.Errorf("delete disabled")}
		}
		return topicMutatedMsg{action: "deleted", topic: name, err: fn(name)}
	}
}

// beginDeleteTopic opens a type-to-confirm modal for the selected topic, with a
// guard against deleting internal (_-prefixed) topics unless show-internal is on.
func (m Model) beginDeleteTopic() (Model, tea.Cmd) {
	name, ok := m.currentTopicName()
	if !ok {
		return m, nil
	}
	if m.deleteTopicFn == nil {
		m.status = "delete disabled"
		return m, nil
	}
	if strings.HasPrefix(name, "_") && !m.showInternal {
		m.errDialog = fmt.Sprintf("Refusing to delete internal topic\n\n%q looks internal; toggle 'i' to show internal topics if you really mean it", name)
		return m, nil
	}
	m = m.askConfirmTyped(
		fmt.Sprintf("Delete topic %q? Type the name to confirm.", name),
		name,
		func() tea.Cmd { return m.deleteTopicCmd(name) },
	)
	return m, nil
}

// ---------------------------------------------------------------------------
// Task 4: add partitions (a)
// ---------------------------------------------------------------------------

func (m Model) beginAddPartitions() (Model, tea.Cmd) {
	name, ok := m.currentTopicName()
	if !ok {
		return m, nil
	}
	if m.addPartitionsFn == nil {
		m.status = "add-partitions disabled"
		return m, nil
	}
	cur := m.currentTopicPartitions()
	in := textinput.New()
	in.Prompt = "count: "
	in.SetValue(strconv.Itoa(cur + 1))
	in.CursorEnd()
	in.Focus()
	m.apInput = in
	m.apTopic = name
	m.apCurrent = cur
	m.apActive = true
	return m, nil
}

func (m Model) addPartitionsCmd(name string, total int) tea.Cmd {
	fn := m.addPartitionsFn
	return func() tea.Msg {
		if fn == nil {
			return topicMutatedMsg{action: "repartitioned", topic: name, err: fmt.Errorf("disabled")}
		}
		return topicMutatedMsg{action: "repartitioned", topic: name, err: fn(name, total)}
	}
}

func (m Model) updateAddPartitions(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.apActive = false
		return m, nil
	case tea.KeyEnter:
		n, err := strconv.Atoi(strings.TrimSpace(m.apInput.Value()))
		if err != nil || n <= m.apCurrent {
			m.status = fmt.Sprintf("count must be an integer greater than %d", m.apCurrent)
			return m, nil
		}
		m.apActive = false
		name, total := m.apTopic, n
		m = m.askConfirm(
			fmt.Sprintf("Set %q to %d partitions? (keyed-message partitioning may change)", name, total),
			func() tea.Cmd { return m.addPartitionsCmd(name, total) },
		)
		return m, nil
	}
	var cmd tea.Cmd
	m.apInput, cmd = m.apInput.Update(msg)
	return m, cmd
}

func (m Model) addPartitionsView() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	body := titleStyle.Render(fmt.Sprintf("Add partitions — %q (current %d)", m.apTopic, m.apCurrent)) + "\n\n" +
		m.apInput.View() + "\n\n" +
		statusStyle.Render("enter apply  •  esc cancel")
	if m.status != "" {
		body += "\n" + statusStyle.Render(m.status)
	}
	return centeredBox(m, body, 56)
}

// ---------------------------------------------------------------------------
// Task 5: create topic (n)
// ---------------------------------------------------------------------------

const (
	ntFieldName   = 0
	ntFieldParts  = 1
	ntFieldRF     = 2
	ntFieldConfig = 3
	ntFieldCount  = 4
)

func (m Model) beginCreateTopic() (Model, tea.Cmd) {
	if m.createTopicFn == nil {
		m.status = "create disabled"
		return m, nil
	}
	name := textinput.New()
	name.Prompt = "name:        "
	name.Focus()
	parts := textinput.New()
	parts.Prompt = "partitions:  "
	parts.SetValue("1")
	rf := textinput.New()
	rf.Prompt = "replication: "
	rf.SetValue("1")
	ta := textarea.New()
	ta.Placeholder = "cleanup.policy=compact\nretention.ms=604800000"
	ta.SetWidth(60)
	ta.SetHeight(4)
	ta.Prompt = "│ "
	ta.ShowLineNumbers = false
	m.ntInputs = []textinput.Model{name, parts, rf}
	m.ntConfigsTA = ta
	m.ntFocus = ntFieldName
	m.ntActive = true
	return m, nil
}

// parseConfigLines parses "k=v" lines (and JSON object) into a config map.
// Reuses the header parser's tolerant splitting.
func parseConfigLines(s string) map[string]string {
	hs := parseHeaders(s) // returns []record.Header — k=v or JSON, comma/newline tolerant
	if len(hs) == 0 {
		return nil
	}
	out := make(map[string]string, len(hs))
	for _, h := range hs {
		out[h.Key] = string(h.Value)
	}
	return out
}

func validTopicName(s string) bool {
	if s == "" || s == "." || s == ".." || len(s) > 249 {
		return false
	}
	for _, r := range s {
		ok := r == '.' || r == '_' || r == '-' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if !ok {
			return false
		}
	}
	return true
}

func (m Model) createTopicCmd(name string, parts int32, rf int16, cfg map[string]string) tea.Cmd {
	fn := m.createTopicFn
	return func() tea.Msg {
		if fn == nil {
			return topicMutatedMsg{action: "created", topic: name, err: fmt.Errorf("disabled")}
		}
		return topicMutatedMsg{action: "created", topic: name, err: fn(name, parts, rf, cfg)}
	}
}

func (m *Model) ntSetFocus(i int) {
	for j := range m.ntInputs {
		m.ntInputs[j].Blur()
	}
	m.ntConfigsTA.Blur()
	if i == ntFieldConfig {
		m.ntConfigsTA.Focus()
	} else {
		m.ntInputs[i].Focus()
	}
	m.ntFocus = i
}

func (m Model) updateCreateTopic(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.ntActive = false
		return m, nil
	case tea.KeyTab:
		m.ntSetFocus((m.ntFocus + 1) % ntFieldCount)
		return m, nil
	case tea.KeyShiftTab:
		m.ntSetFocus((m.ntFocus - 1 + ntFieldCount) % ntFieldCount)
		return m, nil
	case tea.KeyCtrlS:
		return m.submitCreateTopic()
	case tea.KeyEnter:
		if m.ntFocus != ntFieldConfig {
			return m.submitCreateTopic()
		}
	}
	var cmd tea.Cmd
	if m.ntFocus == ntFieldConfig {
		m.ntConfigsTA, cmd = m.ntConfigsTA.Update(msg)
	} else {
		m.ntInputs[m.ntFocus], cmd = m.ntInputs[m.ntFocus].Update(msg)
	}
	return m, cmd
}

func (m Model) submitCreateTopic() (tea.Model, tea.Cmd) {
	name := strings.TrimSpace(m.ntInputs[ntFieldName].Value())
	if !validTopicName(name) {
		m.status = "invalid topic name"
		return m, nil
	}
	parts, err := strconv.Atoi(strings.TrimSpace(m.ntInputs[ntFieldParts].Value()))
	if err != nil || parts < 1 {
		m.status = "partitions must be ≥ 1"
		return m, nil
	}
	rf, err := strconv.Atoi(strings.TrimSpace(m.ntInputs[ntFieldRF].Value()))
	if err != nil || rf < 1 {
		m.status = "replication must be ≥ 1"
		return m, nil
	}
	cfg := parseConfigLines(m.ntConfigsTA.Value())
	m.ntActive = false
	return m, m.createTopicCmd(name, int32(parts), int16(rf), cfg)
}

func (m Model) createTopicView() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	muted := lipgloss.NewStyle().Faint(true)
	m.ntConfigsTA.SetWidth(56)
	var b strings.Builder
	b.WriteString(titleStyle.Render("Create topic") + "\n\n")
	for i := range m.ntInputs {
		b.WriteString(m.ntInputs[i].View() + "\n")
	}
	lbl := "configs:  (one k=v per line, optional)"
	if m.ntFocus == ntFieldConfig {
		b.WriteString("\n" + titleStyle.Render(lbl) + "\n")
	} else {
		b.WriteString("\n" + muted.Render(lbl) + "\n")
	}
	b.WriteString(m.ntConfigsTA.View() + "\n\n")
	b.WriteString(statusStyle.Render("tab/shift-tab field  •  ctrl+s create  •  enter on a single-line field  •  esc cancel"))
	if m.status != "" {
		b.WriteString("\n" + muted.Render(m.status))
	}
	return centeredBox(m, b.String(), 64)
}

// ---------------------------------------------------------------------------
// Task 6: config view / edit (c)
// ---------------------------------------------------------------------------

type tcRow struct {
	key      string
	value    string
	editable bool
}

// topicConfigLoadedMsg delivers DescribeTopicConfigs results.
type topicConfigLoadedMsg struct {
	topic string
	rows  []tcRow
	orig  map[string]string
	err   error
}

func (m Model) beginTopicConfig() (Model, tea.Cmd) {
	name, ok := m.currentTopicName()
	if !ok {
		return m, nil
	}
	if m.getTopicConfigFn == nil {
		m.status = "config view disabled"
		return m, nil
	}
	m.tcActive = true
	m.tcTopic = name
	m.tcLoading = true
	m.tcRows = nil
	fn := m.getTopicConfigFn
	return m, func() tea.Msg {
		entries, err := fn(name)
		if err != nil {
			return topicConfigLoadedMsg{topic: name, err: err}
		}
		rows := make([]tcRow, len(entries))
		orig := make(map[string]string, len(entries))
		for i, e := range entries {
			rows[i] = tcRow{key: e.Key, value: e.Value, editable: e.Editable}
			orig[e.Key] = e.Value
		}
		return topicConfigLoadedMsg{topic: name, rows: rows, orig: orig}
	}
}

func (m *Model) tcSyncInputToRow() {
	if m.tcCursor >= 0 && m.tcCursor < len(m.tcRows) && m.tcRows[m.tcCursor].editable {
		m.tcRows[m.tcCursor].value = m.tcInput.Value()
	}
}

func (m *Model) tcLoadRowToInput() {
	if m.tcCursor >= 0 && m.tcCursor < len(m.tcRows) && m.tcRows[m.tcCursor].editable {
		m.tcInput.SetValue(m.tcRows[m.tcCursor].value)
		m.tcInput.CursorEnd()
	} else {
		m.tcInput.SetValue("")
	}
}

func (m Model) setTopicConfigCmd(name string, set map[string]string) tea.Cmd {
	fn := m.setTopicConfigFn
	return func() tea.Msg {
		if fn == nil {
			return topicMutatedMsg{action: "configured", topic: name, err: fmt.Errorf("disabled")}
		}
		return topicMutatedMsg{action: "configured", topic: name, err: fn(name, set)}
	}
}

func (m Model) updateTopicConfig(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.tcLoading {
		switch msg.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit
		case tea.KeyEsc:
			m.tcActive = false
		}
		return m, nil
	}
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.tcActive = false
		return m, nil
	case tea.KeyUp:
		m.tcSyncInputToRow()
		if m.tcCursor > 0 {
			m.tcCursor--
		}
		m.tcLoadRowToInput()
		return m, nil
	case tea.KeyDown, tea.KeyTab:
		m.tcSyncInputToRow()
		if m.tcCursor < len(m.tcRows)-1 {
			m.tcCursor++
		}
		m.tcLoadRowToInput()
		return m, nil
	case tea.KeyCtrlS:
		return m.submitTopicConfig()
	}
	// Only editable rows accept edits.
	if m.tcCursor >= 0 && m.tcCursor < len(m.tcRows) && m.tcRows[m.tcCursor].editable {
		var cmd tea.Cmd
		m.tcInput, cmd = m.tcInput.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m Model) submitTopicConfig() (tea.Model, tea.Cmd) {
	m.tcSyncInputToRow()
	set := map[string]string{}
	for _, r := range m.tcRows {
		if !r.editable {
			continue
		}
		if r.value != m.tcOrig[r.key] {
			set[r.key] = r.value
		}
	}
	if len(set) == 0 {
		m.status = "no config changes"
		m.tcActive = false
		return m, nil
	}
	name := m.tcTopic
	n := len(set)
	m.tcActive = false
	m = m.askConfirm(fmt.Sprintf("Apply %d config change(s) to %q?", n, name),
		func() tea.Cmd { return m.setTopicConfigCmd(name, set) })
	return m, nil
}

func (m Model) topicConfigView() string {
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	muted := lipgloss.NewStyle().Faint(true)
	if m.tcLoading {
		return centeredBox(m, titleStyle.Render("Config — "+m.tcTopic)+"\n\nloading…", 64)
	}
	var b strings.Builder
	b.WriteString(titleStyle.Render("Config — "+m.tcTopic) + muted.Render("   (* = editable)") + "\n\n")
	for i, r := range m.tcRows {
		cursor := "  "
		val := r.value
		if i == m.tcCursor {
			cursor = "› "
			if r.editable {
				val = m.tcInput.View()
			}
		}
		star := " "
		line := fmt.Sprintf("%s%-32s %s", cursor, truncate(r.key, 32), val)
		if r.editable {
			star = "*"
		} else {
			line = muted.Render(line)
		}
		b.WriteString(star + line + "\n")
	}
	b.WriteString("\n" + statusStyle.Render("↑/↓ row  •  edit editable values  •  ctrl+s apply  •  esc cancel"))
	if m.status != "" {
		b.WriteString("\n" + muted.Render(m.status))
	}
	return centeredBox(m, b.String(), 72)
}
