package tui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// subjectGroup collects all registered versions of one subject.
type subjectGroup struct {
	Name     string
	Type     string // type of the latest version
	Versions []SchemaEntry
}

// schemasMsg carries the result of a BrowseSchemas fetch.
type schemasMsg struct {
	entries []SchemaEntry
	err     error
}

func (m Model) browseSchemasCmd() tea.Cmd {
	fn := m.browseSchemasFn
	return func() tea.Msg {
		if fn == nil {
			return schemasMsg{}
		}
		entries, err := fn()
		return schemasMsg{entries: entries, err: err}
	}
}

// openSchemaBrowse enters the schema-registry screen and kicks off the fetch.
func (m Model) openSchemaBrowse() (Model, tea.Cmd) {
	if m.browseSchemasFn == nil {
		return m, nil
	}
	m.mode = modeSchema
	m.schemas = nil
	m.schemaErr = ""
	m.schemaCursor = 0
	m.schemaPartsFocused = false
	m.schemaSearch = ""
	m.searchingSchema = false
	return m, m.browseSchemasCmd()
}

// groupSubjects groups flat schema entries by subject, sorting subjects by name
// and each subject's versions ascending. The group Type is the latest version's.
func groupSubjects(entries []SchemaEntry) []subjectGroup {
	byName := map[string]*subjectGroup{}
	var order []string
	for _, e := range entries {
		g, ok := byName[e.Subject]
		if !ok {
			g = &subjectGroup{Name: e.Subject}
			byName[e.Subject] = g
			order = append(order, e.Subject)
		}
		g.Versions = append(g.Versions, e)
	}
	sort.Strings(order)
	out := make([]subjectGroup, 0, len(order))
	for _, name := range order {
		g := byName[name]
		sort.Slice(g.Versions, func(i, j int) bool { return g.Versions[i].Version < g.Versions[j].Version })
		g.Type = g.Versions[len(g.Versions)-1].Type
		out = append(out, *g)
	}
	return out
}

// typeInitial is the single-letter abbreviation for a schema type.
func typeInitial(t string) string {
	if t == "" {
		return "?"
	}
	return t[:1]
}

// schemaText pretty-prints schema text: valid JSON (Avro/JSON schemas) is
// indented; anything else (protobuf) passes through unchanged.
// ponytail: indent-or-raw, no schema parsing.
func schemaText(text string) string {
	if json.Valid([]byte(text)) {
		var buf bytes.Buffer
		if err := json.Indent(&buf, []byte(text), "", "  "); err == nil {
			return buf.String()
		}
	}
	return text
}

// renderSubjectList renders one line per subject: cursor marker, name, type
// initial, and the selected version.
func renderSubjectList(groups []subjectGroup, filtered []int, cursor int, focused bool, sel map[string]int, nameW int) string {
	if len(groups) == 0 {
		return "  (no subjects)"
	}
	if len(filtered) == 0 {
		return "  (no subjects match)"
	}
	if nameW < 8 {
		nameW = 8
	}
	var b strings.Builder
	for i, gi := range filtered {
		g := groups[gi]
		marker := "  "
		if focused && i == cursor {
			marker = "> "
		}
		v := sel[g.Name]
		if v == 0 && len(g.Versions) > 0 {
			v = g.Versions[len(g.Versions)-1].Version
		}
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "%s%s %s v%d", marker, truncate(g.Name, nameW), typeInitial(g.Type), v)
	}
	return b.String()
}

// applySchemaFilter rebuilds filteredSubjects from the current search query.
func (m *Model) applySchemaFilter() {
	if m.schemaSearch == "" {
		m.filteredSubjects = make([]int, len(m.subjectGroups))
		for i := range m.subjectGroups {
			m.filteredSubjects[i] = i
		}
		return
	}
	type entry struct{ idx, score int }
	var matches []entry
	for i, g := range m.subjectGroups {
		if s, ok := fuzzyMatch(m.schemaSearch, g.Name); ok {
			matches = append(matches, entry{i, s})
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].score != matches[j].score {
			return matches[i].score > matches[j].score
		}
		return m.subjectGroups[matches[i].idx].Name < m.subjectGroups[matches[j].idx].Name
	})
	m.filteredSubjects = make([]int, len(matches))
	for k, e := range matches {
		m.filteredSubjects[k] = e.idx
	}
}

// currentSubject returns the group under the cursor in the filtered view.
func (m Model) currentSubject() (subjectGroup, bool) {
	if m.schemaCursor < 0 || m.schemaCursor >= len(m.filteredSubjects) {
		return subjectGroup{}, false
	}
	return m.subjectGroups[m.filteredSubjects[m.schemaCursor]], true
}

// selectedEntry returns the entry for the cursor subject at its selected
// version (defaulting to the latest).
func (m Model) selectedEntry() (SchemaEntry, subjectGroup, bool) {
	g, ok := m.currentSubject()
	if !ok || len(g.Versions) == 0 {
		return SchemaEntry{}, subjectGroup{}, false
	}
	want := m.schemaVersionSel[g.Name]
	for _, e := range g.Versions {
		if e.Version == want {
			return e, g, true
		}
	}
	// default: latest
	return g.Versions[len(g.Versions)-1], g, true
}

// refreshSchemaText repaints the right pane from the current selection.
func (m *Model) refreshSchemaText() {
	e, _, ok := m.selectedEntry()
	if !ok {
		m.schemaVP.SetContent("")
		return
	}
	m.schemaVP.SetContent(wrapForVP(schemaText(e.Text), m.schemaVP.Width))
	m.schemaVP.GotoTop()
}

// stepVersion moves the cursor subject's selected version by delta, clamped.
func (m *Model) stepVersion(delta int) {
	g, ok := m.currentSubject()
	if !ok || len(g.Versions) == 0 {
		return
	}
	cur := m.schemaVersionSel[g.Name]
	// locate current index (default latest)
	idx := len(g.Versions) - 1
	for i, e := range g.Versions {
		if e.Version == cur {
			idx = i
			break
		}
	}
	idx += delta
	if idx < 0 {
		idx = 0
	}
	if idx >= len(g.Versions) {
		idx = len(g.Versions) - 1
	}
	if m.schemaVersionSel == nil {
		m.schemaVersionSel = map[string]int{}
	}
	m.schemaVersionSel[g.Name] = g.Versions[idx].Version
	m.refreshSchemaText()
}

func (m Model) updateSchema(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.searchingSchema {
		return m.updateSchemaSearchPrompt(msg)
	}
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		if m.schemaSearch != "" {
			m.schemaSearch = ""
			m.applySchemaFilter()
			m.schemaCursor = 0
			m.refreshSchemaText()
			return m, nil
		}
		m.mode = modeNormal
		return m, nil
	case tea.KeyTab, tea.KeyShiftTab:
		m.schemaPartsFocused = !m.schemaPartsFocused
		return m, nil
	}
	switch msg.String() {
	case "q":
		return m, tea.Quit
	case "1":
		m.schemaPartsFocused = false
		return m, nil
	case "2":
		m.schemaPartsFocused = true
		return m, nil
	case "/":
		m.searchingSchema = true
		return m, nil
	case "r":
		m.schemas = nil
		m.schemaErr = ""
		return m, m.browseSchemasCmd()
	case "[":
		m.stepVersion(-1)
		return m, nil
	case "]":
		m.stepVersion(1)
		return m, nil
	}
	if m.schemaPartsFocused {
		var cmd tea.Cmd
		m.schemaVP, cmd = m.schemaVP.Update(msg)
		return m, cmd
	}
	// left pane: subject cursor nav
	n := len(m.filteredSubjects)
	switch msg.Type {
	case tea.KeyUp:
		if m.schemaCursor > 0 {
			m.schemaCursor--
		}
		m.refreshSchemaText()
	case tea.KeyDown:
		if m.schemaCursor < n-1 {
			m.schemaCursor++
		}
		m.refreshSchemaText()
	case tea.KeyPgUp:
		m.schemaCursor -= 10
		if m.schemaCursor < 0 {
			m.schemaCursor = 0
		}
		m.refreshSchemaText()
	case tea.KeyPgDown:
		m.schemaCursor += 10
		if m.schemaCursor >= n {
			m.schemaCursor = n - 1
		}
		if m.schemaCursor < 0 {
			m.schemaCursor = 0
		}
		m.refreshSchemaText()
	case tea.KeyHome:
		m.schemaCursor = 0
		m.refreshSchemaText()
	case tea.KeyEnd:
		if n > 0 {
			m.schemaCursor = n - 1
		}
		m.refreshSchemaText()
	}
	return m, nil
}

func (m Model) updateSchemaSearchPrompt(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		m.searchingSchema = false
		return m, nil
	case tea.KeyEsc:
		m.searchingSchema = false
		m.schemaSearch = ""
		m.applySchemaFilter()
		m.schemaCursor = 0
		m.refreshSchemaText()
		return m, nil
	case tea.KeyBackspace:
		if m.schemaSearch != "" {
			m.schemaSearch = m.schemaSearch[:len(m.schemaSearch)-1]
		}
	case tea.KeyRunes, tea.KeySpace:
		m.schemaSearch += string(msg.Runes)
	}
	m.applySchemaFilter()
	m.schemaCursor = 0
	m.refreshSchemaText()
	return m, nil
}

func (m Model) schemaView() string {
	header := statusStyle.Render(fmt.Sprintf("schema registry  subjects:%d", len(m.subjectGroups)))

	leftW, rightW, msgsH, detailH := paneSizes(m.width, m.height)
	innerH := msgsH + detailH

	listFocused := !m.schemaPartsFocused
	var leftBody string
	switch {
	case m.schemaErr != "":
		leftBody = "failed to load schemas:\n" + m.schemaErr
	case m.schemas == nil:
		leftBody = "loading…"
	default:
		leftBody = renderSubjectList(m.subjectGroups, m.filteredSubjects, m.schemaCursor, listFocused, m.schemaVersionSel, leftW-6)
	}
	leftContent := paneTitle("subjects", listFocused) + "\n" + leftBody
	leftBox := paneStyle(listFocused).Width(leftW - 2).Height(innerH - 2).Render(leftContent)

	rightTitle := "schema"
	if e, g, ok := m.selectedEntry(); ok {
		rightTitle = fmt.Sprintf("%s  v%d/%d  %s", g.Name, e.Version, len(g.Versions), g.Type)
	}
	rightContent := paneTitle(rightTitle, m.schemaPartsFocused) + "\n" + m.schemaVP.View()
	rightBox := paneStyle(m.schemaPartsFocused).Width(rightW - 2).Height(innerH - 2).Render(rightContent)

	body := lipgloss.JoinHorizontal(lipgloss.Top, leftBox, rightBox)
	var footer string
	if m.searchingSchema {
		footer = m.schemaSearchPanel()
	} else {
		hint := "tab/1/2 pane  •  ↑/↓ subjects  •  [ ] version  •  / search  •  r reload  •  esc back  •  q quit"
		prefix := ""
		if m.schemaSearch != "" {
			prefix = fmt.Sprintf("[search: %s  %d/%d] ", m.schemaSearch, len(m.filteredSubjects), len(m.subjectGroups))
		}
		footer = statusStyle.Render(prefix + hint)
	}
	return header + "\n" + body + "\n" + footer
}

func (m Model) schemaSearchPanel() string {
	prompt := lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("subject search> ")
	cursor := lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("▎")
	counter := lipgloss.NewStyle().Faint(true).Render(fmt.Sprintf("  %d/%d", len(m.filteredSubjects), len(m.subjectGroups)))
	help := statusStyle.Render("enter apply  •  esc cancel  •  empty input clears the search")
	return prompt + m.schemaSearch + cursor + counter + "\n" + help
}
