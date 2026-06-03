package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"franta/internal/kafka"
)

func (m Model) loadGroupsCmd() tea.Cmd {
	fn := m.listGroupsFn
	gen := m.groupLoadGen
	return func() tea.Msg {
		gs, err := fn()
		return groupsLoadedMsg{groups: gs, gen: gen, err: err}
	}
}

// loadGroupLagsCmd fetches phase-2 total lag for the given group names,
// generation-tagged so reload mid-fetch supersedes the result.
func (m Model) loadGroupLagsCmd(names []string) tea.Cmd {
	fn := m.groupLagsFn
	gen := m.groupLoadGen
	return func() tea.Msg {
		if fn == nil {
			return groupLagsMsg{gen: gen}
		}
		lags, err := fn(names)
		return groupLagsMsg{lags: lags, gen: gen, err: err}
	}
}

func (m Model) describeGroupCmd(name string) tea.Cmd {
	fn := m.describeGroupFn
	return func() tea.Msg {
		if fn == nil {
			return groupDetailMsg{detail: &kafka.GroupDetail{Name: name}}
		}
		d, err := fn(name)
		if err != nil {
			return groupDetailMsg{err: err}
		}
		return groupDetailMsg{detail: &d}
	}
}

// refreshSelectedGroupDetail keeps the right pane in sync with the cursor on
// the left. Uses the in-memory cache; on miss, returns a describe cmd to fetch.
// Cursor indexes m.filteredGroups (the fuzzy-filtered view); empty filter
// passes through to all groups.
func (m *Model) refreshSelectedGroupDetail() tea.Cmd {
	if m.groupCursor < 0 || m.groupCursor >= len(m.filteredGroups) {
		m.groupDetail = nil
		m.groupDetailErr = ""
		m.groupDetailVP.SetContent("")
		return nil
	}
	name := m.groups[m.filteredGroups[m.groupCursor]].Name
	m.selectedGroup = name
	if m.groupDetails == nil {
		m.groupDetails = make(map[string]*kafka.GroupDetail)
	}
	if d, ok := m.groupDetails[name]; ok {
		m.groupDetail = d
		m.groupDetailErr = ""
		m.groupDetailLoading = false
		m.groupDetailVP.SetContent(wrapForVP(renderGroupDetail(d), m.groupDetailVP.Width))
		m.groupDetailVP.GotoTop()
		return nil
	}
	m.groupDetail = nil
	m.groupDetailErr = ""
	m.groupDetailLoading = true
	m.groupDetailVP.SetContent("loading group…")
	m.groupDetailVP.GotoTop()
	return m.describeGroupCmd(name)
}

// wrapForVP wraps content lines to the viewport width so long member
// assignments / topic-partition rows don't overflow the right pane.
func wrapForVP(s string, w int) string {
	if w <= 0 {
		return s
	}
	return lipgloss.NewStyle().Width(w).Render(s)
}

func (m Model) updateGroups(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Search input has priority when active (mirrors the topics pane).
	if m.searchingGroup {
		return m.updateGroupSearchPrompt(msg)
	}
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		// First esc clears an active filter; second exits the groups view.
		if m.groupSearch != "" {
			m.groupSearch = ""
			m.applyGroupFilter()
			return m, m.refreshSelectedGroupDetail()
		}
		m.mode = modeNormal
		return m, nil
	case tea.KeyTab:
		m.groupDetailFocused = !m.groupDetailFocused
		return m, nil
	case tea.KeyShiftTab:
		m.groupDetailFocused = !m.groupDetailFocused
		return m, nil
	}
	// When detail pane is focused, arrows scroll the viewport instead of
	// moving the list cursor.
	if m.groupDetailFocused {
		switch msg.String() {
		case "q":
			return m, tea.Quit
		case "1":
			m.groupDetailFocused = false
			return m, nil
		case "/":
			m.groupDetailFocused = false
			m.searchingGroup = true
			return m, nil
		case "r":
			return m, m.reloadGroupsCmd()
		}
		var cmd tea.Cmd
		m.groupDetailVP, cmd = m.groupDetailVP.Update(msg)
		return m, cmd
	}
	// List-pane navigation.
	switch msg.Type {
	case tea.KeyUp:
		if m.groupCursor > 0 {
			m.groupCursor--
		}
		return m, m.refreshSelectedGroupDetail()
	case tea.KeyDown:
		if m.groupCursor < len(m.filteredGroups)-1 {
			m.groupCursor++
		}
		return m, m.refreshSelectedGroupDetail()
	case tea.KeyPgUp:
		m.groupCursor -= 10
		if m.groupCursor < 0 {
			m.groupCursor = 0
		}
		return m, m.refreshSelectedGroupDetail()
	case tea.KeyPgDown:
		m.groupCursor += 10
		if m.groupCursor >= len(m.filteredGroups) {
			m.groupCursor = len(m.filteredGroups) - 1
		}
		if m.groupCursor < 0 {
			m.groupCursor = 0
		}
		return m, m.refreshSelectedGroupDetail()
	case tea.KeyHome:
		m.groupCursor = 0
		return m, m.refreshSelectedGroupDetail()
	case tea.KeyEnd:
		if n := len(m.filteredGroups); n > 0 {
			m.groupCursor = n - 1
		}
		return m, m.refreshSelectedGroupDetail()
	}
	switch msg.String() {
	case "q":
		return m, tea.Quit
	case "/":
		m.searchingGroup = true
		return m, nil
	case "2":
		m.groupDetailFocused = true
		return m, nil
	case "1":
		m.groupDetailFocused = false
		return m, nil
	case "r":
		return m, m.reloadGroupsCmd()
	case "o":
		// Cycle sort: name → lag↓ → members↓ → name. Re-apply filter to
		// resort; cursor to top.
		m.groupSort = (m.groupSort + 1) % sortModeCount
		m.applyGroupFilter()
		m.groupCursor = 0
		m.status = "sort: " + m.groupSort.sortLabel(true)
		return m, m.refreshSelectedGroupDetail()
	}
	return m, nil
}

// reloadGroupsCmd kicks off a fresh phase-1 + phase-2 groups load, bumping the
// generation and clearing stale lag values so the UI shows lag:? until the
// new fetch lands.
func (m *Model) reloadGroupsCmd() tea.Cmd {
	if m.selectedGroup != "" {
		delete(m.groupDetails, m.selectedGroup)
	}
	m.groupLoadGen++
	m.groupsLoading = true
	for i := range m.groups {
		m.groups[i].TotalLag = -1
	}
	return tea.Batch(m.loadGroupsCmd(), m.refreshSelectedGroupDetail())
}

// renderGroupListWindowed renders only the visible slice of groups around the
// cursor as one line per entry. filtered is the slice of indices into groups
// that satisfy the current fuzzy filter; pass an identity slice for "no
// filter active". lags maps group name to TotalLag from the describe cache;
// "lag:?" rendered when entry missing. Shares windowedList with the topics
// pane → identical N/M counter behaviour.
func renderGroupListWindowed(groups []kafka.GroupInfo, filtered []int, cursor, visibleEntries, nameW int, lags map[string]int64) string {
	if len(groups) == 0 {
		return "  (no consumer groups)\n"
	}
	if len(filtered) == 0 {
		return "  (no groups match)\n"
	}
	if nameW < 8 {
		nameW = 8
	}
	return windowedList(len(filtered), cursor, visibleEntries, func(i int) string {
		g := groups[filtered[i]]
		marker := "  "
		if i == cursor {
			marker = "> "
		}
		// Prefer the describe-cache TotalLag (freshest if the user navigated
		// here); fall back to the bulk-loaded GroupInfo.TotalLag; "?" when both
		// are unknown.
		var lag string
		if v, ok := lags[g.Name]; ok {
			lag = shortNum(v)
		} else {
			lag = shortNum(g.TotalLag)
		}
		return fmt.Sprintf("%s%s  %s  m:%d  lag:%s\n",
			marker, truncate(g.Name, nameW), shortState(g.State), g.Members, lag)
	})
}

// shortState abbreviates group state to a single letter so the single-line
// list stays compact: S=Stable, E=Empty, P=PreparingRebalance, R=Rebalancing /
// CompletingRebalance, D=Dead, ?=unknown.
func shortState(s string) string {
	switch s {
	case "Stable":
		return "S"
	case "Empty":
		return "E"
	case "Dead":
		return "D"
	case "PreparingRebalance":
		return "P"
	case "CompletingRebalance", "Rebalancing":
		return "R"
	case "":
		return "?"
	default:
		if len(s) > 0 {
			return s[:1]
		}
		return "?"
	}
}

func renderGroupDetail(d *kafka.GroupDetail) string {
	var b strings.Builder
	fmt.Fprintf(&b, "State:     %s\n", d.State)
	fmt.Fprintf(&b, "Total lag: %d\n\n", d.TotalLag)
	b.WriteString("Lag:\n")
	if len(d.Lag) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, r := range d.Lag {
		fmt.Fprintf(&b, "  %s[%d]  committed:%d  end:%d  lag:%d\n",
			r.Topic, r.Partition, r.Committed, r.End, r.Lag)
	}
	b.WriteString("\nMembers:\n")
	if len(d.Members) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, mem := range d.Members {
		fmt.Fprintf(&b, "  %s  client:%s  host:%s\n", mem.MemberID, mem.ClientID, mem.Host)
		if len(mem.Assignments) > 0 {
			fmt.Fprintf(&b, "    assigned: %s\n", strings.Join(mem.Assignments, ", "))
		}
	}
	return b.String()
}

// groupsView renders the 2-pane consumer-groups screen: list on the left,
// detail on the right (auto-syncs with the list cursor). Tab toggles focus
// between the panes; the focused pane gets the active border colour.
func (m Model) groupsView() string {
	if m.width < 60 || m.height < 12 {
		return m.narrowGroupsView()
	}
	leftW, rightW, msgsH, detailH := paneSizes(m.width, m.height)
	innerH := msgsH + detailH // share the full body height; same as topics pane

	listFocused := !m.groupDetailFocused
	listBody := groupsListBody(m, leftW-4)
	listContent := paneTitleWithSort("groups", listFocused, m.groupSort.sortLabel(true)) + "\n" + listBody
	listBox := paneStyle(listFocused).
		Width(leftW - 2).Height(innerH - 2).
		Render(listContent)

	detailBody := m.groupDetailBody(rightW - 4)
	detailContent := paneTitle("detail", m.groupDetailFocused) + "\n" + detailBody
	detailBox := paneStyle(m.groupDetailFocused).
		Width(rightW - 2).Height(innerH - 2).
		Render(detailContent)

	header := statusStyle.Render("consumer groups — " + m.cluster)
	body := lipgloss.JoinHorizontal(lipgloss.Top, listBox, detailBox)
	var footer string
	if m.searchingGroup {
		footer = m.groupSearchPanel()
	} else {
		hint := "↑/↓ pgup/pgdn home/end nav  •  tab/1/2 switch pane  •  / search  •  o sort  •  r reload  •  esc back  •  q quit"
		prefix := ""
		if m.groupSearch != "" {
			prefix = fmt.Sprintf("[search: %s  %d/%d] ", m.groupSearch, len(m.filteredGroups), len(m.groups))
		}
		footer = statusStyle.Render(prefix + hint)
	}
	return header + "\n" + body + "\n" + footer
}

func (m Model) narrowGroupsView() string {
	body := groupsListBody(m, m.width-4)
	if m.groupDetail != nil || m.groupDetailErr != "" || m.groupDetailLoading {
		body = m.groupDetailBody(m.width - 4)
	}
	box := paneStyle(true).Width(m.width - 2).Height(m.height - 4).Render(body)
	return box + "\n" + statusStyle.Render("[narrow] esc back  q quit")
}

func groupsListBody(m Model, innerW int) string {
	if m.groupsErr != "" && len(m.groups) == 0 {
		return "failed to list groups:\n" + m.groupsErr
	}
	if m.groups == nil {
		return "loading groups…"
	}
	// One row per group now (single-line entries). Available rows ≈ inner
	// height minus borders + header + footer + title + status lines.
	entries := m.height - 7
	if entries < 1 {
		entries = 1
	}
	// Reserve right side of the row for "  S  m:N  lag:N" (~18 chars + cursor
	// marker); truncate name to fit so long group names don't wrap.
	nameW := innerW - 22
	if nameW < 8 {
		nameW = 8
	}
	// Build lag map from describe cache; entries without a cached describe show
	// "lag:?" until the row gets focus and the detail loads.
	lags := make(map[string]int64, len(m.groupDetails))
	for name, d := range m.groupDetails {
		if d != nil {
			lags[name] = d.TotalLag
		}
	}
	out := renderGroupListWindowed(m.groups, m.filteredGroups, m.groupCursor, entries, nameW, lags)
	if m.groupsErr != "" {
		out += "\n⚠ " + m.groupsErr
	}
	if m.groupsLoading {
		out += "\nloading lag…"
	}
	return out
}

// groupSearchPanel mirrors topicSearchPanel: bottom-of-screen live input with
// matched/total counter.
func (m Model) groupSearchPanel() string {
	matched := len(m.filteredGroups)
	total := len(m.groups)
	prompt := lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("group search> ")
	cursor := lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("▎")
	counter := lipgloss.NewStyle().Faint(true).Render(fmt.Sprintf("  %d/%d", matched, total))
	help := statusStyle.Render("enter apply  •  esc cancel  •  empty input clears the search")
	return prompt + m.groupSearch + cursor + counter + "\n" + help
}

func (m Model) groupDetailBody(innerW int) string {
	if m.groupDetailErr != "" {
		return "failed to describe group:\n" + m.groupDetailErr
	}
	if m.groupDetailLoading || m.groupDetail == nil {
		if m.groupCursor < 0 || m.groupCursor >= len(m.groups) {
			return "(no group selected)"
		}
		return "loading group…"
	}
	return m.groupDetailVP.View()
}
