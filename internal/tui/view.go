package tui

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"franta/internal/query"
	"franta/internal/record"
)

var (
	statusStyle = lipgloss.NewStyle().Faint(true)
	inactiveBdr = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("241"))
	activeBdr   = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(lipgloss.Color("205"))
)

// View satisfies tea.Model.
func (m Model) View() string {
	if m.errDialog != "" {
		return m.errorDialogView()
	}
	if m.pickingFilter {
		return m.filterPickerView()
	}
	if m.showHelp {
		return m.helpView()
	}
	switch m.mode {
	case modeProducer:
		return m.producerView()
	case modeGroups:
		return m.groupsView()
	}
	return m.normalView()
}

// errorDialogView renders a centered red-bordered modal that holds focus until
// dismissed. Used for fatal-ish errors (seek/switch/produce/error msgs) so
// they cannot be missed by a flicker on the footer.
func (m Model) errorDialogView() string {
	w := m.width * 60 / 100
	if w < 50 {
		w = 50
	}
	if w > 100 {
		w = 100
	}
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("196"))
	hint := statusStyle.Render("press enter / esc / space / any key to dismiss")
	body := titleStyle.Render("⚠ " + firstLine(m.errDialog))
	if rest := afterFirstLine(m.errDialog); rest != "" {
		body += "\n\n" + rest
	}
	dialog := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("196")).
		Padding(1, 2).
		Width(w).
		Render(body + "\n\n" + hint)
	if m.width > 0 && m.height > 0 {
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, dialog)
	}
	return dialog
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func afterFirstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimLeft(s[i+1:], "\n")
	}
	return ""
}

const helpText = `franta — keys

  1 / 2 / 3 / t       focus topics / messages / detail (t = topics)
  tab / shift+tab     cycle focus between panes
  ↑ ↓ pgup pgdn       navigate or scroll the focused pane
  home / end          first / last entry
  enter               topics pane: switch to highlighted topic
  /                   topics: fuzzy topic search
                      messages: DSL filter (see below)
  f                   open DSL filter from any pane (alias for messages /)
  F                   recall a saved filter (picker; d deletes)
  ctrl+s              in filter editor: save the current query under a name
  i / r               topics: toggle internal / reload
  s                   seek prompt (modal): end | beginning | last:N | 1h | RFC3339
  p                   produce form (modal): topic, key, headers (k=v,k=v), value (textarea)
                      ctrl+s sends; enter inserts a newline while in value
  P                   produce with TEMPLATE: prefill from the currently
                      selected record (topic/key/headers/value)
  g                   consumer groups (modal): list left, lag+members right;
                      cursor auto-syncs detail; r refresh; esc back
  space               pause / resume tailing (records keep buffering)
  esc                 cancel a prompt or close a modal
  ?                   toggle this help
  q / ctrl+c          quit

DSL filter examples (focus messages pane → press /):

  partition == 0
  offset >= 1000
  key contains "user-"
  key matches "^id-[0-9]+$"
  value.payload.status == "ACTIVE"
  value.amount >= 100
  header['source'] == "web"
  timestamp >= "2026-05-27T00:00:00Z"
  not (partition == 3) and value.ok == "true"

Fields: key, value, value.<json.path>, partition, offset, timestamp, header['name'].
Ops:    == != < > <= >= contains matches    and / or / not / ()
Strings need quotes (""), numbers don't. Booleans compare as "true"/"false".

Press ? or esc to close.`

// helpView renders the help overlay shown when m.showHelp is true.
func (m Model) helpView() string {
	style := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("205")).
		Padding(0, 1)
	return style.Render(helpText) + "\n" + statusStyle.Render("? or esc to close")
}

// normalView renders the 3-pane layout for modeNormal. It falls back to a
// single focused pane when the terminal is too narrow or short for the full
// layout.
func (m Model) normalView() string {
	const minW, minH = 60, 12
	if m.width < minW || m.height < minH {
		return m.narrowOrShortView()
	}
	leftW, rightW, msgsH, detailH := paneSizes(m.width, m.height)

	topic := m.topic
	if topic == "" {
		topic = "(no topic — pick one in the topics pane)"
	}
	headerLine := fmt.Sprintf("%s / %s", m.cluster, topic)
	if g := adaGreeting(time.Now()); g != "" {
		headerLine += "   " + g
	}
	header := statusStyle.Render(headerLine)
	footer := statusStyle.Render(m.normalFooter())

	// lipgloss Width/Height set the INNER content box; the border adds 2 cells
	// outside on each axis. Subtract 2 so the rendered pane occupies exactly
	// the requested outer cells (leftW, msgsH, etc.).
	topicsContent := paneTitleWithSort("topics", m.paneFocus == paneTopics, m.topicSort.sortLabel(false)) + "\n" +
		m.topicsPaneContent(leftW-2)
	msgsContent := paneTitle("messages", m.paneFocus == paneMessages) + "\n" +
		m.table.View()
	detailContentStr := paneTitle("detail", m.paneFocus == paneDetail) + "\n" +
		m.detail.View()

	topicsBox := paneStyle(m.paneFocus == paneTopics).
		Width(leftW - 2).Height(msgsH + detailH - 2).
		Render(topicsContent)
	msgsBox := paneStyle(m.paneFocus == paneMessages).
		Width(rightW - 2).Height(msgsH - 2).
		Render(msgsContent)
	detailBox := paneStyle(m.paneFocus == paneDetail).
		Width(rightW - 2).Height(detailH - 2).
		Render(detailContentStr)

	right := lipgloss.JoinVertical(lipgloss.Left, msgsBox, detailBox)
	body := lipgloss.JoinHorizontal(lipgloss.Top, topicsBox, right)
	return header + "\n" + body + "\n" + footer
}

// narrowOrShortView is the fallback for tiny terminals — render just the
// focused pane (or the messages pane by default), with a tag in the footer.
func (m Model) narrowOrShortView() string {
	tag := "[short]"
	if m.width < 60 {
		tag = "[narrow]"
	}
	var body string
	switch m.paneFocus {
	case paneTopics:
		body = m.topicsPaneContent(m.width - 2)
	case paneDetail:
		body = m.detail.View()
	default:
		body = m.table.View()
	}
	return paneStyle(true).Width(m.width-2).Height(m.height-4).Render(body) + "\n" + statusStyle.Render(tag+" "+m.normalFooter())
}

// paneSizes computes per-pane dimensions from outer terminal width/height.
// Returns left-pane width, right-column width, messages-pane height, and
// detail-pane height (all in cells, including border).
func paneSizes(w, h int) (leftW, rightW, msgsH, detailH int) {
	// Reserve 3 cells: header + footer + 1 safety row (some terminals reserve
	// a status line, and lipgloss output occasionally drops a trailing line).
	innerH := h - 3
	if innerH < 6 {
		innerH = 6
	}
	leftW = w * 28 / 100
	if leftW < 18 {
		leftW = 18
	}
	rightW = w - leftW
	msgsH = innerH * 55 / 100
	detailH = innerH - msgsH
	return
}

func paneStyle(active bool) lipgloss.Style {
	if active {
		return activeBdr
	}
	return inactiveBdr
}

// paneTitle returns a styled, bold title line for a pane. The active pane gets
// a foreground colour matching the active border.
func paneTitle(name string, active bool) string {
	s := lipgloss.NewStyle().Bold(true)
	if active {
		s = s.Foreground(lipgloss.Color("205"))
	} else {
		s = s.Foreground(lipgloss.Color("244"))
	}
	return s.Render(name)
}

// paneTitleWithSort renders a pane title plus a faint "[sort: <label>]" tag
// after it so the current sort order is always visible.
func paneTitleWithSort(name string, active bool, sortLabel string) string {
	if sortLabel == "" {
		return paneTitle(name, active)
	}
	tag := lipgloss.NewStyle().Faint(true).Render("  [sort: " + sortLabel + "]")
	return paneTitle(name, active) + tag
}

// topicsPaneContent renders the topics list (filtered + scrolled) into a
// string sized to fit the topics pane's inner width.
func (m Model) topicsPaneContent(innerW int) string {
	// Full failure: no topics at all + an error.
	if m.topicsErr != "" && len(m.topics) == 0 {
		return "failed to list topics:\n" + m.topicsErr
	}
	if m.topics == nil {
		return "loading topics…"
	}
	if len(m.filteredTopics) == 0 {
		return "(no topics match)"
	}
	// Two visual markers distinguish "cursor" vs "currently consuming":
	//   "> "  cursor (where enter would switch)
	//   "● "  the topic the consumer is currently reading from
	// Both can be the same row, in which case the cursor marker wins.
	activeStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("82")).Bold(true) // green = live
	cursorStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true)
	current := m.topic

	entries := m.height - 7 // border (2) + header + footer + title + search line
	out := windowedList(len(m.filteredTopics), m.topicCursor, entries, func(i int) string {
		ti := m.topics[m.filteredTopics[i]]
		isCursor := i == m.topicCursor
		isActive := current != "" && ti.Name == current
		var marker string
		switch {
		case isCursor:
			marker = "> "
		case isActive:
			marker = "● "
		default:
			marker = "  "
		}
		// Reserve right side for counts; truncate name to fit.
		nameW := innerW - 18
		if nameW < 6 {
			nameW = 6
		}
		line := fmt.Sprintf("%s%s p:%d m:%s", marker, truncate(ti.Name, nameW), ti.Partitions, shortNum(ti.Messages))
		switch {
		case isCursor:
			return cursorStyle.Render(line) + "\n"
		case isActive:
			return activeStyle.Render(line) + "\n"
		default:
			return line + "\n"
		}
	})
	if m.topicsErr != "" {
		out += "\n⚠ " + m.topicsErr
	}
	if m.topicsLoading {
		out += "\nloading counts…"
	}
	return out
}

// windowedList renders a scrollable list with a cursor and an N/M position
// counter when the view is truncated. Shared between the topics and groups
// panes so they look and behave identically.
//
//   - total: number of items in the underlying (possibly filtered) slice
//   - cursor: index of the highlighted item in that slice
//   - entries: how many list ENTRIES fit in the pane (a multi-line entry counts as one)
//   - renderEntry(i): the rendered string for entry i (already includes its own
//     "> " / "  " cursor marker and trailing newline)
func windowedList(total, cursor, entries int, renderEntry func(i int) string) string {
	if entries < 1 {
		entries = 1
	}
	first, last := windowRange(cursor, entries, total)
	var b strings.Builder
	for i := first; i < last; i++ {
		b.WriteString(renderEntry(i))
	}
	if first > 0 || last < total {
		fmt.Fprintf(&b, "\n%d/%d", cursor+1, total)
	}
	return b.String()
}

func windowRange(cursor, rows, total int) (first, last int) {
	first = cursor - rows/2
	if first < 0 {
		first = 0
	}
	last = first + rows
	if last > total {
		last = total
		first = last - rows
		if first < 0 {
			first = 0
		}
	}
	return
}

// normalFooter builds the footer string for modeNormal. Renders the
// active text-input prompt when one is open, else per-pane key hints
// (with the transient status prefixed when present).
func (m Model) normalFooter() string {
	switch {
	case m.savingFilter:
		return m.saveFilterPromptView()
	case m.filtering:
		return m.filterPanel()
	case m.seeking:
		return m.seekIn.View()
	case m.searchingTopic:
		return m.topicSearchPanel()
	}
	var hint string
	switch m.paneFocus {
	case paneTopics:
		hint = "↑/↓ nav  enter switch  / search  o sort  i internal  r reload  •  1/2/3 t tab focus  •  space pause  •  s p P g  •  ? help  q quit"
	case paneDetail:
		hint = "↑/↓/pgup/pgdn scroll  •  1/2/3 t tab focus  •  P produce-template  •  space pause  •  s p g  •  ? help  q quit"
	default:
		hint = "↑/↓ nav  / f filter (e.g. header['src'] == \"web\")  •  P produce-template  •  1/2/3 t tab focus  •  space pause  •  s p g  •  ? help  q quit"
	}
	prefix := ""
	if m.paused {
		prefix = "[PAUSED] "
	}
	if m.topicSearch != "" {
		prefix += fmt.Sprintf("[search: %s  %d/%d] ", m.topicSearch, len(m.filteredTopics), len(m.topics))
	}
	if m.status != "" {
		prefix += "[" + m.status + "] "
	}
	return prefix + hint
}

// topicSearchPanel renders the live topic fuzzy-search input at the bottom of
// the screen with a "N/total" counter, mirroring filterPanel's UX.
func (m Model) topicSearchPanel() string {
	matched := len(m.filteredTopics)
	total := len(m.topics)
	prompt := lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("topic search> ")
	cursor := lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render("▎")
	counter := lipgloss.NewStyle().Faint(true).Render(fmt.Sprintf("  %d/%d", matched, total))
	help := statusStyle.Render("enter apply  •  esc cancel  •  empty input clears the search")
	return prompt + m.topicSearch + cursor + counter + "\n" + help
}

// filterPanel renders the multi-line filter editor: the live input, a live
// parse status, and a compact hints block (fields, ops, examples).
func (m Model) filterPanel() string {
	raw := strings.TrimSpace(m.queryIn.Value())
	parseStatus := lipgloss.NewStyle().Foreground(lipgloss.Color("82")).Render("✓ ok")
	if raw != "" {
		if _, err := query.Parse(raw); err != nil {
			parseStatus = lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Render("✗ " + err.Error())
		}
	}
	hint := statusStyle.Render(
		"fields: key, value, value.<json.path>, partition, offset, timestamp, header['name']\n" +
			"ops:    == != < > <= >= contains matches    and / or / not / ()\n" +
			"ex:     header['x-trace-id'] == \"abc\"   |   value.amount >= 100   |   key contains \"user-\"")
	help := statusStyle.Render("enter apply  •  ctrl+s save as…  •  esc cancel  •  empty input clears the filter")
	return m.queryIn.View() + "  " + parseStatus + "\n" + hint + "\n" + help
}

func detailContent(r record.Record) string {
	var b strings.Builder
	fmt.Fprintf(&b, "topic:     %s\n", r.Topic)
	fmt.Fprintf(&b, "partition: %d\n", r.Partition)
	fmt.Fprintf(&b, "offset:    %d\n", r.Offset)
	fmt.Fprintf(&b, "timestamp: %s\n", r.Timestamp.Format("2006-01-02T15:04:05Z07:00"))
	fmt.Fprintf(&b, "key:       %s\n", r.KeyDisplay())
	// Always render the headers section so the reader can confirm "no headers"
	// vs "section missing"; populated rows indented under the label.
	if len(r.Headers) == 0 {
		b.WriteString("headers:   (none)\n")
	} else {
		b.WriteString("headers:\n")
		for _, h := range r.Headers {
			fmt.Fprintf(&b, "  %s = %s\n", h.Key, string(h.Value))
		}
	}
	b.WriteString("\nvalue:\n")
	b.WriteString(r.ValueDisplay())
	return b.String()
}

func itoa32(v int32) string { return strconv.FormatInt(int64(v), 10) }
func itoa64(v int64) string { return strconv.FormatInt(v, 10) }

func oneLine(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", "")
}

// shortNum renders a non-negative count with a k/M/G suffix once the value
// crosses ~10k, so the topics + groups single-line entries fit inside narrow
// panes without wrapping. Negative inputs return "?" (caller convention).
func shortNum(n int64) string {
	if n < 0 {
		return "?"
	}
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fG", float64(n)/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 10_000:
		return fmt.Sprintf("%.1fk", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}

// truncate shortens s to at most max runes, appending an ellipsis when cut.
func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	if max <= 1 {
		return string(r[:max])
	}
	return string(r[:max-1]) + "…"
}
