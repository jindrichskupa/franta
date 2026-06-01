package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
)

// loadTopicsCmd fetches the topic list (basic phase 1) off the UI goroutine.
// The caller is expected to bump m.topicLoadGen before invoking so stale
// in-flight phase-2 fetches can be discarded on arrival.
func (m Model) loadTopicsCmd() tea.Cmd {
	list := m.listTopics
	internal := m.showInternal
	gen := m.topicLoadGen
	return func() tea.Msg {
		ts, err := list(internal)
		return topicsLoadedMsg{topics: ts, gen: gen, err: err}
	}
}

// loadTopicOffsetsCmd fetches phase-2 message counts for the given topic
// names. Tagged with the current load generation so a reload mid-fetch
// supersedes the result.
func (m Model) loadTopicOffsetsCmd(names []string) tea.Cmd {
	fn := m.topicOffsets
	gen := m.topicLoadGen
	return func() tea.Msg {
		if fn == nil {
			return topicOffsetsMsg{gen: gen}
		}
		counts, err := fn(names)
		return topicOffsetsMsg{counts: counts, gen: gen, err: err}
	}
}

// renderTopicList renders a topic list with a cursor. Shared by the live topic
// view and the standalone startup picker.
func renderTopicList(topics []kafka.TopicInfo, cursor int) string {
	if len(topics) == 0 {
		return "  (no topics)\n"
	}
	var b strings.Builder
	for i, ti := range topics {
		marker := "  "
		if i == cursor {
			marker = "> "
		}
		fmt.Fprintf(&b, "%s%-40s parts:%-4d msgs:%d\n", marker, ti.Name, ti.Partitions, ti.Messages)
	}
	return b.String()
}
