package tui

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
	"franta/internal/record"
)

func recordWithOffset(off int64) record.Record { return record.Record{Offset: off} }

func TestPaneTopicsFocusAndSwitch(t *testing.T) {
	var switched string
	m := sized(New(nil))
	m.listTopics = func(internal bool) ([]kafka.TopicInfo, error) {
		return []kafka.TopicInfo{
			{Name: "a", Partitions: 1},
			{Name: "b", Partitions: 2},
		}, nil
	}
	m.switchTopic = func(name string) (int64, error) { switched = name; return 1, nil }
	// Init fires the initial topic load.
	nm, _ := m.Update(topicsLoadedMsg{topics: []kafka.TopicInfo{{Name: "a", Partitions: 1}, {Name: "b", Partitions: 2}}})
	m = nm.(Model)
	if len(m.filteredTopics) != 2 {
		t.Fatalf("filteredTopics = %d, want 2", len(m.filteredTopics))
	}
	// Focus pane 1
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'1'}})
	m = nm.(Model)
	if m.paneFocus != paneTopics {
		t.Fatal("paneFocus != paneTopics after '1'")
	}
	// Down + Enter -> switch to "b"
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m = nm.(Model)
	nm, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = nm.(Model)
	if cmd == nil {
		t.Fatal("expected switch command")
	}
	msg := cmd()
	sw, ok := msg.(switchedMsg)
	if !ok || sw.topic != "b" || switched != "b" {
		t.Fatalf("switch msg = %+v, switched=%q", msg, switched)
	}
}

func TestPaneTopicsFuzzySearch(t *testing.T) {
	m := sized(New(nil))
	m.topics = []kafka.TopicInfo{
		{Name: "orders"}, {Name: "shipments"}, {Name: "meta-orders"},
	}
	m.applyTopicFilter()
	// Focus pane 1, then '/'
	nm, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'1'}})
	m = nm.(Model)
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'/'}})
	m = nm.(Model)
	if !m.searchingTopic {
		t.Fatal("expected searchingTopic after '/'")
	}
	for _, r := range "ord" {
		nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = nm.(Model)
	}
	if len(m.filteredTopics) != 2 {
		t.Fatalf("filteredTopics = %d, want 2", len(m.filteredTopics))
	}
	if m.topics[m.filteredTopics[0]].Name != "orders" {
		t.Fatalf("first match = %q, want orders (prefix bonus)", m.topics[m.filteredTopics[0]].Name)
	}
	// esc clears
	nm, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = nm.(Model)
	if m.topicSearch != "" || len(m.filteredTopics) != 3 {
		t.Fatalf("esc did not clear: search=%q filtered=%d", m.topicSearch, len(m.filteredTopics))
	}
}

func TestSwitchedMsgClearsAndSetsTopic(t *testing.T) {
	m := sized(New(nil))
	nm, _ := m.Update(RecordMsg{Record: recordWithOffset(1), Gen: 0})
	m = nm.(Model)
	nm, _ = m.Update(switchedMsg{topic: "b", gen: 1})
	m = nm.(Model)
	if m.topic != "b" {
		t.Fatalf("topic = %q, want b", m.topic)
	}
	if m.mode != modeNormal {
		t.Fatal("expected modeNormal after switch")
	}
	if got := len(m.visible()); got != 0 {
		t.Fatalf("visible = %d, want 0 (cleared on new generation)", got)
	}
}

func TestTopicPickerSelects(t *testing.T) {
	p := newTopicPicker([]kafka.TopicInfo{{Name: "a"}, {Name: "b"}})
	np, _ := p.Update(tea.KeyMsg{Type: tea.KeyDown})
	p = np.(topicPicker)
	np, cmd := p.Update(tea.KeyMsg{Type: tea.KeyEnter})
	p = np.(topicPicker)
	if !p.done || p.Choice() != "b" {
		t.Fatalf("choice = %q done=%v, want b/true", p.Choice(), p.done)
	}
	if cmd == nil {
		t.Fatal("expected tea.Quit on selection")
	}
}

func TestTopicPickerCancel(t *testing.T) {
	p := newTopicPicker([]kafka.TopicInfo{{Name: "a"}})
	np, _ := p.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	p = np.(topicPicker)
	if p.Choice() != "" {
		t.Fatalf("choice = %q, want empty on cancel", p.Choice())
	}
}
