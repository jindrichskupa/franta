package tui

import (
	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
)

type topicPicker struct {
	topics  []kafka.TopicInfo
	cursor  int
	choice  string
	done    bool
	cluster string
}

func newTopicPicker(topics []kafka.TopicInfo) topicPicker {
	return topicPicker{topics: topics}
}

func (p topicPicker) Choice() string { return p.choice }

func (p topicPicker) Init() tea.Cmd { return nil }

func (p topicPicker) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
			if p.cursor < len(p.topics)-1 {
				p.cursor++
			}
		case tea.KeyEnter:
			if len(p.topics) > 0 {
				p.choice = p.topics[p.cursor].Name
			}
			p.done = true
			return p, tea.Quit
		}
	}
	return p, nil
}

func (p topicPicker) View() string {
	header := "Select a topic:\n\n"
	if p.cluster != "" {
		header = "Cluster " + p.cluster + " — select a topic:\n\n"
	}
	return header + renderTopicList(p.topics, p.cursor) + "\n↑/↓: move  enter: select  esc: cancel"
}

// PickTopic runs an interactive topic picker and returns the chosen topic name,
// or "" if cancelled.
func PickTopic(cluster string, topics []kafka.TopicInfo) (string, error) {
	p := newTopicPicker(topics)
	p.cluster = cluster
	final, err := tea.NewProgram(p).Run()
	if err != nil {
		return "", err
	}
	return final.(topicPicker).Choice(), nil
}
