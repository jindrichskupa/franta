package tui

import (
	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
)

// Callbacks bundles the actions the TUI invokes back into the kafka layer.
// Any field may be nil to disable the corresponding feature.
type Callbacks struct {
	Produce       ProduceFunc
	Seek          SeekFunc
	ListTopics    TopicsFunc       // phase 1: names + partitions (fast)
	TopicOffsets  TopicOffsetsFunc // phase 2: per-topic message counts (slow); optional
	Switch        SwitchFunc
	Groups        GroupsFunc    // phase 1: names + state + members (fast)
	GroupLags     GroupLagsFunc // phase 2: per-group total lag (slow); optional
	DescribeGroup DescribeGroupFunc
	ResetOffsets  ResetOffsetsFunc // nil → R disabled
	DeleteGroup   DeleteGroupFunc  // nil → D disabled

	CreateTopic    CreateTopicFunc    // nil → n disabled
	DeleteTopic    DeleteTopicFunc    // nil → D disabled
	AddPartitions  AddPartitionsFunc  // nil → a disabled
	GetTopicConfig GetTopicConfigFunc // nil → c disabled
	SetTopicConfig SetTopicConfigFunc

	Cluster string // shown in the header
	Topic   string // shown in the header (initial topic)

	// Saved DSL filters: names + queries the user can recall with F. The save
	// callback persists a new entry to the side-file (filters.yaml).
	SavedFilters []SavedFilter
	SaveFilter   SaveFilterFunc
	DeleteFilter DeleteFilterFunc

	// Copy writes text to the clipboard.
	// Optional; overrides the default (clipboard.WriteAll) wired in New.
	Copy CopyFunc
}

// ResetOffsetsFunc resets a consumer group's committed offsets.
type ResetOffsetsFunc func(group string, spec kafka.ResetSpec) error

// DeleteGroupFunc deletes a consumer group.
type DeleteGroupFunc func(group string) error

// CreateTopicFunc creates a topic.
type CreateTopicFunc func(name string, partitions int32, rf int16, configs map[string]string) error

// DeleteTopicFunc deletes a topic.
type DeleteTopicFunc func(name string) error

// AddPartitionsFunc raises a topic's partition count to total.
type AddPartitionsFunc func(name string, total int) error

// GetTopicConfigFunc fetches a topic's effective configs.
type GetTopicConfigFunc func(name string) ([]kafka.TopicConfigEntry, error)

// SetTopicConfigFunc incrementally sets topic config keys.
type SetTopicConfigFunc func(name string, set map[string]string) error

// SavedFilter mirrors config.SavedFilter without coupling tui to the config
// package (keeps tui tests dependency-free).
type SavedFilter struct {
	Name  string
	Query string
}

// SaveFilterFunc persists a named filter; called from the TUI on ctrl+s in
// the filter prompt.
type SaveFilterFunc func(name, query string) error

// DeleteFilterFunc drops a named filter from the side-file.
type DeleteFilterFunc func(name string) error

// CopyFunc writes text to the system clipboard.
type CopyFunc func(text string) error

// Run launches the TUI. Fetched records arriving on records are streamed into
// the UI and errors on errs are shown in the status bar. errs may be nil. Run
// blocks until the user quits.
func Run(records <-chan kafka.Fetched, errs <-chan error, cb Callbacks) error {
	m := New(cb.Produce)
	m.seek = cb.Seek
	m.listTopics = cb.ListTopics
	m.topicOffsets = cb.TopicOffsets
	m.switchTopic = cb.Switch
	m.listGroupsFn = cb.Groups
	m.groupLagsFn = cb.GroupLags
	m.describeGroupFn = cb.DescribeGroup
	m.resetOffsetsFn = cb.ResetOffsets
	m.deleteGroupFn = cb.DeleteGroup
	m.createTopicFn = cb.CreateTopic
	m.deleteTopicFn = cb.DeleteTopic
	m.addPartitionsFn = cb.AddPartitions
	m.getTopicConfigFn = cb.GetTopicConfig
	m.setTopicConfigFn = cb.SetTopicConfig
	m.savedFilters = cb.SavedFilters
	m.saveFilterFn = cb.SaveFilter
	m.deleteFilterFn = cb.DeleteFilter
	m.cluster = cb.Cluster
	m.topic = cb.Topic
	if cb.Copy != nil {
		m.copyFn = cb.Copy
	}
	// When the caller starts without a topic, focus the topics pane so the
	// user immediately sees the picker.
	if cb.Topic == "" {
		m.paneFocus = paneTopics
	}
	p := tea.NewProgram(m, tea.WithAltScreen())
	go func() {
		for f := range records {
			p.Send(RecordMsg{Record: f.Record, Gen: f.Gen})
		}
	}()
	if errs != nil {
		go func() {
			for e := range errs {
				p.Send(errMsg{err: e})
			}
		}()
	}
	_, err := p.Run()
	return err
}
