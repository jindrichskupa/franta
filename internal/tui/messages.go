package tui

import (
	"franta/internal/kafka"
	"franta/internal/record"
)

// RecordMsg delivers a newly consumed record to the UI, tagged with the seek
// generation it was read under (see Model.curGen).
type RecordMsg struct {
	Record record.Record
	Gen    int64
}

// errMsg reports a background error to display in the status bar.
type errMsg struct{ err error }

// producedMsg reports the result of a produce attempt.
type producedMsg struct{ err error }

// seekDoneMsg reports the result of a live re-seek, carrying the generation the
// consumer advanced to on success.
type seekDoneMsg struct {
	gen int64
	err error
}

// topicsLoadedMsg delivers the result of phase 1 (basic) topic list load. The
// counts arrive separately in topicOffsetsMsg.
type topicsLoadedMsg struct {
	topics []kafka.TopicInfo
	gen    int64 // load generation; stale messages dropped by Update
	err    error
}

// topicOffsetsMsg delivers phase 2 (counts) for the current topic list. Carries
// the generation the fetch was started under so a reload that fires while the
// previous fetch is in flight discards the stale result.
type topicOffsetsMsg struct {
	counts map[string]int64
	gen    int64
	err    error
}

// switchedMsg reports the result of a live topic switch.
type switchedMsg struct {
	topic string
	gen   int64
	err   error
}

// groupsLoadedMsg delivers phase 1 (basic) consumer-group list load. Lag
// arrives separately in groupLagsMsg.
type groupsLoadedMsg struct {
	groups []kafka.GroupInfo
	gen    int64
	err    error
}

// groupLagsMsg delivers phase 2 (total lag per group) for the current group
// list, generation-tagged like topicOffsetsMsg.
type groupLagsMsg struct {
	lags map[string]int64
	gen  int64
	err  error
}

// groupDetailMsg delivers the result of describing one group.
type groupDetailMsg struct {
	detail *kafka.GroupDetail
	err    error
}
