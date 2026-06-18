// Package tui implements franta's Bubble Tea terminal UI.
package tui

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/atotto/clipboard"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/jsontree"
	"franta/internal/kafka"
	"franta/internal/query"
	"franta/internal/record"
)

type viewMode int

const (
	modeNormal viewMode = iota
	modeProducer
	modeGroups
	modeExport
)

// sortMode is the cycling sort order for the topics and groups lists. The
// fuzzy filter still scores and reorders matches when active; the sortMode
// only kicks in when no fuzzy query is set, mirroring fzf-style UX.
type sortMode int

const (
	sortNameAsc   sortMode = iota
	sortCountDesc          // messages (topics) / lag (groups), desc
	sortAltDesc            // partitions (topics) / members (groups), desc
	sortModeCount
)

// sortLabel returns a short, direction-annotated label for the pane header,
// footer, and status line. ↑=asc, ↓=desc.
func (s sortMode) sortLabel(forGroups bool) string {
	switch s {
	case sortCountDesc:
		if forGroups {
			return "lag↓"
		}
		return "msgs↓"
	case sortAltDesc:
		if forGroups {
			return "members↓"
		}
		return "parts↓"
	default:
		return "name↑"
	}
}

// paneID identifies a focusable pane in modeNormal.
type paneID int

const (
	paneTopics paneID = iota + 1
	paneMessages
	paneDetail
)

// bufferCap is the ring-buffer capacity (records retained for display).
const bufferCap = 500

// ProduceFunc sends a record to Kafka. Provided by the caller (cmd wiring).
type ProduceFunc func(record.Record) error

// SeekFunc repositions consumption to a start spec and returns the seek
// generation now in effect. Provided by the caller.
type SeekFunc func(kafka.StartSpec) (int64, error)

// TopicsFunc lists the cluster's topics (basic info, no counts).
type TopicsFunc func(includeInternal bool) ([]kafka.TopicInfo, error)

// TopicOffsetsFunc fetches approximate message counts per topic. Optional
// second-phase fill called after TopicsFunc returns; nil disables counts.
type TopicOffsetsFunc func(names []string) (map[string]int64, error)

// SwitchFunc switches consumption to a topic and returns the new generation.
type SwitchFunc func(topic string) (int64, error)

// GroupsFunc lists consumer groups (basic info, no lag).
type GroupsFunc func() ([]kafka.GroupInfo, error)

// GroupLagsFunc fetches total lag per group name. Optional second-phase fill
// called after GroupsFunc returns; nil disables list-level lag.
type GroupLagsFunc func(names []string) (map[string]int64, error)

// DescribeGroupFunc describes one consumer group. Provided by the caller.
type DescribeGroupFunc func(name string) (kafka.GroupDetail, error)

// Model is the root Bubble Tea model.
type Model struct {
	buf  *record.Buffer
	pred query.Predicate

	mode      viewMode
	width     int
	height    int
	status    string
	errDialog string // non-empty → modal error overlay; dismissed by any key
	produce   ProduceFunc
	copyFn    CopyFunc

	table   table.Model
	detail  viewport.Model
	queryIn textinput.Model

	// JSON tree detail view. detailRaw (toggled by 'v', persists across
	// records) forces the raw viewport; otherwise a JSON value is shown as a
	// foldable tree. detailRecKey detects selection changes so the tree
	// rebuilds (cursor + fold reset) only when the record actually changes.
	detailRaw        bool
	detailTree       *jsontree.Node
	detailRows       []jsontree.Row
	detailTreeCursor int
	detailRecKey     string

	filtering bool // query bar focused

	// Saved filter state. savedFilters is the loaded list (config.yaml inline
	// + filters.yaml side-file, merged). filterPicker holds picker-modal state.
	savedFilters     []SavedFilter
	saveFilterFn     SaveFilterFunc
	deleteFilterFn   DeleteFilterFunc
	pickingFilter    bool
	filterPickCursor int
	// savingFilter is the in-progress "save filter as…" prompt that opens
	// from ctrl+s in the filter editor.
	savingFilter   bool
	saveFilterIn   textinput.Model
	pendingSaveQry string // captured at ctrl+s so refreshes don't drop it

	// Cluster picker (C).
	clusters          []string
	pickingCluster    bool
	clusterPickCursor int
	switchClusterFn   SwitchClusterFunc

	// live re-seek
	seek    SeekFunc
	seekIn  textinput.Model
	seeking bool
	curGen  int64 // highest seek generation seen; records below it are stale

	// producer form
	prodInputs       []textinput.Model // topic, key (single-line)
	prodHeadersTA    textarea.Model    // headers (multi-line: one k=v per row or JSON)
	prodValueTA      textarea.Model    // value (multi-line)
	prodFocus        int
	prodTemplateFrom string // non-empty source label when opened with template

	// topic list / switch
	listTopics    TopicsFunc
	topicOffsets  TopicOffsetsFunc
	switchTopic   SwitchFunc
	topics        []kafka.TopicInfo
	topicCursor   int
	showInternal  bool
	topicsErr     string
	topicLoadGen  int64    // bumped on every reload; stale phase-2 msgs dropped
	topicsLoading bool     // true while phase-1 (or phase-2) in flight
	topicSort     sortMode // cycles via 'o' in the topics pane
	cluster       string   // for the header
	topic         string   // current topic, for the header

	// 3-pane layout state
	paneFocus      paneID
	topicSearch    string // current fuzzy filter for the topics pane
	filteredTopics []int  // indices into m.topics after fuzzy filter, score-sorted
	searchingTopic bool   // true while the topic-search input is focused
	showHelp       bool   // help overlay toggled via '?'
	paused         bool   // when true, incoming records still buffer but the
	// table/detail don't redraw (frozen view)

	// consumer groups
	listGroupsFn       GroupsFunc
	groupLagsFn        GroupLagsFunc
	describeGroupFn    DescribeGroupFunc
	resetOffsetsFn     ResetOffsetsFunc
	deleteGroupFn      DeleteGroupFunc
	createTopicFn      CreateTopicFunc
	deleteTopicFn      DeleteTopicFunc
	addPartitionsFn    AddPartitionsFunc
	getTopicConfigFn   GetTopicConfigFunc
	setTopicConfigFn   SetTopicConfigFunc
	groupLoadGen       int64 // bumped on every reload; stale phase-2 msgs dropped
	groupsLoading      bool
	groupSort          sortMode
	groups             []kafka.GroupInfo
	groupCursor        int
	groupsErr          string
	groupDetail        *kafka.GroupDetail
	groupDetailErr     string
	groupDetailVP      viewport.Model
	selectedGroup      string
	groupDetails       map[string]*kafka.GroupDetail // cache: name -> detail
	groupDetailLoading bool
	// fuzzy search over groups (mirrors topicSearch / filteredTopics).
	groupSearch    string
	filteredGroups []int
	searchingGroup bool

	// groupsPane focus: false = list (left), true = detail (right). Tab toggles.
	groupDetailFocused bool

	// Generic confirm overlay (reused by group + topic destructive actions).
	confirmActive bool
	confirmPrompt string
	confirmExpect string          // "" → y/n mode; else type-to-confirm value
	confirmInput  textinput.Model // used only in type-to-confirm mode
	confirmAction func() tea.Cmd  // executed on confirm

	// Reset-offset dialog state (within modeGroups).
	resetActive   bool // target picker open
	resetCursor   int  // 0=begin 1=end 2=timestamp 3=explicit
	resetGroup    string
	resetTSActive bool // timestamp text input open
	resetTSInput  textinput.Model

	// Explicit per-partition offset editor (within modeGroups reset flow).
	explicitActive bool
	explicitRows   []explicitRow
	explicitCursor int
	explicitInput  textinput.Model

	// Export dialog state.
	exportPath       textinput.Model
	exportFmt        exportFormat
	exportN          int  // record count captured when the dialog opened
	exportPathEdited bool // user typed in the path field → stop auto-deriving it
	openFn           func(path string) (io.WriteCloser, error)

	// Add-partitions prompt.
	apActive  bool
	apInput   textinput.Model
	apTopic   string
	apCurrent int

	// New-topic dialog.
	ntActive    bool
	ntInputs    []textinput.Model // 0=name 1=partitions 2=replication
	ntConfigsTA textarea.Model
	ntFocus     int

	// Topic config editor.
	tcActive  bool
	tcTopic   string
	tcRows    []tcRow
	tcOrig    map[string]string // key → original value (changed-detection)
	tcCursor  int
	tcInput   textinput.Model
	tcLoading bool
}

// New builds a Model. produce may be nil (producing disabled).
func New(produce ProduceFunc) Model {
	t := table.New(
		table.WithColumns([]table.Column{
			{Title: "PART", Width: 5},
			{Title: "OFFSET", Width: 10},
			{Title: "KEY", Width: 24},
			{Title: "VALUE", Width: 50},
		}),
		table.WithFocused(true),
	)

	qi := textinput.New()
	qi.Prompt = "filter> "
	qi.Placeholder = `e.g. partition == 0 and value.type == "order"`

	si := textinput.New()
	si.Prompt = "seek> "
	si.Placeholder = "beginning | end | last:500 | 1h | 2026-05-27T00:00:00Z"

	matchAll, _ := query.Parse("")

	prodInputs, prodHeadersTA, prodValueTA := initProducerFields()

	model := Model{
		buf:           record.NewBuffer(bufferCap),
		pred:          matchAll,
		mode:          modeNormal,
		paneFocus:     paneMessages,
		produce:       produce,
		table:         t,
		queryIn:       qi,
		seekIn:        si,
		prodInputs:    prodInputs,
		prodHeadersTA: prodHeadersTA,
		prodValueTA:   prodValueTA,
	}
	// Default clipboard writer; Run() overrides only when a Copy callback is set.
	model.copyFn = clipboard.WriteAll
	// Default file opener for the export dialog; tests inject their own.
	model.openFn = func(p string) (io.WriteCloser, error) { return os.Create(p) }
	return model
}

// Init satisfies tea.Model. Auto-loads the topic list when a TopicsFunc is
// available so the topics pane has content from the start.
func (m Model) Init() tea.Cmd {
	if m.listTopics == nil {
		return nil
	}
	// Init runs on a value receiver, so bumping m.topicLoadGen here would be
	// lost. The model passed to tea.NewProgram is what Update sees; bumping
	// in Run() before launching keeps generations consistent. Start at gen=1.
	return m.loadTopicsCmd()
}

// visible returns buffered records matching the current predicate, newest first.
func (m Model) visible() []record.Record {
	all := m.buf.Records()
	out := make([]record.Record, 0, len(all))
	for i := len(all) - 1; i >= 0; i-- {
		if m.pred(all[i]) {
			out = append(out, all[i])
		}
	}
	return out
}

// selectedRecord returns the record under the messages-table cursor within the
// current visible (filtered) set, and false if there is none.
func (m Model) selectedRecord() (record.Record, bool) {
	vis := m.visible()
	idx := m.table.Cursor()
	if idx < 0 || idx >= len(vis) {
		return record.Record{}, false
	}
	return vis[idx], true
}

// refreshTable rebuilds table rows from the visible records.
func (m *Model) refreshTable() {
	vis := m.visible()
	rows := make([]table.Row, len(vis))
	for i, r := range vis {
		rows[i] = table.Row{
			itoa32(r.Partition),
			itoa64(r.Offset),
			truncate(string(r.Key), 24),
			truncate(oneLine(r.ValueDisplay()), 50),
		}
	}
	m.table.SetRows(rows)
	// bubbles/table.SetRows clamps the cursor to -1 when transitioning from an
	// empty table; without this nudge, the next refreshDetail sees idx<0 and
	// blanks the detail pane even though records are present.
	if len(rows) > 0 && m.table.Cursor() < 0 {
		m.table.SetCursor(0)
	}
}

// Update satisfies tea.Model.
func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		_, rightW, msgsH, detailH := paneSizes(msg.Width, msg.Height)
		// inner sizes deduct the border (2 cells).
		m.table.SetWidth(rightW - 2)
		// -3 = border (2) + title row (1)
		m.table.SetHeight(msgsH - 3)
		m.detail = viewport.New(rightW-2, detailH-3)
		// Groups detail viewport shares the right column of the groups screen.
		m.groupDetailVP = viewport.New(rightW-4, msgsH+detailH-4)
		if m.groupDetail != nil {
			m.groupDetailVP.SetContent(wrapForVP(renderGroupDetail(m.groupDetail), m.groupDetailVP.Width))
		}
		m.refreshDetail()
		return m, nil

	case RecordMsg:
		if msg.Gen < m.curGen {
			return m, nil // record from a superseded position
		}
		if msg.Gen > m.curGen {
			// First record of a new generation: drop the prior position's view.
			m.curGen = msg.Gen
			m.buf = record.NewBuffer(bufferCap)
		}
		m.buf.Add(msg.Record)
		// Skip redraw while paused so the user can read the current snapshot;
		// records still accumulate in the buffer and appear on resume.
		if !m.paused {
			m.refreshTable()
			m.refreshDetail()
		}
		return m, nil

	case errMsg:
		if msg.err != nil {
			m.errDialog = "Error\n\n" + msg.err.Error()
		}
		return m, nil

	case clusterSwitchedMsg:
		if msg.err != nil {
			m.errDialog = "Switch cluster failed\n\n" + msg.err.Error()
			return m, nil
		}
		m.cluster = msg.cluster
		m.curGen = msg.gen
		m.topicLoadGen++
		m.groupLoadGen++
		m.buf = record.NewBuffer(bufferCap)
		m.topics = nil
		m.groups = nil
		m.groupDetails = nil
		m.pred, _ = query.Parse("") // match-all; m.pred is called unguarded in visible(), never set it nil
		m.queryIn.SetValue("")
		m.topic = ""
		m.topicCursor = 0
		m.mode = modeNormal
		m.paneFocus = paneTopics
		m.topicsLoading = true
		m.refreshTable()
		m.refreshDetail()
		m.status = "switched to " + msg.cluster
		return m, m.loadTopicsCmd()

	case producedMsg:
		if msg.err != nil {
			m.errDialog = "Produce failed\n\n" + msg.err.Error()
		} else {
			m.status = "produced"
			m.mode = modeNormal
		}
		return m, nil

	case exportDoneMsg:
		if msg.err != nil {
			m.errDialog = "Export failed\n\n" + msg.err.Error()
		} else {
			m.status = fmt.Sprintf("exported %d → %s", msg.n, msg.path)
			m.mode = modeNormal
		}
		return m, nil

	case seekDoneMsg:
		if msg.err != nil {
			m.errDialog = "Seek failed\n\n" + msg.err.Error()
			return m, nil
		}
		// Clear only if no record of this generation has arrived yet (strict >),
		// so a freshly-sought record that raced ahead of this message survives.
		if msg.gen > m.curGen {
			m.curGen = msg.gen
			m.buf = record.NewBuffer(bufferCap)
			m.refreshTable()
			m.refreshDetail()
		}
		m.status = "seeked"
		return m, nil

	case topicsLoadedMsg:
		// Drop messages from a superseded load (user hit 'r' or 'i' since).
		if msg.gen != m.topicLoadGen {
			return m, nil
		}
		if msg.err != nil {
			m.topicsErr = msg.err.Error()
		} else {
			m.topicsErr = ""
		}
		// Always populate topics when present so a partial-warning load
		// (topics OK, some per-partition offsets failed) still updates the UI.
		if msg.topics != nil {
			m.topics = msg.topics
			m.applyTopicFilter()
		}
		// Phase 2: fire the counts fetch if a callback is wired and we have
		// any topics to count.
		if m.topicOffsets != nil && len(m.topics) > 0 {
			names := make([]string, len(m.topics))
			for i, t := range m.topics {
				names[i] = t.Name
			}
			return m, m.loadTopicOffsetsCmd(names)
		}
		m.topicsLoading = false
		return m, nil

	case topicOffsetsMsg:
		if msg.gen != m.topicLoadGen {
			return m, nil
		}
		m.topicsLoading = false
		if msg.err != nil {
			m.topicsErr = msg.err.Error()
		}
		for i := range m.topics {
			if c, ok := msg.counts[m.topics[i].Name]; ok {
				m.topics[i].Messages = c
			}
		}
		return m, nil

	case switchedMsg:
		if msg.err != nil {
			m.errDialog = "Topic switch failed\n\n" + msg.err.Error()
			return m, nil
		}
		m.topic = msg.topic
		m.mode = modeNormal
		if msg.gen > m.curGen {
			m.curGen = msg.gen
			m.buf = record.NewBuffer(bufferCap)
			m.refreshTable()
			m.refreshDetail()
		}
		m.status = "switched to " + msg.topic
		return m, nil

	case groupsLoadedMsg:
		if msg.gen != m.groupLoadGen {
			return m, nil
		}
		if msg.err != nil {
			m.groupsErr = msg.err.Error()
		} else {
			m.groupsErr = ""
		}
		if msg.groups != nil {
			m.groups = msg.groups
		}
		m.applyGroupFilter()
		if m.groupCursor >= len(m.filteredGroups) {
			m.groupCursor = 0
		}
		// Auto-load detail for the highlighted group.
		cmd := m.refreshSelectedGroupDetail()
		// Phase 2: kick off batch-lag fetch if the callback is wired.
		var lagsCmd tea.Cmd
		if m.groupLagsFn != nil && len(m.groups) > 0 {
			names := make([]string, len(m.groups))
			for i, g := range m.groups {
				names[i] = g.Name
			}
			lagsCmd = m.loadGroupLagsCmd(names)
		} else {
			m.groupsLoading = false
		}
		return m, tea.Batch(cmd, lagsCmd)

	case groupLagsMsg:
		if msg.gen != m.groupLoadGen {
			return m, nil
		}
		m.groupsLoading = false
		if msg.err != nil {
			m.groupsErr = msg.err.Error()
		}
		for i := range m.groups {
			if v, ok := msg.lags[m.groups[i].Name]; ok {
				m.groups[i].TotalLag = v
			}
		}
		return m, nil

	case groupDetailMsg:
		m.groupDetailLoading = false
		if msg.err != nil {
			m.groupDetailErr = msg.err.Error()
			return m, nil
		}
		m.groupDetailErr = ""
		m.groupDetail = msg.detail
		if m.groupDetails == nil {
			m.groupDetails = make(map[string]*kafka.GroupDetail)
		}
		if msg.detail != nil {
			m.groupDetails[msg.detail.Name] = msg.detail
			// Only update viewport if this detail matches the active selection
			// (cursor may have moved while we were fetching).
			if msg.detail.Name == m.selectedGroup {
				m.groupDetailVP.SetContent(wrapForVP(renderGroupDetail(msg.detail), m.groupDetailVP.Width))
				m.groupDetailVP.GotoTop()
			}
		}
		return m, nil

	case groupMutatedMsg:
		if msg.err != nil {
			m.errDialog = fmt.Sprintf("%s group failed\n\n%s", capitalize(msg.action), msg.err.Error())
			return m, nil
		}
		m.status = fmt.Sprintf("%s group %s", msg.action, msg.group)
		// Refresh the groups list + detail after a successful mutation.
		return m, m.reloadGroupsCmd()

	case topicMutatedMsg:
		if msg.err != nil {
			m.errDialog = fmt.Sprintf("%s topic failed\n\n%s", capitalize(msg.action), msg.err.Error())
			return m, nil
		}
		m.status = fmt.Sprintf("%s topic %s", msg.action, msg.topic)
		return m, m.reloadTopicsCmd()

	case topicConfigLoadedMsg:
		if msg.topic != m.tcTopic || !m.tcActive {
			return m, nil // superseded
		}
		m.tcLoading = false
		if msg.err != nil {
			m.tcActive = false
			m.errDialog = "Load config failed\n\n" + msg.err.Error()
			return m, nil
		}
		m.tcRows = msg.rows
		m.tcOrig = msg.orig
		m.tcCursor = 0
		in := textinput.New()
		in.Prompt = ""
		m.tcInput = in
		m.tcLoadRowToInput()
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Save-filter prompt has priority over the picker (it can only open from
	// inside the filter editor, after ctrl+s).
	if m.savingFilter {
		return m.updateSaveFilterPrompt(msg)
	}
	if m.pickingFilter {
		return m.updateFilterPicker(msg)
	}
	if m.pickingCluster {
		return m.updateClusterPicker(msg)
	}
	// Error dialog is the top-most overlay — eats everything except quit and
	// dismiss, so a failure can never be missed by drifting off the footer.
	if m.errDialog != "" {
		switch msg.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit
		case tea.KeyEsc, tea.KeyEnter, tea.KeySpace:
			m.errDialog = ""
			return m, nil
		}
		// Any other key dismisses too — be forgiving so a user mashing keys
		// gets back to work fast.
		m.errDialog = ""
		return m, nil
	}

	if m.confirmActive {
		return m.updateConfirm(msg)
	}

	if m.ntActive {
		return m.updateCreateTopic(msg)
	}
	if m.apActive {
		return m.updateAddPartitions(msg)
	}
	if m.tcActive {
		return m.updateTopicConfig(msg)
	}

	// Help overlay swallows everything except its own close keys.
	if m.showHelp {
		switch {
		case msg.Type == tea.KeyCtrlC:
			return m, tea.Quit
		case msg.Type == tea.KeyEsc, msg.String() == "?", msg.String() == "q":
			m.showHelp = false
			return m, nil
		}
		return m, nil
	}

	// Mode-specific modal screens take input before the global pane keys.
	switch m.mode {
	case modeProducer:
		return m.updateProducer(msg)
	case modeGroups:
		return m.updateGroups(msg)
	case modeExport:
		return m.updateExport(msg)
	}

	// modeNormal — text-input subscreens first (they swallow most keys).
	if m.filtering {
		return m.updateFilterPrompt(msg)
	}
	if m.seeking {
		return m.updateSeekPrompt(msg)
	}
	if m.searchingTopic {
		return m.updateTopicSearchPrompt(msg)
	}

	// Global quit + focus + modal openers (only while no prompt is active).
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyCtrlY:
		if r, ok := m.selectedRecord(); ok {
			b, err := record.JSON(r)
			if err != nil {
				m.errDialog = "clipboard: " + err.Error()
			} else if err := m.copyFn(string(b)); err != nil {
				m.errDialog = "clipboard: " + err.Error()
			} else {
				m.status = "copied record"
			}
		} else {
			m.status = "nothing to copy"
		}
		return m, nil
	case tea.KeyTab:
		m.paneFocus = nextPane(m.paneFocus, +1)
		return m, nil
	case tea.KeyShiftTab:
		m.paneFocus = nextPane(m.paneFocus, -1)
		return m, nil
	}
	switch msg.String() {
	case "q":
		return m, tea.Quit
	case "?":
		m.showHelp = !m.showHelp
		return m, nil
	case " ":
		m.paused = !m.paused
		if !m.paused {
			// Resume: redraw with all records buffered while paused.
			m.refreshTable()
			m.refreshDetail()
		}
		return m, nil
	case "1", "t":
		m.paneFocus = paneTopics
		return m, nil
	case "2":
		m.paneFocus = paneMessages
		return m, nil
	case "3":
		m.paneFocus = paneDetail
		return m, nil
	case "s":
		m.seeking = true
		m.seekIn.Focus()
		return m, nil
	case "f":
		// Global "open DSL filter" — works from any pane so the user doesn't
		// have to remember that '/' in the topics pane means topic search,
		// while '/' in the messages pane means filter.
		m.filtering = true
		m.queryIn.Focus()
		return m, nil
	case "F":
		// Recall a saved filter — picker modal of names.
		if len(m.savedFilters) == 0 {
			m.status = "no saved filters (ctrl+s in the filter editor to save one)"
			return m, nil
		}
		m.pickingFilter = true
		m.filterPickCursor = 0
		return m, nil
	case "e":
		m = m.openExport()
		return m, nil
	case "p":
		if m.produce != nil {
			m = m.openProducer()
		}
		return m, nil
	case "P":
		// Produce with template: prefill from the currently-selected record
		// in the messages buffer (whatever pane is focused).
		if m.produce != nil {
			if r, ok := m.selectedRecord(); ok {
				m = m.openProducerTemplate(r)
			} else {
				m = m.openProducer()
			}
		}
		return m, nil
	case "y":
		if r, ok := m.selectedRecord(); ok {
			if err := m.copyFn(r.KeyDisplay()); err != nil {
				m.errDialog = "clipboard: " + err.Error()
			} else {
				m.status = "copied key"
			}
		} else {
			m.status = "nothing to copy"
		}
		return m, nil
	case "Y":
		if r, ok := m.selectedRecord(); ok {
			if err := m.copyFn(r.ValueDisplay()); err != nil {
				m.errDialog = "clipboard: " + err.Error()
			} else {
				m.status = "copied value"
			}
		} else {
			m.status = "nothing to copy"
		}
		return m, nil
	case "g":
		if m.listGroupsFn != nil {
			m.mode = modeGroups
			m.groups = nil
			m.groupsErr = ""
			m.groupCursor = 0
			m.groupLoadGen++
			m.groupsLoading = true
			return m, m.loadGroupsCmd()
		}
		return m, nil
	case "C":
		m = m.openClusterPicker()
		return m, nil
	}

	// Pane-specific keys.
	switch m.paneFocus {
	case paneTopics:
		return m.updateTopicsPane(msg)
	case paneDetail:
		return m.updateDetailPane(msg)
	default:
		return m.updateMessagesPane(msg)
	}
}

// nextPane returns the pane reached by stepping `dir` panes from cur
// (dir == +1 forward, -1 backward), wrapping at the ends.
func nextPane(cur paneID, dir int) paneID {
	n := int(cur) + dir
	switch {
	case n < int(paneTopics):
		return paneDetail
	case n > int(paneDetail):
		return paneTopics
	}
	return paneID(n)
}

// updateFilterPrompt handles keys while the DSL filter input is focused.
func (m Model) updateFilterPrompt(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyCtrlS:
		// "Save as…" — open the save-filter prompt with the current query
		// captured. The editor stays open behind it so the user can come back.
		raw := strings.TrimSpace(m.queryIn.Value())
		if raw == "" {
			m.status = "nothing to save (filter is empty)"
			return m, nil
		}
		m.pendingSaveQry = raw
		m.savingFilter = true
		ti := textinput.New()
		ti.Prompt = "save filter as: "
		ti.Placeholder = "errors"
		ti.Focus()
		m.saveFilterIn = ti
		return m, nil
	case tea.KeyEsc:
		m.filtering = false
		m.queryIn.Blur()
		return m, nil
	case tea.KeyEnter:
		raw := strings.TrimSpace(m.queryIn.Value())
		pred, err := query.Parse(raw)
		if err != nil {
			m.status = "filter error: " + err.Error()
			return m, nil
		}
		m.pred = pred
		m.filtering = false
		m.queryIn.Blur()
		m.refreshTable()
		m.refreshDetail()
		// Confirm the apply so the user sees something happened even when the
		// predicate yields zero matches (the table empties otherwise without
		// any visible feedback).
		n := len(m.visible())
		if raw == "" {
			m.status = fmt.Sprintf("filter cleared (%d records)", n)
		} else {
			m.status = fmt.Sprintf("filter applied (%d matches)", n)
		}
		return m, nil
	}
	var cmd tea.Cmd
	m.queryIn, cmd = m.queryIn.Update(msg)
	return m, cmd
}

// updateSeekPrompt handles keys while the seek input is focused.
func (m Model) updateSeekPrompt(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.seeking = false
		m.seekIn.Blur()
		return m, nil
	case tea.KeyEnter:
		spec, err := kafka.ParseStart(m.seekIn.Value())
		if err != nil {
			m.status = "seek error: " + err.Error()
			return m, nil
		}
		m.seeking = false
		m.seekIn.Blur()
		seek := m.seek
		return m, func() tea.Msg {
			if seek == nil {
				return seekDoneMsg{}
			}
			gen, err := seek(spec)
			return seekDoneMsg{gen: gen, err: err}
		}
	}
	var cmd tea.Cmd
	m.seekIn, cmd = m.seekIn.Update(msg)
	return m, cmd
}

// updateGroupSearchPrompt handles keys while the group-fuzzy-search input is
// focused. Mirrors updateTopicSearchPrompt.
func (m Model) updateGroupSearchPrompt(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.searchingGroup = false
		m.groupSearch = ""
		m.applyGroupFilter()
		return m, m.refreshSelectedGroupDetail()
	case tea.KeyEnter:
		m.searchingGroup = false
		return m, nil
	case tea.KeyBackspace:
		if n := len(m.groupSearch); n > 0 {
			m.groupSearch = m.groupSearch[:n-1]
			m.applyGroupFilter()
		}
		return m, m.refreshSelectedGroupDetail()
	case tea.KeyRunes:
		m.groupSearch += string(msg.Runes)
		m.applyGroupFilter()
		return m, m.refreshSelectedGroupDetail()
	}
	return m, nil
}

// updateTopicSearchPrompt handles keys while the topic-fuzzy-search input is
// focused. Typing rebuilds the filtered topic list in place.
func (m Model) updateTopicSearchPrompt(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.searchingTopic = false
		m.topicSearch = ""
		m.applyTopicFilter()
		return m, nil
	case tea.KeyEnter:
		m.searchingTopic = false
		return m, nil
	case tea.KeyBackspace:
		if n := len(m.topicSearch); n > 0 {
			m.topicSearch = m.topicSearch[:n-1]
			m.applyTopicFilter()
		}
		return m, nil
	case tea.KeyRunes:
		m.topicSearch += string(msg.Runes)
		m.applyTopicFilter()
		return m, nil
	}
	return m, nil
}

// updateTopicsPane handles keys while the topics pane has focus.
func (m Model) updateTopicsPane(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyUp:
		if m.topicCursor > 0 {
			m.topicCursor--
		}
		return m, nil
	case tea.KeyDown:
		if m.topicCursor < len(m.filteredTopics)-1 {
			m.topicCursor++
		}
		return m, nil
	case tea.KeyPgUp:
		m.topicCursor -= 10
		if m.topicCursor < 0 {
			m.topicCursor = 0
		}
		return m, nil
	case tea.KeyPgDown:
		m.topicCursor += 10
		if m.topicCursor >= len(m.filteredTopics) {
			m.topicCursor = len(m.filteredTopics) - 1
		}
		if m.topicCursor < 0 {
			m.topicCursor = 0
		}
		return m, nil
	case tea.KeyHome:
		m.topicCursor = 0
		return m, nil
	case tea.KeyEnd:
		if n := len(m.filteredTopics); n > 0 {
			m.topicCursor = n - 1
		}
		return m, nil
	case tea.KeyEnter:
		idx := m.topicCursor
		if idx < 0 || idx >= len(m.filteredTopics) {
			return m, nil
		}
		name := m.topics[m.filteredTopics[idx]].Name
		sw := m.switchTopic
		return m, func() tea.Msg {
			if sw == nil {
				return switchedMsg{topic: name}
			}
			gen, err := sw(name)
			return switchedMsg{topic: name, gen: gen, err: err}
		}
	}
	switch msg.String() {
	case "/":
		m.searchingTopic = true
		return m, nil
	case "n":
		return m.beginCreateTopic()
	case "D":
		return m.beginDeleteTopic()
	case "a":
		return m.beginAddPartitions()
	case "c":
		return m.beginTopicConfig()
	case "i":
		if m.listTopics == nil {
			return m, nil
		}
		m.showInternal = !m.showInternal
		m.topicCursor = 0
		m.topicLoadGen++
		m.topicsLoading = true
		return m, m.loadTopicsCmd()
	case "r":
		if m.listTopics == nil {
			return m, nil
		}
		m.topicLoadGen++
		m.topicsLoading = true
		// Mark all counts unknown so the user sees "loading counts…" semantics
		// (m:?) until phase 2 lands.
		for i := range m.topics {
			m.topics[i].Messages = -1
		}
		return m, m.loadTopicsCmd()
	case "o":
		// Cycle sort: name → msgs↓ → parts↓ → name. Re-apply filter to
		// resort. Cursor goes to the top so the user sees the new first row.
		m.topicSort = (m.topicSort + 1) % sortModeCount
		m.applyTopicFilter()
		m.topicCursor = 0
		m.status = "sort: " + m.topicSort.sortLabel(false)
		return m, nil
	}
	return m, nil
}

// updateMessagesPane handles keys while the messages pane has focus.
func (m Model) updateMessagesPane(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "/" {
		m.filtering = true
		m.queryIn.Focus()
		return m, nil
	}
	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	m.refreshDetail()
	return m, cmd
}

// updateDetailPane handles keys while the detail pane has focus.
func (m Model) updateDetailPane(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// 'v' toggles raw/tree in either sub-mode.
	if msg.String() == "v" {
		return m.toggleDetailRaw(), nil
	}
	// Tree sub-mode: node navigation + folding.
	if !m.detailRaw && m.detailTree != nil {
		return m.updateDetailTree(msg), nil
	}
	// Raw sub-mode: viewport scroll (unchanged).
	var cmd tea.Cmd
	m.detail, cmd = m.detail.Update(msg)
	return m, cmd
}

// applyGroupFilter rebuilds m.filteredGroups from m.groups using m.groupSearch.
// Mirrors applyTopicFilter (same fuzzy scoring + tie-break) so the UX is
// consistent between the topics and groups panes. With no fuzzy query the
// list is ordered by m.groupSort; with a query the score order wins (the
// user is hunting a specific match).
func (m *Model) applyGroupFilter() {
	if m.groupSearch == "" {
		m.filteredGroups = make([]int, len(m.groups))
		for i := range m.groups {
			m.filteredGroups[i] = i
		}
		sort.SliceStable(m.filteredGroups, func(i, j int) bool {
			a, b := m.groups[m.filteredGroups[i]], m.groups[m.filteredGroups[j]]
			switch m.groupSort {
			case sortCountDesc:
				if a.TotalLag != b.TotalLag {
					return a.TotalLag > b.TotalLag
				}
			case sortAltDesc:
				if a.Members != b.Members {
					return a.Members > b.Members
				}
			}
			return a.Name < b.Name
		})
	} else {
		type entry struct{ idx, score int }
		var matches []entry
		for i, g := range m.groups {
			if s, ok := fuzzyMatch(m.groupSearch, g.Name); ok {
				matches = append(matches, entry{i, s})
			}
		}
		sort.Slice(matches, func(i, j int) bool {
			if matches[i].score != matches[j].score {
				return matches[i].score > matches[j].score
			}
			return m.groups[matches[i].idx].Name < m.groups[matches[j].idx].Name
		})
		m.filteredGroups = make([]int, len(matches))
		for k, e := range matches {
			m.filteredGroups[k] = e.idx
		}
	}
	if m.groupCursor < 0 || m.groupCursor >= len(m.filteredGroups) {
		m.groupCursor = 0
	}
}

// applyTopicFilter rebuilds m.filteredTopics from m.topics using m.topicSearch.
// Empty search → identity slice. Non-empty → fuzzy-matched entries sorted by
// score desc (ties broken by name asc). Clamps the cursor.
func (m *Model) applyTopicFilter() {
	if m.topicSearch == "" {
		m.filteredTopics = make([]int, len(m.topics))
		for i := range m.topics {
			m.filteredTopics[i] = i
		}
		sort.SliceStable(m.filteredTopics, func(i, j int) bool {
			a, b := m.topics[m.filteredTopics[i]], m.topics[m.filteredTopics[j]]
			switch m.topicSort {
			case sortCountDesc:
				if a.Messages != b.Messages {
					return a.Messages > b.Messages
				}
			case sortAltDesc:
				if a.Partitions != b.Partitions {
					return a.Partitions > b.Partitions
				}
			}
			return a.Name < b.Name
		})
	} else {
		type entry struct {
			idx, score int
		}
		var matches []entry
		for i, t := range m.topics {
			if s, ok := fuzzyMatch(m.topicSearch, t.Name); ok {
				matches = append(matches, entry{i, s})
			}
		}
		sort.Slice(matches, func(i, j int) bool {
			if matches[i].score != matches[j].score {
				return matches[i].score > matches[j].score
			}
			return m.topics[matches[i].idx].Name < m.topics[matches[j].idx].Name
		})
		m.filteredTopics = make([]int, len(matches))
		for k, e := range matches {
			m.filteredTopics[k] = e.idx
		}
	}
	if m.topicCursor < 0 || m.topicCursor >= len(m.filteredTopics) {
		m.topicCursor = 0
	}
}

// refreshDetail rebuilds the detail viewport content from the currently
// selected visible record. Resets the viewport scroll position to the top so
// the beginning of value/JSON is visible after switching selection.
func (m *Model) refreshDetail() {
	vis := m.visible()
	idx := m.table.Cursor()
	if idx < 0 || idx >= len(vis) {
		m.detail.SetContent("")
		m.detail.GotoTop()
		m.detailTree = nil
		m.detailRows = nil
		m.detailRecKey = ""
		return
	}
	r := vis[idx]
	key := recKey(r)
	changed := key != m.detailRecKey
	m.detailRecKey = key

	// Raw viewport is always kept current so 'v' can switch to it instantly.
	m.detail.SetContent(detailContent(r))
	if changed {
		m.detail.GotoTop()
	}

	// Tree: rebuild (resetting cursor + fold state) only when the selection
	// changed, so streamed records for the same selection don't disturb folds.
	// A non-JSON value leaves detailTree nil → the view falls back to raw.
	if m.detailRaw {
		m.detailTree = nil
		m.detailRows = nil
		return
	}
	if changed {
		if root, err := jsontree.Parse([]byte(r.ValueDisplay())); err == nil {
			m.detailTree = root
			m.detailRows = root.Rows()
			m.detailTreeCursor = 0
		} else {
			m.detailTree = nil
			m.detailRows = nil
		}
	}
}
