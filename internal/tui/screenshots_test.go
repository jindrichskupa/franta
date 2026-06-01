//go:build screenshots

// Run: go test -tags screenshots -run TestGenerateScreenshots ./internal/tui/
//
// Renders Model.View() with realistic sample state into docs/screenshots/*.txt
// so README.md can embed monospace previews of every screen.
package tui

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
	"franta/internal/record"
)

const (
	shotW = 120
	shotH = 32
)

// ansi strips SGR / cursor / OSC sequences so the dumps render plain on
// GitHub. Bubble Tea emits a lot of escapes per frame; this matches the
// "ESC + non-letter chars + a final letter" form used by all of them.
var ansi = regexp.MustCompile(`\x1b\[[0-9;?]*[A-Za-z]|\x1b\][^\x07]*\x07`)

func strip(s string) string { return ansi.ReplaceAllString(s, "") }

func writeShot(t *testing.T, name, body string) {
	t.Helper()
	dir, _ := filepath.Abs("../../docs/screenshots")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(strip(body)+"\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	t.Logf("wrote %s", path)
}

// shotModel builds a model in modeNormal sized to shotW × shotH with a fake
// cluster + topic, a populated topics list, and a few sample records so all
// three panes have something to display.
func shotModel() Model {
	m := New(nil)
	m.cluster = "prod-msk"
	m.topic = "orders"
	m, _ = updateAs(m, tea.WindowSizeMsg{Width: shotW, Height: shotH})

	m.topics = []kafka.TopicInfo{
		{Name: "orders", Partitions: 12, Messages: 1840372},
		{Name: "payments", Partitions: 6, Messages: 92410},
		{Name: "shipments", Partitions: 3, Messages: 12044},
		{Name: "audit-log", Partitions: 24, Messages: 9_812_004},
		{Name: "users", Partitions: 1, Messages: 4203},
		{Name: "search-events", Partitions: 8, Messages: 305_811},
		{Name: "click-stream", Partitions: 16, Messages: 24_001_555},
		{Name: "metrics-raw", Partitions: 32, Messages: 88_011_240},
	}
	m.applyTopicFilter()
	m.topicCursor = 0

	now := time.Date(2026, 6, 1, 9, 14, 0, 0, time.UTC)
	// Insert oldest first so visible() (which reverses) lands the newest one
	// (with headers + a real JSON payload) at the top — that's the row the
	// table cursor lands on, and the row whose detail the right pane shows.
	recs := []record.Record{
		{
			Topic: "orders", Partition: 4, Offset: 14_829_104,
			Timestamp: now.Add(-22 * time.Second),
			Key:       []byte("ORD-44EE-009"),
			Value:     []byte(`{"order_id":"ORD-44EE-009","customer":"u4","amount":7.50,"items":1,"status":"PAID"}`),
		},
		{
			Topic: "orders", Partition: 0, Offset: 14_829_110,
			Timestamp: now.Add(-12 * time.Second),
			Key:       []byte("ORD-12CD-002"),
			Value:     []byte(`{"order_id":"ORD-12CD-002","customer":"u17","amount":1850.00,"items":12,"status":"PAID"}`),
		},
		{
			Topic: "orders", Partition: 1, Offset: 14_829_112,
			Timestamp: now.Add(-7 * time.Second),
			Key:       []byte("ORD-7B22-007"),
			Value:     []byte(`{"order_id":"ORD-7B22-007","customer":"u51","amount":42.00,"items":1,"status":"PENDING"}`),
		},
		{
			Topic: "orders", Partition: 3, Offset: 14_829_113,
			Timestamp: now.Add(-3 * time.Second),
			Key:       []byte("ORD-9F1A-001"),
			Headers:   []record.Header{{Key: "source", Value: []byte("web")}, {Key: "trace-id", Value: []byte("abc123")}},
			Value:     []byte(`{"order_id":"ORD-9F1A-001","customer":"u42","amount":129.95,"items":3,"status":"PAID"}`),
		},
	}
	for _, r := range recs {
		var nm tea.Model
		nm, _ = m.Update(RecordMsg{Record: r})
		m = nm.(Model)
	}
	m.refreshTable()
	m.refreshDetail()
	return m
}

func updateAs(m Model, msg tea.Msg) (Model, tea.Cmd) {
	nm, cmd := m.Update(msg)
	return nm.(Model), cmd
}

func TestGenerateScreenshots(t *testing.T) {
	// 1. Normal 3-pane view (default landing screen)
	m := shotModel()
	writeShot(t, "01-normal.txt", m.View())

	// 2. Topics pane focused with active fuzzy search "audi"
	m2 := shotModel()
	m2.paneFocus = paneTopics
	m2.topicSearch = "audi"
	m2.applyTopicFilter()
	writeShot(t, "02-topic-search.txt", m2.View())

	// 3. Filter prompt panel (DSL filter editor open with a sample query).
	m3 := shotModel()
	m3.filtering = true
	m3.queryIn.SetValue(`header['source'] == "web" and value.amount >= 100`)
	writeShot(t, "03-filter.txt", m3.View())

	// 4. Producer dialog with prefilled fields (P from template).
	m4 := shotModel()
	m4 = m4.openProducerTemplate(record.Record{
		Topic: "orders", Partition: 3, Offset: 14_829_113,
		Key:     []byte("ORD-9F1A-001"),
		Headers: []record.Header{{Key: "source", Value: []byte("web")}, {Key: "trace-id", Value: []byte("abc123")}},
		Value:   []byte(`{"order_id":"ORD-9F1A-001","customer":"u42","amount":129.95,"items":3,"status":"PAID"}`),
	})
	writeShot(t, "04-producer.txt", m4.View())

	// 5. Consumer groups view, populated, list focused.
	m5 := shotModel()
	m5.mode = modeGroups
	m5.groups = []kafka.GroupInfo{
		{Name: "orders-processor", State: "Stable", Members: 6, TotalLag: 0},
		{Name: "fraud-detector", State: "Stable", Members: 3, TotalLag: 142},
		{Name: "shipments-router", State: "Stable", Members: 2, TotalLag: 17},
		{Name: "analytics-writer", State: "Stable", Members: 12, TotalLag: 9_241_006},
		{Name: "compactor-v2", State: "Empty", Members: 0, TotalLag: -1},
		{Name: "metrics-uploader", State: "Stable", Members: 4, TotalLag: 0},
		{Name: "legacy-replay", State: "Dead", Members: 0, TotalLag: -1},
	}
	m5.applyGroupFilter()
	m5.groupDetail = &kafka.GroupDetail{
		Name: "fraud-detector", State: "Stable", TotalLag: 142,
		Lag: []kafka.GroupLagRow{
			{Topic: "orders", Partition: 0, Committed: 14_829_101, End: 14_829_113, Lag: 12},
			{Topic: "orders", Partition: 1, Committed: 14_829_080, End: 14_829_112, Lag: 32},
			{Topic: "orders", Partition: 2, Committed: 14_829_007, End: 14_829_105, Lag: 98},
		},
		Members: []kafka.GroupMember{
			{MemberID: "fd-1-7af2", ClientID: "fd-1", Host: "/10.0.4.12", Assignments: []string{"orders:0", "orders:1"}},
			{MemberID: "fd-2-9bb1", ClientID: "fd-2", Host: "/10.0.4.18", Assignments: []string{"orders:2"}},
		},
	}
	m5.selectedGroup = "fraud-detector"
	m5.groupDetails = map[string]*kafka.GroupDetail{"fraud-detector": m5.groupDetail}
	m5.groupCursor = 1
	m5.groupDetailVP.SetContent(wrapForVP(renderGroupDetail(m5.groupDetail), m5.groupDetailVP.Width))
	writeShot(t, "05-groups.txt", m5.View())
}
