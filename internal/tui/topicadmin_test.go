package tui

import (
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
)

func topicsModel(names ...string) Model {
	m := New(nil)
	m.width, m.height = 120, 40
	ts := make([]kafka.TopicInfo, len(names))
	for i, n := range names {
		ts[i] = kafka.TopicInfo{Name: n, Partitions: 2, Messages: 0}
	}
	m.topics = ts
	m.applyTopicFilter()
	m.topicCursor = 0
	m.paneFocus = paneTopics
	return m
}

// --- Task 3: delete ---

func TestDeleteTopicInternalGuard(t *testing.T) {
	m := topicsModel("__consumer_offsets")
	m.deleteTopicFn = func(string) error { return nil }
	nm, _ := m.beginDeleteTopic()
	m2 := nm
	if m2.confirmActive {
		t.Fatal("internal topic delete should be blocked")
	}
	if !strings.Contains(m2.errDialog, "internal") {
		t.Fatalf("errDialog %q", m2.errDialog)
	}
}

func TestDeleteTopicConfirmRuns(t *testing.T) {
	deleted := ""
	m := topicsModel("orders")
	m.deleteTopicFn = func(n string) error { deleted = n; return nil }
	nm, _ := m.beginDeleteTopic()
	m = nm
	if !m.confirmActive {
		t.Fatal("expected confirm")
	}
	m.confirmInput.SetValue("orders")
	_, cmd := m.updateConfirm(keyEnter())
	msg := cmd().(topicMutatedMsg)
	if msg.err != nil || deleted != "orders" {
		t.Fatalf("delete not run: %v %q", msg.err, deleted)
	}
}

func TestTopicMutatedError(t *testing.T) {
	m := topicsModel("x")
	nm, _ := m.Update(topicMutatedMsg{action: "created", topic: "x", err: errors.New("nope")})
	if !strings.Contains(nm.(Model).errDialog, "nope") {
		t.Fatalf("errDialog %q", nm.(Model).errDialog)
	}
}

// --- Task 4: add partitions ---

func TestAddPartitionsRejectsNotGreater(t *testing.T) {
	m := topicsModel("orders") // 2 partitions
	m.addPartitionsFn = func(string, int) error { return nil }
	nm, _ := m.beginAddPartitions()
	m = nm
	if !m.apActive {
		t.Fatal("prompt should open")
	}
	m.apInput.SetValue("2") // not greater than current 2
	nm2, _ := m.updateAddPartitions(keyEnter())
	if !strings.Contains(nm2.(Model).status, "greater than 2") {
		t.Fatalf("status %q", nm2.(Model).status)
	}
}

func TestAddPartitionsConfirms(t *testing.T) {
	var gotName string
	var gotTotal int
	m := topicsModel("orders")
	m.addPartitionsFn = func(n string, total int) error { gotName = n; gotTotal = total; return nil }
	nm, _ := m.beginAddPartitions()
	m = nm
	m.apInput.SetValue("5")
	nm2, _ := m.updateAddPartitions(keyEnter())
	m = nm2.(Model)
	if !m.confirmActive {
		t.Fatal("should ask confirm")
	}
	_, cmd := m.updateConfirm(keyRunes("y"))
	cmd()
	if gotName != "orders" || gotTotal != 5 {
		t.Fatalf("add partitions: %q %d", gotName, gotTotal)
	}
}

// --- Task 5: create topic ---

func TestCreateTopicValidation(t *testing.T) {
	m := topicsModel()
	m.createTopicFn = func(string, int32, int16, map[string]string) error { return nil }
	nm, _ := m.beginCreateTopic()
	m = nm
	m.ntInputs[ntFieldName].SetValue("bad name!") // space + !
	nm2, _ := m.updateCreateTopic(tea.KeyMsg{Type: tea.KeyCtrlS})
	if !strings.Contains(nm2.(Model).status, "invalid topic name") {
		t.Fatalf("status %q", nm2.(Model).status)
	}
}

func TestCreateTopicSubmits(t *testing.T) {
	var gotName string
	var gotParts int32
	var gotCfg map[string]string
	m := topicsModel()
	m.createTopicFn = func(n string, p int32, rf int16, cfg map[string]string) error {
		gotName, gotParts, gotCfg = n, p, cfg
		return nil
	}
	nm, _ := m.beginCreateTopic()
	m = nm
	m.ntInputs[ntFieldName].SetValue("orders-v2")
	m.ntInputs[ntFieldParts].SetValue("6")
	m.ntInputs[ntFieldRF].SetValue("1")
	m.ntConfigsTA.SetValue("cleanup.policy=compact")
	_, cmd := m.updateCreateTopic(tea.KeyMsg{Type: tea.KeyCtrlS})
	msg := cmd().(topicMutatedMsg)
	if msg.err != nil || gotName != "orders-v2" || gotParts != 6 || gotCfg["cleanup.policy"] != "compact" {
		t.Fatalf("create: %q %d %+v err=%v", gotName, gotParts, gotCfg, msg.err)
	}
}

// --- Task 6: config view/edit ---

func TestTopicConfigLoadsAndEdits(t *testing.T) {
	m := topicsModel("orders")
	m.getTopicConfigFn = func(string) ([]kafka.TopicConfigEntry, error) {
		return []kafka.TopicConfigEntry{
			{Key: "cleanup.policy", Value: "delete", Editable: true},
			{Key: "segment.bytes", Value: "1073741824", Editable: false},
		}, nil
	}
	var gotSet map[string]string
	m.setTopicConfigFn = func(_ string, set map[string]string) error { gotSet = set; return nil }

	nm, cmd := m.beginTopicConfig()
	m = nm
	if !m.tcActive || !m.tcLoading {
		t.Fatal("expected loading config dialog")
	}
	// Deliver the load.
	loaded := cmd().(topicConfigLoadedMsg)
	nm2, _ := m.Update(loaded)
	m = nm2.(Model)
	if m.tcLoading || len(m.tcRows) != 2 {
		t.Fatalf("rows not loaded: %+v", m.tcRows)
	}
	// Edit the first (editable) row.
	m.tcInput.SetValue("compact")
	nm3, _ := m.updateTopicConfig(tea.KeyMsg{Type: tea.KeyCtrlS})
	m = nm3.(Model)
	if !m.confirmActive {
		t.Fatal("should confirm config change")
	}
	_, c2 := m.updateConfirm(keyRunes("y"))
	c2()
	if gotSet["cleanup.policy"] != "compact" {
		t.Fatalf("set: %+v", gotSet)
	}
	if _, ok := gotSet["segment.bytes"]; ok {
		t.Fatal("read-only key must not be sent")
	}
}

func TestTopicConfigNoChange(t *testing.T) {
	m := topicsModel("orders")
	m.getTopicConfigFn = func(string) ([]kafka.TopicConfigEntry, error) {
		return []kafka.TopicConfigEntry{{Key: "retention.ms", Value: "1000", Editable: true}}, nil
	}
	nm, cmd := m.beginTopicConfig()
	m = nm
	loaded := cmd().(topicConfigLoadedMsg)
	nm2, _ := m.Update(loaded)
	m = nm2.(Model)
	nm3, _ := m.updateTopicConfig(tea.KeyMsg{Type: tea.KeyCtrlS})
	if nm3.(Model).status != "no config changes" {
		t.Fatalf("status %q", nm3.(Model).status)
	}
}
