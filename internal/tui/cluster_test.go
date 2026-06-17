package tui

import (
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func TestClusterKeyOpensPicker(t *testing.T) {
	m := New(nil)
	m.width, m.height = 120, 40
	m.clusters = []string{"local", "prod"}
	m.switchClusterFn = func(string) (int64, error) { return 0, nil }

	nm, _ := m.handleKey(keyRunes("C"))
	if !nm.(Model).pickingCluster {
		t.Fatal("C should open the cluster picker")
	}
}

func TestClusterKeySingleNoop(t *testing.T) {
	m := New(nil)
	m.width, m.height = 120, 40
	m.clusters = []string{"local"}
	m.switchClusterFn = func(string) (int64, error) { return 0, nil }

	nm, _ := m.handleKey(keyRunes("C"))
	m2 := nm.(Model)
	if m2.pickingCluster {
		t.Fatal("single cluster must not open picker")
	}
	if !strings.Contains(m2.status, "one cluster") {
		t.Fatalf("status %q", m2.status)
	}
}

func TestClusterPickerSelectFiresSwitch(t *testing.T) {
	picked := ""
	m := New(nil)
	m.width, m.height = 120, 40
	m.clusters = []string{"local", "prod"}
	m.switchClusterFn = func(n string) (int64, error) { picked = n; return 7, nil }
	m.pickingCluster = true
	m.clusterPickCursor = 1 // "prod"

	nm, cmd := m.updateClusterPicker(tea.KeyMsg{Type: tea.KeyEnter})
	if nm.(Model).pickingCluster {
		t.Fatal("selecting should close the picker")
	}
	msg := cmd().(clusterSwitchedMsg)
	if picked != "prod" || msg.cluster != "prod" || msg.gen != 7 || msg.err != nil {
		t.Fatalf("switch result: picked=%q msg=%+v", picked, msg)
	}
}

func TestClusterSwitchedSuccessResets(t *testing.T) {
	m := newModelWithRecords()
	m.cluster = "local"
	m.topic = "orders"
	nm, cmd := m.Update(clusterSwitchedMsg{cluster: "prod", gen: 3})
	m2 := nm.(Model)
	if m2.cluster != "prod" || m2.topic != "" {
		t.Fatalf("header/topic not reset: %q %q", m2.cluster, m2.topic)
	}
	if m2.curGen != 3 {
		t.Fatalf("curGen %d", m2.curGen)
	}
	if m2.paneFocus != paneTopics {
		t.Fatal("focus should be topics pane")
	}
	if cmd == nil {
		t.Fatal("expected a topics-load cmd")
	}
}

func TestClusterSwitchedError(t *testing.T) {
	m := newModelWithRecords()
	m.cluster = "local"
	nm, _ := m.Update(clusterSwitchedMsg{cluster: "prod", err: errors.New("unreachable")})
	m2 := nm.(Model)
	if m2.cluster != "local" {
		t.Fatal("cluster must be unchanged on error")
	}
	if !strings.Contains(m2.errDialog, "unreachable") {
		t.Fatalf("errDialog %q", m2.errDialog)
	}
}
