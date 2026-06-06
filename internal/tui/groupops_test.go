package tui

import (
	"errors"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/kafka"
)

func keyEnter() tea.KeyMsg { return tea.KeyMsg{Type: tea.KeyEnter} }

func groupsModel(gs ...kafka.GroupInfo) Model {
	m := New(nil)
	m.width, m.height = 120, 40
	m.mode = modeGroups
	m.groups = gs
	m.applyGroupFilter()
	m.groupCursor = 0
	return m
}

func TestDeleteGroupGate(t *testing.T) {
	m := groupsModel(kafka.GroupInfo{Name: "live", State: "Stable", Members: 2})
	m.deleteGroupFn = func(string) error { return nil }
	nm, _ := m.beginDeleteGroup()
	m2 := nm
	if m2.confirmActive {
		t.Fatal("should not confirm a Stable group")
	}
	if !strings.Contains(m2.errDialog, "active members") {
		t.Fatalf("errDialog %q", m2.errDialog)
	}
}

func TestDeleteGroupConfirmAndRun(t *testing.T) {
	deleted := ""
	m := groupsModel(kafka.GroupInfo{Name: "dead-grp", State: "Empty"})
	m.deleteGroupFn = func(g string) error { deleted = g; return nil }
	nm, _ := m.beginDeleteGroup()
	m = nm
	if !m.confirmActive {
		t.Fatal("expected confirm modal")
	}
	m.confirmInput.SetValue("dead-grp")
	nm2, cmd := m.updateConfirm(keyEnter())
	if nm2.(Model).confirmActive {
		t.Fatal("should close after matching name")
	}
	msg := cmd().(groupMutatedMsg)
	if msg.err != nil || deleted != "dead-grp" {
		t.Fatalf("delete not run: err=%v deleted=%q", msg.err, deleted)
	}
}

func TestGroupMutatedError(t *testing.T) {
	m := groupsModel(kafka.GroupInfo{Name: "g", State: "Empty"})
	nm, _ := m.Update(groupMutatedMsg{action: "deleted", group: "g", err: errors.New("boom")})
	if !strings.Contains(nm.(Model).errDialog, "boom") {
		t.Fatalf("errDialog %q", nm.(Model).errDialog)
	}
}

func TestResetGate(t *testing.T) {
	m := groupsModel(kafka.GroupInfo{Name: "live", State: "Stable", Members: 1})
	m.resetOffsetsFn = func(string, kafka.ResetSpec) error { return nil }
	nm, _ := m.beginReset()
	if nm.resetActive {
		t.Fatal("should not open reset for Stable group")
	}
	if !strings.Contains(nm.errDialog, "active members") {
		t.Fatalf("errDialog %q", nm.errDialog)
	}
}

func TestResetBeginningConfirms(t *testing.T) {
	var gotSpec kafka.ResetSpec
	gotGroup := ""
	m := groupsModel(kafka.GroupInfo{Name: "g", State: "Empty"})
	m.resetOffsetsFn = func(g string, s kafka.ResetSpec) error { gotGroup = g; gotSpec = s; return nil }
	nm, _ := m.beginReset()
	m = nm
	if !m.resetActive {
		t.Fatal("picker should be open")
	}
	// cursor 0 = beginning, enter
	nm2, _ := m.updateReset(keyEnter())
	m = nm2.(Model)
	if !m.confirmActive {
		t.Fatal("should ask confirm")
	}
	_, cmd := m.updateConfirm(keyRunes("y"))
	msg := cmd().(groupMutatedMsg)
	if msg.err != nil || gotGroup != "g" || gotSpec.Kind != kafka.ResetBeginning {
		t.Fatalf("reset spec wrong: %+v group=%q err=%v", gotSpec, gotGroup, msg.err)
	}
}

func TestResetTimestampParse(t *testing.T) {
	var gotSpec kafka.ResetSpec
	m := groupsModel(kafka.GroupInfo{Name: "g", State: "Empty"})
	m.resetOffsetsFn = func(_ string, s kafka.ResetSpec) error { gotSpec = s; return nil }
	m.resetGroup = "g"
	m.resetTSActive = true
	in := textinput.New()
	in.SetValue("1h")
	m.resetTSInput = in
	nm, _ := m.updateResetTS(keyEnter())
	m = nm.(Model)
	if !m.confirmActive {
		t.Fatal("timestamp should lead to confirm")
	}
	_, cmd := m.updateConfirm(keyRunes("y"))
	cmd()
	if gotSpec.Kind != kafka.ResetTimestamp || gotSpec.At.IsZero() {
		t.Fatalf("timestamp spec: %+v", gotSpec)
	}
}

func explicitModel() Model {
	m := groupsModel(kafka.GroupInfo{Name: "g", State: "Empty"})
	m.resetGroup = "g"
	m.resetOffsetsFn = func(string, kafka.ResetSpec) error { return nil }
	m.groupDetail = &kafka.GroupDetail{
		Name:  "g",
		State: "Empty",
		Lag: []kafka.GroupLagRow{
			{Topic: "t", Partition: 0, Committed: 5, End: 10},
			{Topic: "t", Partition: 1, Committed: 2, End: 8},
		},
	}
	nm, _ := m.openExplicitReset()
	return nm.(Model)
}

func TestExplicitInvalidOffset(t *testing.T) {
	m := explicitModel()
	m.explicitInput.SetValue("nope")
	nm, _ := m.updateExplicit(tea.KeyMsg{Type: tea.KeyCtrlS})
	if !strings.Contains(nm.(Model).status, "invalid offset") {
		t.Fatalf("status %q", nm.(Model).status)
	}
}

func TestExplicitExceedsEnd(t *testing.T) {
	m := explicitModel()
	m.explicitInput.SetValue("99")
	nm, _ := m.updateExplicit(tea.KeyMsg{Type: tea.KeyCtrlS})
	if !strings.Contains(nm.(Model).status, "exceeds end") {
		t.Fatalf("status %q", nm.(Model).status)
	}
}

func TestExplicitAppliesChanged(t *testing.T) {
	var gotSpec kafka.ResetSpec
	m := explicitModel()
	m.resetOffsetsFn = func(_ string, s kafka.ResetSpec) error { gotSpec = s; return nil }
	m.explicitInput.SetValue("7") // partition 0: 5 → 7
	nm, _ := m.updateExplicit(tea.KeyMsg{Type: tea.KeyCtrlS})
	m = nm.(Model)
	if !m.confirmActive {
		t.Fatal("should ask confirm")
	}
	_, cmd := m.updateConfirm(keyRunes("y"))
	cmd()
	if gotSpec.Kind != kafka.ResetExplicit || gotSpec.Offsets["t"][0] != 7 {
		t.Fatalf("explicit spec: %+v", gotSpec)
	}
	if _, ok := gotSpec.Offsets["t"][1]; ok {
		t.Fatal("unchanged partition should not be in the spec")
	}
}
