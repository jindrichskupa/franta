package tui

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"franta/internal/record"
)

type nopWriteCloser struct{ *bytes.Buffer }

func (nopWriteCloser) Close() error { return nil }

type errWriteCloser struct{}

func (errWriteCloser) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func (errWriteCloser) Close() error              { return nil }

type errCloseWriter struct{ *bytes.Buffer }

func (errCloseWriter) Close() error { return errors.New("close failed") }

func TestOpenExportEmpty(t *testing.T) {
	m := New(nil)
	m.width, m.height = 120, 40
	m = m.openExport()
	if m.mode == modeExport {
		t.Fatal("should not enter export mode with empty buffer")
	}
	if m.status != "nothing to export" {
		t.Fatalf("status %q", m.status)
	}
}

func TestOpenExportDefaultPath(t *testing.T) {
	m := newModelWithRecords(record.Record{Offset: 1, Key: []byte("k"), Value: []byte("v")})
	m.topic = "orders"
	m = m.openExport()
	if m.mode != modeExport {
		t.Fatal("expected export mode")
	}
	if m.exportN != 1 {
		t.Fatalf("count %d", m.exportN)
	}
	p := m.exportPath.Value()
	if !strings.HasPrefix(p, "franta-orders-") || !strings.HasSuffix(p, ".jsonl") {
		t.Fatalf("default path %q", p)
	}
}

func TestExportFormatCycle(t *testing.T) {
	m := newModelWithRecords(record.Record{Offset: 1, Key: []byte("k"), Value: []byte("v")})
	m.topic = "orders"
	m = m.openExport()

	nm, _ := m.updateExport(tea.KeyMsg{Type: tea.KeyTab})
	m = nm.(Model)
	if m.exportFmt != fmtJSONArray {
		t.Fatalf("after 1 tab: %v", m.exportFmt)
	}
	if !strings.HasSuffix(m.exportPath.Value(), ".json") {
		t.Fatalf("path ext not updated: %q", m.exportPath.Value())
	}

	// esc cancels back to normal mode.
	nm, _ = m.updateExport(tea.KeyMsg{Type: tea.KeyEsc})
	if nm.(Model).mode != modeNormal {
		t.Fatal("esc should return to normal mode")
	}
}

func TestExportPathEditedStopsAutoDerive(t *testing.T) {
	m := newModelWithRecords(record.Record{Offset: 1, Key: []byte("k")})
	m.topic = "orders"
	m = m.openExport()
	// Simulate a keystroke in the path field.
	nm, _ := m.updateExport(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("x")})
	m = nm.(Model)
	edited := m.exportPath.Value()
	nm, _ = m.updateExport(tea.KeyMsg{Type: tea.KeyTab})
	m = nm.(Model)
	if m.exportPath.Value() != edited {
		t.Fatalf("path should not be re-derived after edit: %q -> %q", edited, m.exportPath.Value())
	}
}

func TestSubmitExportWritesVisible(t *testing.T) {
	var buf bytes.Buffer
	m := newModelWithRecords(
		record.Record{Partition: 0, Offset: 1, Key: []byte("k1"), Value: []byte("v1")},
		record.Record{Partition: 1, Offset: 2, Key: []byte("k2"), Value: []byte("v2")},
	)
	m.topic = "orders"
	m = m.openExport()
	m.openFn = func(string) (io.WriteCloser, error) { return nopWriteCloser{&buf}, nil }

	_, cmd := m.submitExport()
	msg := cmd().(exportDoneMsg)
	if msg.err != nil {
		t.Fatalf("unexpected err: %v", msg.err)
	}
	if msg.n != 2 {
		t.Fatalf("n=%d", msg.n)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 jsonl lines, got %d:\n%s", len(lines), buf.String())
	}

	// Result handling sets status + returns to normal mode.
	nm, _ := m.Update(msg)
	m2 := nm.(Model)
	if m2.mode != modeNormal || !strings.HasPrefix(m2.status, "exported 2 ") {
		t.Fatalf("mode=%v status=%q", m2.mode, m2.status)
	}
}

func TestSubmitExportWriteError(t *testing.T) {
	m := newModelWithRecords(record.Record{Offset: 1, Key: []byte("k"), Value: []byte("v")})
	m = m.openExport()
	m.openFn = func(string) (io.WriteCloser, error) { return errWriteCloser{}, nil }
	_, cmd := m.submitExport()
	msg := cmd().(exportDoneMsg)
	if msg.err == nil || !strings.Contains(msg.err.Error(), "write failed") {
		t.Fatalf("want write error, got %v", msg.err)
	}
}

func TestSubmitExportCloseError(t *testing.T) {
	var buf bytes.Buffer
	m := newModelWithRecords(record.Record{Offset: 1, Key: []byte("k"), Value: []byte("v")})
	m = m.openExport()
	m.openFn = func(string) (io.WriteCloser, error) { return errCloseWriter{&buf}, nil }
	_, cmd := m.submitExport()
	msg := cmd().(exportDoneMsg)
	if msg.err == nil || !strings.Contains(msg.err.Error(), "close failed") {
		t.Fatalf("want close error, got %v", msg.err)
	}
}

func TestExpandPath(t *testing.T) {
	orig := homeDir
	homeDir = func() (string, error) { return "/home/u", nil }
	defer func() { homeDir = orig }()
	cases := map[string]string{
		"~":      "/home/u",
		"~/a/b":  "/home/u/a/b",
		"/abs/x": "/abs/x",
		"rel/y":  "rel/y",
	}
	for in, want := range cases {
		if got := expandPath(in); got != want {
			t.Errorf("expandPath(%q)=%q want %q", in, got, want)
		}
	}
}

func TestSubmitExportOpenError(t *testing.T) {
	m := newModelWithRecords(record.Record{Offset: 1, Key: []byte("k")})
	m = m.openExport()
	m.openFn = func(string) (io.WriteCloser, error) { return nil, errors.New("denied") }

	_, cmd := m.submitExport()
	msg := cmd().(exportDoneMsg)
	if msg.err == nil {
		t.Fatal("expected error")
	}
	nm, _ := m.Update(msg)
	if !strings.Contains(nm.(Model).errDialog, "denied") {
		t.Fatalf("errDialog %q", nm.(Model).errDialog)
	}
}
