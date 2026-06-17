package session

import (
	"errors"
	"testing"
)

// fakeSession returns a Session with no real clients; Stop just flips stopped.
func fakeSession(name string) *Session {
	return &Session{Name: name} // Cons nil → Gen()==0; cancel/done nil → Stop safe
}

func TestSwitchSuccess(t *testing.T) {
	old := fakeSession("a")
	h := &Holder{cur: old}
	var built string
	h.build = func(name string) (*Session, error) { built = name; return fakeSession(name), nil }
	h.ping = func(*Session) error { return nil }
	h.start = func(*Session) {}

	gen, err := h.Switch("b")
	if err != nil {
		t.Fatalf("switch: %v", err)
	}
	if gen != 0 {
		t.Fatalf("gen %d", gen)
	}
	if h.Cur().Name != "b" {
		t.Fatalf("cur = %q", h.Cur().Name)
	}
	if !old.stopped {
		t.Fatal("old session not stopped")
	}
	_ = built
}

func TestSwitchBuildFails(t *testing.T) {
	old := fakeSession("a")
	h := &Holder{cur: old}
	h.build = func(string) (*Session, error) { return nil, errors.New("no such cluster") }
	h.ping = func(*Session) error { return nil }
	h.start = func(*Session) {}

	if _, err := h.Switch("b"); err == nil {
		t.Fatal("expected build error")
	}
	if h.Cur() != old || old.stopped {
		t.Fatal("cur must be unchanged and old still running on build failure")
	}
}

func TestSwitchPingFails(t *testing.T) {
	old := fakeSession("a")
	var rejected *Session
	h := &Holder{cur: old}
	h.build = func(name string) (*Session, error) { rejected = fakeSession(name); return rejected, nil }
	h.ping = func(*Session) error { return errors.New("unreachable") }
	h.start = func(*Session) {}

	if _, err := h.Switch("b"); err == nil {
		t.Fatal("expected ping error")
	}
	if h.Cur() != old || old.stopped {
		t.Fatal("cur unchanged + old running on ping failure")
	}
	if !rejected.stopped {
		t.Fatal("rejected session must be stopped")
	}
}
