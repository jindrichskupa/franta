package session

import (
	"context"
	"fmt"
	"sync"

	"franta/internal/config"
	"franta/internal/kafka"
)

// Holder owns the current Session and everything Switch needs to build the next
// one. Callback closures capture *Holder and read Cur(), so a swap reroutes
// every callback. The build/ping/start fields are seams overridden in tests.
type Holder struct {
	mu  sync.Mutex
	cur *Session

	build func(name string) (*Session, error) // build only (no start)
	ping  func(*Session) error                // reachability check
	start func(*Session)                      // start consumer goroutine
}

// NewHolder wires the real build/ping/start over the program ctx, cluster
// config, and the shared channels. start is the initial, already-started
// session built in main. Switches always pass topic "" (land in the picker).
func NewHolder(ctx context.Context, clusters map[string]config.Cluster,
	records chan<- kafka.Fetched, errs chan<- error, start *Session) *Holder {
	h := &Holder{cur: start}
	h.build = func(name string) (*Session, error) {
		cl, ok := clusters[name]
		if !ok {
			return nil, fmt.Errorf("unknown cluster %q", name)
		}
		return Build(ctx, cl, name, "")
	}
	h.ping = func(s *Session) error {
		_, err := kafka.ListTopicsBasic(ctx, s.Client, false)
		return err
	}
	h.start = func(s *Session) { s.start(records, errs) }
	return h
}

// Cur returns the current session (locked read).
func (h *Holder) Cur() *Session {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cur
}

func (h *Holder) set(s *Session) {
	h.mu.Lock()
	h.cur = s
	h.mu.Unlock()
}

// Switch builds the named cluster, validates reachability, then swaps it in.
// Build-then-validate-then-swap: a bad target leaves the current session
// running. Returns the new session's consume generation.
func (h *Holder) Switch(name string) (int64, error) {
	ns, err := h.build(name)
	if err != nil {
		return 0, err
	}
	if err := h.ping(ns); err != nil {
		ns.Stop()
		return 0, err
	}
	old := h.Cur()
	if old != nil {
		old.Stop() // stop old before starting new — no double-writer on records
	}
	h.set(ns)
	h.start(ns)
	return ns.Gen(), nil
}
