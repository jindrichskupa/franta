package tui

import (
	"testing"

	"franta/internal/record"
)

func TestRateMeterEmpty(t *testing.T) {
	var m rateMeter
	if mps, bps := m.rate(); mps != 0 || bps != 0 {
		t.Fatalf("empty meter: want 0,0 got %v,%v", mps, bps)
	}
}

func TestRateMeterNoCompletedSecond(t *testing.T) {
	var m rateMeter
	m.record(100)
	m.record(100)
	// No tick yet → no completed second → rate still 0.
	if mps, bps := m.rate(); mps != 0 || bps != 0 {
		t.Fatalf("before first tick: want 0,0 got %v,%v", mps, bps)
	}
}

func TestRateMeterSingleSecond(t *testing.T) {
	var m rateMeter
	m.record(100)
	m.record(100)
	m.record(100)
	m.tick()
	mps, bps := m.rate()
	if mps != 3 {
		t.Fatalf("msg/s: want 3 got %v", mps)
	}
	if bps != 300 {
		t.Fatalf("bytes/s: want 300 got %v", bps)
	}
}

func TestRateMeterAveragesWindow(t *testing.T) {
	var m rateMeter
	// second 1: 5 msgs / 500 bytes
	for i := 0; i < 5; i++ {
		m.record(100)
	}
	m.tick()
	// second 2: 3 msgs / 300 bytes
	for i := 0; i < 3; i++ {
		m.record(100)
	}
	m.tick()
	mps, bps := m.rate()
	if mps != 4 { // (5+3)/2
		t.Fatalf("msg/s: want 4 got %v", mps)
	}
	if bps != 400 { // (500+300)/2
		t.Fatalf("bytes/s: want 400 got %v", bps)
	}
}

func TestRateMeterWindowCap(t *testing.T) {
	var m rateMeter
	// Fill many more seconds than the window; each completed second carries
	// exactly 2 msgs. Average must stay 2 regardless of how many ticks ran —
	// proving old buckets fall out of the window.
	for s := 0; s < rateWindow*3; s++ {
		m.record(10)
		m.record(10)
		m.tick()
	}
	mps, bps := m.rate()
	if mps != 2 {
		t.Fatalf("msg/s after cap: want 2 got %v", mps)
	}
	if bps != 20 {
		t.Fatalf("bytes/s after cap: want 20 got %v", bps)
	}
}

func TestRateMeterReset(t *testing.T) {
	var m rateMeter
	m.record(100)
	m.tick()
	m.reset()
	if mps, bps := m.rate(); mps != 0 || bps != 0 {
		t.Fatalf("after reset: want 0,0 got %v,%v", mps, bps)
	}
}

func TestRecordBytes(t *testing.T) {
	rec := record.Record{
		Key:   []byte("ab"),    // 2
		Value: []byte("hello"), // 5
		Headers: []record.Header{
			{Key: "k", Value: []byte("vv")}, // 1 + 2
		},
	}
	if got := recordBytes(rec); got != 10 {
		t.Fatalf("recordBytes: want 10 got %d", got)
	}
}
