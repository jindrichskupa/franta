package tui

import (
	"strings"
	"testing"
	"time"
)

// TestAdaLovelaceDayKnownYears pins the function against documented historical
// dates so any refactor that breaks "second Tuesday of October" gets caught.
func TestAdaLovelaceDayKnownYears(t *testing.T) {
	cases := []struct {
		year     int
		expected string // YYYY-MM-DD
	}{
		{2009, "2009-10-13"},
		{2015, "2015-10-13"},
		{2020, "2020-10-13"},
		{2024, "2024-10-08"},
		{2025, "2025-10-14"},
		{2026, "2026-10-13"},
		{2027, "2027-10-12"},
		{2030, "2030-10-08"},
	}
	for _, tc := range cases {
		got := adaLovelaceDay(tc.year).Format("2006-01-02")
		if got != tc.expected {
			t.Errorf("adaLovelaceDay(%d) = %s, want %s", tc.year, got, tc.expected)
		}
		if adaLovelaceDay(tc.year).Weekday() != time.Tuesday {
			t.Errorf("adaLovelaceDay(%d) is not a Tuesday", tc.year)
		}
	}
}

func TestAdaGreetingToday(t *testing.T) {
	now := time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC)
	g := adaGreeting(now)
	if !strings.Contains(g, "Dnes") {
		t.Fatalf("expected 'Dnes' greeting, got %q", g)
	}
}

func TestAdaGreetingTomorrow(t *testing.T) {
	now := time.Date(2026, 10, 12, 23, 0, 0, 0, time.UTC)
	g := adaGreeting(now)
	if !strings.Contains(g, "Zítra") {
		t.Fatalf("expected 'Zítra' greeting, got %q", g)
	}
}

func TestAdaGreetingOtherDays(t *testing.T) {
	cases := []time.Time{
		time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC),
	}
	for _, now := range cases {
		if g := adaGreeting(now); g != "" {
			t.Errorf("adaGreeting(%s) = %q, want empty", now.Format("2006-01-02"), g)
		}
	}
}
