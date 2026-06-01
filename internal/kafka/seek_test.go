package kafka

import (
	"testing"
	"time"
)

func TestParseStartKeywords(t *testing.T) {
	for _, in := range []string{"", "end", "END", "latest"} {
		s, err := ParseStart(in)
		if err != nil || s.Kind != StartEnd {
			t.Fatalf("ParseStart(%q) = %+v, %v; want StartEnd", in, s, err)
		}
	}
	for _, in := range []string{"beginning", "start", "Beginning"} {
		s, err := ParseStart(in)
		if err != nil || s.Kind != StartBeginning {
			t.Fatalf("ParseStart(%q) = %+v, %v; want StartBeginning", in, s, err)
		}
	}
}

func TestParseStartLastN(t *testing.T) {
	s, err := ParseStart("last:500")
	if err != nil || s.Kind != StartLastN || s.N != 500 {
		t.Fatalf("ParseStart(last:500) = %+v, %v", s, err)
	}
	for _, bad := range []string{"last:0", "last:-3", "last:abc", "last:"} {
		if _, err := ParseStart(bad); err == nil {
			t.Fatalf("ParseStart(%q) = nil error, want error", bad)
		}
	}
}

func TestParseStartDuration(t *testing.T) {
	before := time.Now()
	s, err := ParseStart("1h")
	if err != nil || s.Kind != StartTimestamp {
		t.Fatalf("ParseStart(1h) = %+v, %v", s, err)
	}
	wantApprox := before.Add(-time.Hour)
	if d := s.Time.Sub(wantApprox); d < -5*time.Second || d > 5*time.Second {
		t.Fatalf("ParseStart(1h).Time = %v, want ~%v", s.Time, wantApprox)
	}
	// custom days suffix
	sd, err := ParseStart("2d")
	if err != nil || sd.Kind != StartTimestamp {
		t.Fatalf("ParseStart(2d) = %+v, %v", sd, err)
	}
	if d := before.Sub(sd.Time); d < 47*time.Hour || d > 49*time.Hour {
		t.Fatalf("ParseStart(2d) delta = %v, want ~48h", d)
	}
}

func TestParseStartRFC3339(t *testing.T) {
	s, err := ParseStart("2026-05-27T00:00:00Z")
	if err != nil || s.Kind != StartTimestamp {
		t.Fatalf("ParseStart(rfc3339) = %+v, %v", s, err)
	}
	if !s.Time.Equal(time.Date(2026, 5, 27, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("ParseStart(rfc3339).Time = %v", s.Time)
	}
}

func TestParseStartInvalid(t *testing.T) {
	// Includes non-finite and non-positive durations, which ParseFloat would
	// otherwise accept and turn into "now" or a future time.
	for _, bad := range []string{"500", "garbage", "yesterday", "1x", "-3d", "0s", "NaNd", "infd", "Infd"} {
		if _, err := ParseStart(bad); err == nil {
			t.Fatalf("ParseStart(%q) = nil error, want error", bad)
		}
	}
}

func TestLastNTargets(t *testing.T) {
	start := map[int32]int64{0: 0, 1: 100}
	end := map[int32]int64{0: 10, 1: 105}

	// N smaller than available on p0, larger than available on p1.
	got := lastNTargets(start, end, 3)
	if got[0] != 7 {
		t.Fatalf("p0 target = %d, want 7", got[0])
	}
	if got[1] != 102 {
		t.Fatalf("p1 target = %d, want 102", got[1])
	}

	// N exceeds available -> floored at start offset, never negative.
	got = lastNTargets(start, end, 1000)
	if got[0] != 0 {
		t.Fatalf("p0 floored = %d, want 0", got[0])
	}
	if got[1] != 100 {
		t.Fatalf("p1 floored = %d, want 100 (start offset)", got[1])
	}
}
