package tui

import "franta/internal/record"

// rateWindow is the number of completed one-second buckets averaged into the
// reported rate. ponytail: fixed 10s window; promote to a field only if a
// config knob is ever asked for.
const rateWindow = 10

// rateMeter tracks ingest throughput as a rolling average over the last
// rateWindow completed seconds. The live bucket (currently filling) is excluded
// from rate() so the partial in-progress second never reads low. tick() rotates
// buckets once per second; it carries no time source, so the unit is
// deterministic. Not safe for concurrent use — driven from the single Bubble
// Tea Update goroutine.
type rateMeter struct {
	// buckets[cur] is the live (in-progress) second; the other rateWindow
	// entries hold completed seconds. Array is rateWindow+1 long so a full
	// rateWindow of completed buckets coexists with the live one.
	buckets [rateWindow + 1]struct {
		count int
		bytes int64
	}
	cur    int // index of the live bucket
	filled int // completed buckets in the window, capped at rateWindow
}

// record adds one message of n bytes to the live bucket.
func (m *rateMeter) record(n int64) {
	m.buckets[m.cur].count++
	m.buckets[m.cur].bytes += n
}

// tick seals the live bucket and starts a fresh one. Call once per second.
func (m *rateMeter) tick() {
	m.cur = (m.cur + 1) % len(m.buckets)
	m.buckets[m.cur] = struct {
		count int
		bytes int64
	}{}
	if m.filled < rateWindow {
		m.filled++
	}
}

// rate returns the per-second average message and byte rate over the completed
// window, excluding the live bucket. Returns 0,0 until at least one second has
// completed.
func (m *rateMeter) rate() (msgPerSec, bytesPerSec float64) {
	if m.filled == 0 {
		return 0, 0
	}
	var count int
	var bytes int64
	for i := range m.buckets {
		if i == m.cur {
			continue // live bucket: mid-fill, would drag the average down
		}
		count += m.buckets[i].count
		bytes += m.buckets[i].bytes
	}
	return float64(count) / float64(m.filled), float64(bytes) / float64(m.filled)
}

// reset clears the window. Call on resume, topic switch, or generation change.
func (m *rateMeter) reset() {
	*m = rateMeter{}
}

// recordBytes is the wire-ish size of a record: key + value + every header's
// key and value bytes.
func recordBytes(r record.Record) int64 {
	n := int64(len(r.Key) + len(r.Value))
	for _, h := range r.Headers {
		n += int64(len(h.Key) + len(h.Value))
	}
	return n
}
