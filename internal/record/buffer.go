package record

import "sync"

// Buffer is a goroutine-safe ring buffer holding the most recent N records.
type Buffer struct {
	mu   sync.Mutex
	cap  int
	data []Record
}

// NewBuffer returns a buffer holding at most capacity records (min 1).
func NewBuffer(capacity int) *Buffer {
	if capacity < 1 {
		capacity = 1
	}
	return &Buffer{cap: capacity, data: make([]Record, 0, capacity)}
}

// Add appends r, evicting the oldest record once capacity is exceeded.
func (b *Buffer) Add(r Record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data) == b.cap {
		copy(b.data, b.data[1:])
		b.data[len(b.data)-1] = r
		return
	}
	b.data = append(b.data, r)
}

// Records returns a copy of the buffered records, oldest first.
func (b *Buffer) Records() []Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Record, len(b.data))
	copy(out, b.data)
	return out
}

// Len returns the number of buffered records.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.data)
}
