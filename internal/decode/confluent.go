package decode

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sr"

	"franta/internal/record"
)

// cachedSchema is a positive or negative cache entry. ok=true means the parsed
// schema is present in kind/text. ok=false means a recent fetch failed; we keep
// the entry so we don't retry the SR on every record. expiresAt applies to
// negative entries only (positive entries never expire within a session).
type cachedSchema struct {
	ok        bool
	kind      schemaKind
	text      string
	expiresAt time.Time
}

// negativeCacheTTL bounds how long a failed schema lookup is remembered before
// the consumer tries the SR again.
const negativeCacheTTL = 30 * time.Second

// schemaLookupTimeout caps the SR HTTP request issued from the consumer's hot
// path. Kept tight so a slow/unreachable SR cannot stall ingestion for long.
const schemaLookupTimeout = 1 * time.Second

// Registry decodes Confluent-wire-format records using a Schema Registry.
type Registry struct {
	client *sr.Client
	mu     sync.Mutex
	cache  map[int]cachedSchema
}

// NewConfluent builds a Registry talking to the given SR URL with optional
// basic auth, and verifies the URL is reachable. Used at startup; an error
// here is fatal at the call site.
func NewConfluent(url, username, password string) (*Registry, error) {
	// Note: franz-go/pkg/sr v1.7.0 uses sr.ClientOpt (the plan used sr.Opt).
	opts := []sr.ClientOpt{sr.URLs(url)}
	if username != "" || password != "" {
		opts = append(opts, sr.BasicAuth(username, password))
	}
	cl, err := sr.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	// Verify connectivity with a cheap HTTP GET. sr.NewClient itself does not
	// probe the URL, so a typo'd config would otherwise only surface as silent
	// per-record decode failures.
	if err := pingSR(url, username, password); err != nil {
		return nil, fmt.Errorf("schema registry %s unreachable: %w", url, err)
	}
	return &Registry{client: cl, cache: map[int]cachedSchema{}}, nil
}

// pingSR does a short HTTP GET to the SR URL to confirm it responds. Any HTTP
// status counts as "reachable"; only transport errors fail.
func pingSR(url, username, password string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if username != "" || password != "" {
		req.SetBasicAuth(username, password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

// seed is a test-only helper that injects a parsed schema into the cache,
// avoiding a real Schema Registry round-trip.
func (r *Registry) seed(id int, kind schemaKind, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[id] = cachedSchema{ok: true, kind: kind, text: text}
}

// Decode renders bytes for display, decoding via Schema Registry when the
// Confluent wire format is detected. Records that look like wire format but
// cannot be decoded (unknown id, decode error) render a deterministic
// placeholder rather than feeding binary header bytes to record.Display.
func (r *Registry) Decode(b []byte) string {
	if len(b) < 5 || b[0] != 0x00 {
		return record.Display(b)
	}
	id := int(binary.BigEndian.Uint32(b[1:5]))
	payload := b[5:]
	cs, ok := r.lookup(id)
	if !ok {
		return fmt.Sprintf("<sr id=%d: schema unavailable>", id)
	}
	s, err := decodeWith(cs.kind, cs.text, payload)
	if err != nil {
		return fmt.Sprintf("<sr id=%d: decode error: %v>", id, err)
	}
	return s
}

// lookup returns the cached schema for id, fetching from the registry on a
// miss. Failures are cached for negativeCacheTTL so a misconfigured SR cannot
// burn the lookup timeout on every record.
func (r *Registry) lookup(id int) (cachedSchema, bool) {
	now := time.Now()
	r.mu.Lock()
	cs, exists := r.cache[id]
	r.mu.Unlock()
	if exists && cs.ok {
		return cs, true
	}
	if exists && !cs.ok && now.Before(cs.expiresAt) {
		return cs, false
	}
	if r.client == nil {
		return cachedSchema{}, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), schemaLookupTimeout)
	defer cancel()
	schema, err := r.client.SchemaByID(ctx, id)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.cache[id] = cachedSchema{ok: false, expiresAt: now.Add(negativeCacheTTL)}
		return cachedSchema{}, false
	}
	pos := cachedSchema{ok: true, kind: srTypeToKind(schema.Type), text: schema.Schema}
	r.cache[id] = pos
	return pos, true
}

func srTypeToKind(t sr.SchemaType) schemaKind {
	switch t {
	case sr.TypeAvro:
		return kindAvro
	case sr.TypeJSON:
		return kindJSON
	case sr.TypeProtobuf:
		return kindProtobuf
	}
	return kindOther
}
