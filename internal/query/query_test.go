package query

import (
	"testing"
	"time"

	"franta/internal/record"
)

func rec() record.Record {
	return record.Record{
		Partition: 2,
		Offset:    100,
		Timestamp: time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC),
		Key:       []byte("user-42"),
		Value:     []byte(`{"name":"alice","age":30,"active":true}`),
		Headers:   []record.Header{{Key: "source", Value: []byte("web")}},
	}
}

func mustMatch(t *testing.T, q string, want bool) {
	t.Helper()
	p, err := Parse(q)
	if err != nil {
		t.Fatalf("Parse(%q): %v", q, err)
	}
	if got := p(rec()); got != want {
		t.Fatalf("Parse(%q) match = %v, want %v", q, got, want)
	}
}

func TestEmptyQueryMatchesAll(t *testing.T) { mustMatch(t, "", true) }

func TestNumericComparisons(t *testing.T) {
	mustMatch(t, "partition == 2", true)
	mustMatch(t, "partition == 3", false)
	mustMatch(t, "offset >= 100", true)
	mustMatch(t, "offset > 100", false)
}

func TestStringOps(t *testing.T) {
	mustMatch(t, `key contains "user"`, true)
	mustMatch(t, `key == "user-42"`, true)
	mustMatch(t, `key matches "^user-[0-9]+$"`, true)
	mustMatch(t, `value contains "alice"`, true)
}

func TestJSONPath(t *testing.T) {
	mustMatch(t, `value.name == "alice"`, true)
	mustMatch(t, `value.age >= 18`, true)
	mustMatch(t, `value.age == 31`, false)
	mustMatch(t, `value.missing == "x"`, false) // missing path never matches
}

func TestHeaderField(t *testing.T) {
	mustMatch(t, `header['source'] == "web"`, true)
	mustMatch(t, `header['nope'] == "web"`, false)
}

func TestBooleanComposition(t *testing.T) {
	mustMatch(t, `partition == 2 and value.name == "alice"`, true)
	mustMatch(t, `partition == 9 or offset == 100`, true)
	mustMatch(t, `not partition == 9`, true)
	mustMatch(t, `(partition == 9 or offset == 100) and key contains "user"`, true)
}

func TestParseErrors(t *testing.T) {
	for _, q := range []string{"partition ==", "and key == \"x\"", "partition = 2", "value.name == "} {
		if _, err := Parse(q); err == nil {
			t.Fatalf("Parse(%q) = nil error, want error", q)
		}
	}
}

func TestLiteralTypeMustMatchField(t *testing.T) {
	// Numeric field with a quoted-string literal must not match.
	mustMatch(t, `partition == "2"`, false)
	mustMatch(t, `offset == "100"`, false)
	// Numeric JSON value with a string literal must not match.
	mustMatch(t, `value.age == "30"`, false)
	// String field with a numeric literal must not match.
	mustMatch(t, `key == 42`, false)
	// Sanity: correctly-typed comparisons still match.
	mustMatch(t, `partition == 2`, true)
	mustMatch(t, `value.age == 30`, true)
}

func TestBadRegexIsParseError(t *testing.T) {
	if _, err := Parse(`value matches "[unclosed"`); err == nil {
		t.Fatal(`Parse with invalid regex = nil error, want error`)
	}
	if _, err := Parse(`partition matches 5`); err == nil {
		t.Fatal(`matches with numeric literal = nil error, want error`)
	}
}
