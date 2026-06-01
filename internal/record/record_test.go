package record

import "testing"

func TestDisplayJSONPrettyPrints(t *testing.T) {
	got := Display([]byte(`{"a":1,"b":"x"}`))
	want := "{\n  \"a\": 1,\n  \"b\": \"x\"\n}"
	if got != want {
		t.Fatalf("Display() =\n%q\nwant\n%q", got, want)
	}
}

func TestDisplayNonJSONReturnsRawText(t *testing.T) {
	if got := Display([]byte("hello")); got != "hello" {
		t.Fatalf("Display() = %q, want %q", got, "hello")
	}
}

func TestDisplayNilIsEmpty(t *testing.T) {
	if got := Display(nil); got != "" {
		t.Fatalf("Display(nil) = %q, want empty", got)
	}
}

func TestValueDisplayPrefersTextThenFallsBack(t *testing.T) {
	r := Record{Value: []byte(`{"a":1}`), ValueText: "decoded!"}
	if got := r.ValueDisplay(); got != "decoded!" {
		t.Fatalf("ValueDisplay with text = %q, want decoded!", got)
	}
	r2 := Record{Value: []byte(`{"a":1}`)}
	if got := r2.ValueDisplay(); got != "{\n  \"a\": 1\n}" {
		t.Fatalf("ValueDisplay fallback = %q", got)
	}
}

func TestKeyDisplayPrefersTextThenFallsBack(t *testing.T) {
	r := Record{Key: []byte("plain"), KeyText: "decoded-key"}
	if got := r.KeyDisplay(); got != "decoded-key" {
		t.Fatalf("KeyDisplay with text = %q", got)
	}
	r2 := Record{Key: []byte("plain")}
	if got := r2.KeyDisplay(); got != "plain" {
		t.Fatalf("KeyDisplay fallback = %q", got)
	}
}
