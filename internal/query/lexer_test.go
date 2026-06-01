package query

import "testing"

func kinds(toks []token) []tokKind {
	out := make([]tokKind, len(toks))
	for i, t := range toks {
		out[i] = t.kind
	}
	return out
}

func TestLexSimpleComparison(t *testing.T) {
	toks, err := lex(`partition == 2`)
	if err != nil {
		t.Fatalf("lex: %v", err)
	}
	want := []tokKind{tIdent, tOp, tNumber, tEOF}
	got := kinds(toks)
	if len(got) != len(want) {
		t.Fatalf("kinds = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("kinds = %v, want %v", got, want)
		}
	}
	if toks[2].text != "2" {
		t.Fatalf("number text = %q", toks[2].text)
	}
}

func TestLexStringAndKeywords(t *testing.T) {
	toks, err := lex(`value contains "ab c" and not key == 'x'`)
	if err != nil {
		t.Fatalf("lex: %v", err)
	}
	// value contains STRING and not key == STRING EOF
	want := []tokKind{tIdent, tOp, tString, tAnd, tNot, tIdent, tOp, tString, tEOF}
	got := kinds(toks)
	if len(got) != len(want) {
		t.Fatalf("len kinds = %d (%v), want %d", len(got), got, len(want))
	}
	if toks[2].text != "ab c" {
		t.Fatalf("string text = %q, want %q", toks[2].text, "ab c")
	}
}

func TestLexBracketField(t *testing.T) {
	toks, err := lex(`header['k'] == "v"`)
	if err != nil {
		t.Fatalf("lex: %v", err)
	}
	want := []tokKind{tIdent, tLBracket, tString, tRBracket, tOp, tString, tEOF}
	if got := kinds(toks); len(got) != len(want) {
		t.Fatalf("kinds = %v, want %v", got, want)
	}
}
