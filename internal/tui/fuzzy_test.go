package tui

import (
	"sort"
	"testing"
)

func TestFuzzyMatchSubsequence(t *testing.T) {
	if _, ok := fuzzyMatch("ab", "alphabet"); !ok {
		t.Fatal("ab in alphabet should match")
	}
	if _, ok := fuzzyMatch("ba", "alphabet"); ok {
		t.Fatal("ba in alphabet must NOT match (order required)")
	}
	if _, ok := fuzzyMatch("", "anything"); !ok {
		t.Fatal("empty query should always match")
	}
}

func TestFuzzyMatchCaseInsensitive(t *testing.T) {
	if _, ok := fuzzyMatch("AB", "alphabet"); !ok {
		t.Fatal("AB in alphabet should match case-insensitively")
	}
	if _, ok := fuzzyMatch("ab", "ALPHABET"); !ok {
		t.Fatal("ab in ALPHABET should match case-insensitively")
	}
}

func TestFuzzyMatchScoring(t *testing.T) {
	sPrefix, _ := fuzzyMatch("al", "alpha")
	sMiddle, _ := fuzzyMatch("al", "metal-alloy")
	if sPrefix <= sMiddle {
		t.Fatalf("prefix score %d should beat middle %d", sPrefix, sMiddle)
	}
	sConsec, _ := fuzzyMatch("al", "alxxx")
	sGapped, _ := fuzzyMatch("al", "axxxxl")
	if sConsec <= sGapped {
		t.Fatalf("consecutive %d should beat gapped %d", sConsec, sGapped)
	}
}

func TestFuzzyMatchRankingOrder(t *testing.T) {
	candidates := []string{"meta-orders", "orders", "_orders_internal", "out-of-doors"}
	type m struct {
		name  string
		score int
	}
	var matches []m
	for _, c := range candidates {
		if s, ok := fuzzyMatch("orders", c); ok {
			matches = append(matches, m{c, s})
		}
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i].score > matches[j].score })
	if matches[0].name != "orders" {
		t.Fatalf("best match = %q, want orders (highest score)", matches[0].name)
	}
}
