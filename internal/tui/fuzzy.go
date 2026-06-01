package tui

import "strings"

// fuzzyMatch reports whether every rune of query appears in order in
// candidate (case-insensitive), returning a heuristic score. Higher = better.
// Empty query matches everything with score 0.
func fuzzyMatch(query, candidate string) (int, bool) {
	if query == "" {
		return 0, true
	}
	q := strings.ToLower(query)
	c := strings.ToLower(candidate)
	qi := 0
	var score, lastIdx int
	lastIdx = -2
	for i := 0; qi < len(q) && i < len(c); i++ {
		if c[i] == q[qi] {
			score += 10
			if i == 0 && qi == 0 {
				score += 25 // prefix bonus
			}
			if i == lastIdx+1 {
				score += 15 // consecutive bonus
			}
			lastIdx = i
			qi++
		}
	}
	if qi < len(q) {
		return 0, false
	}
	return score, true
}
