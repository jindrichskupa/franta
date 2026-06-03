package tui

import "time"

// adaLovelaceDay returns Ada Lovelace Day for the given year: the second
// Tuesday of October. ALD has been celebrated on that date since 2009.
func adaLovelaceDay(year int) time.Time {
	first := time.Date(year, time.October, 1, 0, 0, 0, 0, time.UTC)
	// Days until the first Tuesday in October: (Tuesday - weekday) mod 7.
	offset := (int(time.Tuesday) - int(first.Weekday()) + 7) % 7
	// Second Tuesday is 7 days after the first.
	return first.AddDate(0, 0, offset+7)
}

// sameDate reports whether two times fall on the same UTC calendar date.
func sameDate(a, b time.Time) bool {
	ay, am, ad := a.UTC().Date()
	by, bm, bd := b.UTC().Date()
	return ay == by && am == bm && ad == bd
}

// adaGreeting returns a non-empty greeting if today or tomorrow is Ada
// Lovelace Day (a.k.a. "Svátek Ady" in this app). Empty string otherwise.
// `now` is taken as a parameter so tests can pin the clock.
func adaGreeting(now time.Time) string {
	year := now.UTC().Year()
	ald := adaLovelaceDay(year)
	if sameDate(now, ald) {
		return "🎉 Dnes je svátek Ady (Ada Lovelace Day) — všechno nej!"
	}
	if sameDate(now.AddDate(0, 0, 1), ald) {
		return "🎂 Zítra je svátek Ady (Ada Lovelace Day) — kup dort!"
	}
	// Year boundary: if "tomorrow" lands in January but ALD is in October next
	// year, the AddDate check above still works — but checking next-year ALD
	// when we are in late December is moot (ALD is always October).
	return ""
}
