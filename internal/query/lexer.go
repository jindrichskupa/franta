package query

import (
	"fmt"
	"strings"
	"unicode"
)

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tNumber
	tString
	tOp // == != < > <= >=
	tAnd
	tOr
	tNot
	tContains
	tMatches
	tLParen
	tRParen
	tLBracket
	tRBracket
	tDot
)

type token struct {
	kind tokKind
	text string
}

// lex tokenizes a query string.
func lex(s string) ([]token, error) {
	var toks []token
	r := []rune(s)
	i := 0
	for i < len(r) {
		c := r[i]
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '(':
			toks = append(toks, token{tLParen, "("})
			i++
		case c == ')':
			toks = append(toks, token{tRParen, ")"})
			i++
		case c == '[':
			toks = append(toks, token{tLBracket, "["})
			i++
		case c == ']':
			toks = append(toks, token{tRBracket, "]"})
			i++
		case c == '.':
			toks = append(toks, token{tDot, "."})
			i++
		case c == '\'' || c == '"':
			j := i + 1
			for j < len(r) && r[j] != c {
				j++
			}
			if j >= len(r) {
				return nil, fmt.Errorf("unterminated string at %d", i)
			}
			toks = append(toks, token{tString, string(r[i+1 : j])})
			i = j + 1
		case c == '=' || c == '!' || c == '<' || c == '>':
			j := i + 1
			if j < len(r) && r[j] == '=' {
				toks = append(toks, token{tOp, string(r[i : j+1])})
				i = j + 1
			} else if c == '<' || c == '>' {
				toks = append(toks, token{tOp, string(c)})
				i++
			} else {
				return nil, fmt.Errorf("unexpected %q at %d", string(c), i)
			}
		case isNumStart(c):
			j := i
			for j < len(r) && (unicode.IsDigit(r[j]) || r[j] == '.' || r[j] == '-' || r[j] == '+' || r[j] == 'e' || r[j] == 'E') {
				j++
			}
			toks = append(toks, token{tNumber, string(r[i:j])})
			i = j
		case unicode.IsLetter(c) || c == '_':
			j := i
			for j < len(r) && (unicode.IsLetter(r[j]) || unicode.IsDigit(r[j]) || r[j] == '_') {
				j++
			}
			word := string(r[i:j])
			toks = append(toks, keywordToken(word))
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q at %d", string(c), i)
		}
	}
	toks = append(toks, token{tEOF, ""})
	return toks, nil
}

func isNumStart(c rune) bool {
	return unicode.IsDigit(c) || c == '-'
}

func keywordToken(word string) token {
	switch strings.ToLower(word) {
	case "and":
		return token{tAnd, word}
	case "or":
		return token{tOr, word}
	case "not":
		return token{tNot, word}
	case "contains":
		return token{tContains, word}
	case "matches":
		return token{tMatches, word}
	default:
		return token{tIdent, word}
	}
}
