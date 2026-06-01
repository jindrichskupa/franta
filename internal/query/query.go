package query

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"franta/internal/record"
)

// Predicate reports whether a record matches a query.
type Predicate func(record.Record) bool

// Parse compiles a query string into a Predicate. An empty/whitespace query
// matches every record.
func Parse(input string) (Predicate, error) {
	if strings.TrimSpace(input) == "" {
		return func(record.Record) bool { return true }, nil
	}
	toks, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	expr, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != tEOF {
		return nil, fmt.Errorf("unexpected token %q", p.peek().text)
	}
	return func(r record.Record) bool { return expr.eval(r) }, nil
}

// --- AST ---

type expr interface{ eval(r record.Record) bool }

type boolExpr struct {
	or          bool
	left, right expr
}

func (b boolExpr) eval(r record.Record) bool {
	if b.or {
		return b.left.eval(r) || b.right.eval(r)
	}
	return b.left.eval(r) && b.right.eval(r)
}

type notExpr struct{ x expr }

func (n notExpr) eval(r record.Record) bool { return !n.x.eval(r) }

type cmpExpr struct {
	field field
	op    string
	lit   string         // raw literal text
	isStr bool           // literal came from a quoted string
	re    *regexp.Regexp // compiled once when op == "matches"
}

func (c cmpExpr) eval(r record.Record) bool {
	return c.field.compare(r, c.op, c.lit, c.isStr, c.re)
}

// --- field selectors ---

type field struct {
	name     string   // key | value | partition | offset | timestamp | header
	jsonPath []string // for value.<path>
	hdrKey   string   // for header['k']
}

// compare extracts the field value from r and compares against the literal.
// The literal's type (string vs number) must match the field's natural type;
// a mismatch never matches. timestamp accepts either an RFC3339 string or an
// epoch-millis number.
func (f field) compare(r record.Record, op, lit string, litIsStr bool, re *regexp.Regexp) bool {
	switch f.name {
	case "partition":
		if litIsStr {
			return false
		}
		return cmpNum(float64(r.Partition), op, lit)
	case "offset":
		if litIsStr {
			return false
		}
		return cmpNum(float64(r.Offset), op, lit)
	case "timestamp":
		return cmpTime(r.Timestamp, op, lit)
	case "key":
		if !litIsStr {
			return false
		}
		return cmpStr(string(r.Key), op, lit, re)
	case "header":
		if !litIsStr {
			return false
		}
		for _, h := range r.Headers {
			if h.Key == f.hdrKey {
				return cmpStr(string(h.Value), op, lit, re)
			}
		}
		return false
	case "value":
		if len(f.jsonPath) == 0 {
			if !litIsStr {
				return false
			}
			return cmpStr(string(r.Value), op, lit, re)
		}
		v, ok := jsonLookup(r.Value, f.jsonPath)
		if !ok {
			return false
		}
		switch tv := v.(type) {
		case string:
			if !litIsStr {
				return false
			}
			return cmpStr(tv, op, lit, re)
		case float64:
			if litIsStr {
				return false
			}
			return cmpNum(tv, op, lit)
		case bool:
			if !litIsStr {
				return false
			}
			return cmpStr(strconv.FormatBool(tv), op, lit, re)
		default:
			return false
		}
	}
	return false
}

func cmpStr(got, op, lit string, re *regexp.Regexp) bool {
	switch op {
	case "==":
		return got == lit
	case "!=":
		return got != lit
	case "contains":
		return strings.Contains(got, lit)
	case "matches":
		return re != nil && re.MatchString(got)
	case "<":
		return got < lit
	case ">":
		return got > lit
	case "<=":
		return got <= lit
	case ">=":
		return got >= lit
	}
	return false
}

func cmpNum(got float64, op, lit string) bool {
	want, err := strconv.ParseFloat(lit, 64)
	if err != nil {
		return false
	}
	switch op {
	case "==":
		return got == want
	case "!=":
		return got != want
	case "<":
		return got < want
	case ">":
		return got > want
	case "<=":
		return got <= want
	case ">=":
		return got >= want
	}
	return false
}

func cmpTime(got time.Time, op, lit string) bool {
	var want time.Time
	if ms, err := strconv.ParseInt(lit, 10, 64); err == nil {
		want = time.UnixMilli(ms).UTC()
	} else if t, err := time.Parse(time.RFC3339, lit); err == nil {
		want = t
	} else {
		return false
	}
	switch op {
	case "==":
		return got.Equal(want)
	case "!=":
		return !got.Equal(want)
	case "<":
		return got.Before(want)
	case ">":
		return got.After(want)
	case "<=":
		return !got.After(want)
	case ">=":
		return !got.Before(want)
	}
	return false
}

func jsonLookup(b []byte, path []string) (any, bool) {
	var m any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, false
	}
	cur := m
	for _, p := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = obj[p]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// --- parser (recursive descent) ---

type parser struct {
	toks []token
	pos  int
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token { t := p.toks[p.pos]; p.pos++; return t }

func (p *parser) parseExpr() (expr, error) { return p.parseOr() }

func (p *parser) parseOr() (expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tOr {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = boolExpr{or: true, left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tAnd {
		p.next()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = boolExpr{or: false, left: left, right: right}
	}
	return left, nil
}

func (p *parser) parseNot() (expr, error) {
	if p.peek().kind == tNot {
		p.next()
		x, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return notExpr{x: x}, nil
	}
	if p.peek().kind == tLParen {
		p.next()
		x, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if p.peek().kind != tRParen {
			return nil, fmt.Errorf("expected ')'")
		}
		p.next()
		return x, nil
	}
	return p.parseCmp()
}

func (p *parser) parseCmp() (expr, error) {
	f, err := p.parseField()
	if err != nil {
		return nil, err
	}
	opTok := p.next()
	var op string
	switch opTok.kind {
	case tOp:
		op = opTok.text
	case tContains:
		op = "contains"
	case tMatches:
		op = "matches"
	default:
		return nil, fmt.Errorf("expected operator after field, got %q", opTok.text)
	}
	litTok := p.next()
	var isStr bool
	switch litTok.kind {
	case tString:
		isStr = true
	case tNumber:
		isStr = false
	default:
		return nil, fmt.Errorf("expected string or number literal, got %q", litTok.text)
	}
	ce := cmpExpr{field: f, op: op, lit: litTok.text, isStr: isStr}
	if op == "matches" {
		if !isStr {
			return nil, fmt.Errorf("matches requires a string regex literal")
		}
		re, err := regexp.Compile(litTok.text)
		if err != nil {
			return nil, fmt.Errorf("invalid regex %q: %w", litTok.text, err)
		}
		ce.re = re
	}
	return ce, nil
}

func (p *parser) parseField() (field, error) {
	t := p.next()
	if t.kind != tIdent {
		return field{}, fmt.Errorf("expected field name, got %q", t.text)
	}
	switch t.text {
	case "key", "partition", "offset", "timestamp":
		return field{name: t.text}, nil
	case "header":
		if p.next().kind != tLBracket {
			return field{}, fmt.Errorf("expected '[' after header")
		}
		k := p.next()
		if k.kind != tString {
			return field{}, fmt.Errorf("expected string header key")
		}
		if p.next().kind != tRBracket {
			return field{}, fmt.Errorf("expected ']' after header key")
		}
		return field{name: "header", hdrKey: k.text}, nil
	case "value":
		f := field{name: "value"}
		for p.peek().kind == tDot {
			p.next()
			seg := p.next()
			if seg.kind != tIdent {
				return field{}, fmt.Errorf("expected json path segment after '.'")
			}
			f.jsonPath = append(f.jsonPath, seg.text)
		}
		return f, nil
	default:
		return field{}, fmt.Errorf("unknown field %q", t.text)
	}
}
