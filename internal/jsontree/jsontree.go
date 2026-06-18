// Package jsontree parses JSON into a foldable tree model for display. It is
// pure (no TUI dependencies) and preserves object key order, which is why it
// walks json.Decoder's token stream instead of decoding into map[string]any.
package jsontree

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// Kind classifies a node as a leaf scalar or a foldable container.
type Kind int

const (
	Scalar Kind = iota
	Object
	Array
)

// SType is the scalar value type, used to colour leaves in the view.
type SType int

const (
	SNone SType = iota
	SStr
	SNum
	SBool
	SNull
)

// Node is one parsed JSON value. Containers hold ordered Children; scalars
// hold Scalar/SType. Collapsed folds a container's subtree out of Rows().
type Node struct {
	Key       string // object-member key (empty for array elements / root)
	HasKey    bool   // true when this node is an object member
	Kind      Kind
	Scalar    string // rendered scalar text (Kind==Scalar)
	SType     SType
	Children  []*Node
	Collapsed bool
	Depth     int
}

// Row is one flattened, displayable line produced by Rows().
type Row struct {
	Depth   int
	Marker  rune   // '▸' collapsed · '▾' expanded · ' ' scalar
	Key     string // member key, if any
	Preview string // "{N}" / "[N]" for folded containers, else ""
	Scalar  string // scalar text (Kind==Scalar)
	SType   SType
	Node    *Node // back-reference so callers can toggle Collapsed
}

// Parse builds a tree from JSON bytes. It returns an error for malformed or
// non-JSON input (callers fall back to raw rendering in that case). Numbers
// keep their original textual form via Decoder.UseNumber.
func Parse(b []byte) (*Node, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	n, err := parseValue(dec)
	if err != nil {
		return nil, err
	}
	// Reject trailing garbage (e.g. "1 2" or "{} junk"): a well-formed value
	// must be the only token stream.
	if dec.More() {
		return nil, fmt.Errorf("unexpected trailing data")
	}
	setDepth(n, 0)
	return n, nil
}

func parseValue(dec *json.Decoder) (*Node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			return parseObject(dec)
		case '[':
			return parseArray(dec)
		default:
			return nil, fmt.Errorf("unexpected delimiter %q", t)
		}
	case string:
		return &Node{Kind: Scalar, Scalar: t, SType: SStr}, nil
	case json.Number:
		return &Node{Kind: Scalar, Scalar: t.String(), SType: SNum}, nil
	case bool:
		return &Node{Kind: Scalar, Scalar: strconv.FormatBool(t), SType: SBool}, nil
	case nil:
		return &Node{Kind: Scalar, Scalar: "null", SType: SNull}, nil
	default:
		return nil, fmt.Errorf("unexpected token %T", tok)
	}
}

func parseObject(dec *json.Decoder) (*Node, error) {
	n := &Node{Kind: Object}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("expected object key, got %T", keyTok)
		}
		child, err := parseValue(dec)
		if err != nil {
			return nil, err
		}
		child.Key = key
		child.HasKey = true
		n.Children = append(n.Children, child)
	}
	if _, err := dec.Token(); err != nil { // consume '}'
		return nil, err
	}
	return n, nil
}

func parseArray(dec *json.Decoder) (*Node, error) {
	n := &Node{Kind: Array}
	for dec.More() {
		child, err := parseValue(dec)
		if err != nil {
			return nil, err
		}
		n.Children = append(n.Children, child)
	}
	if _, err := dec.Token(); err != nil { // consume ']'
		return nil, err
	}
	return n, nil
}

func setDepth(n *Node, d int) {
	n.Depth = d
	for _, c := range n.Children {
		setDepth(c, d+1)
	}
}

// Rows flattens the tree into display rows, skipping the children of any
// collapsed container.
func (n *Node) Rows() []Row {
	var rows []Row
	n.appendRows(&rows)
	return rows
}

func (n *Node) appendRows(rows *[]Row) {
	r := Row{Depth: n.Depth, Key: n.Key, Node: n}
	switch n.Kind {
	case Scalar:
		r.Marker = ' '
		r.Scalar = n.Scalar
		r.SType = n.SType
	default: // Object / Array
		if n.Collapsed {
			r.Marker = '▸'
			r.Preview = n.preview()
		} else {
			r.Marker = '▾'
		}
	}
	*rows = append(*rows, r)
	if n.Kind != Scalar && !n.Collapsed {
		for _, c := range n.Children {
			c.appendRows(rows)
		}
	}
}

// preview is the compact "{N}" / "[N]" child-count shown on a folded container.
func (n *Node) preview() string {
	if n.Kind == Array {
		return fmt.Sprintf("[%d]", len(n.Children))
	}
	return fmt.Sprintf("{%d}", len(n.Children))
}
