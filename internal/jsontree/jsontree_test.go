package jsontree

import "testing"

func TestParseScalarTyping(t *testing.T) {
	cases := []struct {
		in    string
		stype SType
		out   string
	}{
		{`"hi"`, SStr, "hi"},
		{`42`, SNum, "42"},
		{`3.14`, SNum, "3.14"},
		{`true`, SBool, "true"},
		{`false`, SBool, "false"},
		{`null`, SNull, "null"},
	}
	for _, c := range cases {
		n, err := Parse([]byte(c.in))
		if err != nil {
			t.Fatalf("Parse(%s): %v", c.in, err)
		}
		if n.Kind != Scalar {
			t.Errorf("Parse(%s): kind %v want Scalar", c.in, n.Kind)
		}
		if n.SType != c.stype {
			t.Errorf("Parse(%s): stype %v want %v", c.in, n.SType, c.stype)
		}
		if n.Scalar != c.out {
			t.Errorf("Parse(%s): scalar %q want %q", c.in, n.Scalar, c.out)
		}
	}
}

func TestParseKeyOrderPreserved(t *testing.T) {
	// Keys deliberately NOT in sorted order — a map[string]any would reorder.
	n, err := Parse([]byte(`{"zebra":1,"apple":2,"mango":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if n.Kind != Object {
		t.Fatalf("kind %v want Object", n.Kind)
	}
	want := []string{"zebra", "apple", "mango"}
	if len(n.Children) != len(want) {
		t.Fatalf("children %d want %d", len(n.Children), len(want))
	}
	for i, k := range want {
		if n.Children[i].Key != k {
			t.Errorf("child %d key %q want %q", i, n.Children[i].Key, k)
		}
		if !n.Children[i].HasKey {
			t.Errorf("child %d HasKey false", i)
		}
	}
}

func TestParseNestedDepth(t *testing.T) {
	n, err := Parse([]byte(`{"a":{"b":[1,2]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if n.Depth != 0 {
		t.Errorf("root depth %d want 0", n.Depth)
	}
	a := n.Children[0] // "a" -> object
	if a.Depth != 1 || a.Kind != Object {
		t.Errorf("a depth/kind = %d/%v want 1/Object", a.Depth, a.Kind)
	}
	b := a.Children[0] // "b" -> array
	if b.Depth != 2 || b.Kind != Array {
		t.Errorf("b depth/kind = %d/%v want 2/Array", b.Depth, b.Kind)
	}
	if len(b.Children) != 2 {
		t.Fatalf("array children %d want 2", len(b.Children))
	}
	if b.Children[0].HasKey {
		t.Error("array element should not HasKey")
	}
	if b.Children[0].Depth != 3 {
		t.Errorf("array elem depth %d want 3", b.Children[0].Depth)
	}
}

func TestParseInvalid(t *testing.T) {
	if _, err := Parse([]byte(`not json`)); err == nil {
		t.Fatal("expected error for non-JSON")
	}
	if _, err := Parse([]byte(`{"a":}`)); err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestRowsFullyExpanded(t *testing.T) {
	n, _ := Parse([]byte(`{"a":1,"b":[2,3]}`))
	rows := n.Rows()
	// root, a, b(array), 2, 3  => 5 rows
	if len(rows) != 5 {
		t.Fatalf("rows %d want 5: %+v", len(rows), rows)
	}
	if rows[0].Marker != '▾' {
		t.Errorf("root marker %q want ▾", rows[0].Marker)
	}
	if rows[1].Key != "a" || rows[1].Scalar != "1" || rows[1].Marker != ' ' {
		t.Errorf("row1 = %+v", rows[1])
	}
	if rows[2].Key != "b" || rows[2].Marker != '▾' {
		t.Errorf("row2 = %+v", rows[2])
	}
}

func TestRowsHonorCollapsed(t *testing.T) {
	n, _ := Parse([]byte(`{"a":1,"b":[2,3]}`))
	// Collapse the "b" array.
	n.Children[1].Collapsed = true
	rows := n.Rows()
	// root, a, b(collapsed) => 3 rows; 2 and 3 hidden.
	if len(rows) != 3 {
		t.Fatalf("rows %d want 3", len(rows))
	}
	b := rows[2]
	if b.Marker != '▸' {
		t.Errorf("collapsed marker %q want ▸", b.Marker)
	}
	if b.Preview != "[2]" {
		t.Errorf("preview %q want [2]", b.Preview)
	}
}

func TestCollapsedObjectPreview(t *testing.T) {
	n, _ := Parse([]byte(`{"a":1,"b":2,"c":3}`))
	n.Collapsed = true
	rows := n.Rows()
	if len(rows) != 1 {
		t.Fatalf("rows %d want 1", len(rows))
	}
	if rows[0].Preview != "{3}" {
		t.Errorf("preview %q want {3}", rows[0].Preview)
	}
}

func TestToggleChangesRowCount(t *testing.T) {
	n, _ := Parse([]byte(`{"a":{"x":1,"y":2}}`))
	full := len(n.Rows()) // root, a, x, y => 4
	if full != 4 {
		t.Fatalf("full rows %d want 4", full)
	}
	n.Children[0].Collapsed = true
	collapsed := len(n.Rows()) // root, a => 2
	if collapsed != 2 {
		t.Fatalf("collapsed rows %d want 2", collapsed)
	}
}
