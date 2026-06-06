package record

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"strconv"
	"time"
)

// Wire is the JSON-serializable projection of a Record used by the export
// feature and the clipboard "copy record" action. Headers are flattened to a
// string map (last value wins on duplicate keys).
type Wire struct {
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp string            `json:"timestamp"` // RFC3339
	Key       string            `json:"key"`
	Value     string            `json:"value"` // decoded display form
	Headers   map[string]string `json:"headers,omitempty"`
}

// ToWire projects a Record into its Wire form, using the decoded display
// renderings of key and value.
func ToWire(r Record) Wire {
	var hdrs map[string]string
	if len(r.Headers) > 0 {
		hdrs = make(map[string]string, len(r.Headers))
		for _, h := range r.Headers {
			hdrs[h.Key] = string(h.Value)
		}
	}
	return Wire{
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp.UTC().Format(time.RFC3339),
		Key:       r.KeyDisplay(),
		Value:     r.ValueDisplay(),
		Headers:   hdrs,
	}
}

// JSON renders a single record as pretty-printed JSON.
func JSON(r Record) ([]byte, error) {
	return json.MarshalIndent(ToWire(r), "", "  ")
}

// WriteJSONL writes one JSON object per line (newline-delimited).
func WriteJSONL(w io.Writer, recs []Record) error {
	enc := json.NewEncoder(w)
	for _, r := range recs {
		if err := enc.Encode(ToWire(r)); err != nil {
			return err
		}
	}
	return nil
}

// WriteJSONArray writes all records as a single pretty-printed JSON array.
func WriteJSONArray(w io.Writer, recs []Record) error {
	ws := make([]Wire, len(recs))
	for i, r := range recs {
		ws[i] = ToWire(r)
	}
	b, err := json.MarshalIndent(ws, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// WriteCSV writes a header row then one row per record. Headers are encoded as
// a compact JSON object string in the final column.
func WriteCSV(w io.Writer, recs []Record) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"partition", "offset", "timestamp", "key", "value", "headers"}); err != nil {
		return err
	}
	for _, r := range recs {
		wire := ToWire(r)
		hdr := ""
		if len(wire.Headers) > 0 {
			if b, err := json.Marshal(wire.Headers); err == nil {
				hdr = string(b)
			}
		}
		row := []string{
			strconv.Itoa(int(wire.Partition)),
			strconv.FormatInt(wire.Offset, 10),
			wire.Timestamp,
			wire.Key,
			wire.Value,
			hdr,
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}
