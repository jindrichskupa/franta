package tui

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"franta/internal/record"
)

// producer field indices. prodInputs holds the two single-line fields
// (topic, key); prodHeadersTA and prodValueTA are multi-line textareas for
// headers and value. prodFocus indexes 0..3 across the four fields.
const (
	prodFieldTopic   = 0
	prodFieldKey     = 1
	prodFieldHeaders = 2
	prodFieldValue   = 3
	prodFieldCount   = 4
)

// initProducerFields builds the producer's textinputs and textareas. Called
// from New and from openProducer / openProducerTemplate to reset state.
// Returns (singleLineInputs, headersTA, valueTA).
func initProducerFields() ([]textinput.Model, textarea.Model, textarea.Model) {
	topic := textinput.New()
	topic.Prompt = "topic:   "
	topic.Placeholder = "my-topic"

	key := textinput.New()
	key.Prompt = "key:     "
	key.Placeholder = "(optional)"

	headers := textarea.New()
	headers.Placeholder = `k1=v1
k2=v2

or paste JSON: {"k1":"v1","k2":"v2"}`
	headers.SetWidth(60)
	headers.SetHeight(4)
	headers.Prompt = "│ "
	headers.ShowLineNumbers = false

	value := textarea.New()
	value.Placeholder = `{"hello":"world"}`
	value.SetWidth(60)
	value.SetHeight(8)
	value.Prompt = "│ "
	value.ShowLineNumbers = false

	return []textinput.Model{topic, key}, headers, value
}

// openProducer resets the producer form to empty and switches to modeProducer.
// Returns the updated model.
func (m Model) openProducer() Model {
	ins, hta, vta := initProducerFields()
	m.prodInputs = ins
	m.prodHeadersTA = hta
	m.prodValueTA = vta
	m.prodFocus = prodFieldTopic
	m.prodTemplateFrom = ""
	m.prodInputs[0].Focus()
	m.mode = modeProducer
	return m
}

// openProducerTemplate prefills the form from a record and switches to
// modeProducer with the value field focused. Records the source label so the
// dialog title shows "from <topic>[<part>] off:<off>".
func (m Model) openProducerTemplate(r record.Record) Model {
	ins, hta, vta := initProducerFields()
	ins[prodFieldTopic].SetValue(r.Topic)
	ins[prodFieldKey].SetValue(string(r.Key))
	hta.SetValue(formatHeaders(r.Headers))
	vta.SetValue(r.ValueDisplay())
	m.prodInputs = ins
	m.prodHeadersTA = hta
	m.prodValueTA = vta
	m.prodFocus = prodFieldValue
	m.prodTemplateFrom = fmt.Sprintf("%s[%d] off:%d", r.Topic, r.Partition, r.Offset)
	m.prodValueTA.Focus()
	m.mode = modeProducer
	return m
}

// formatHeaders renders record headers as one "k=v" pair per line for the
// headers textarea.
func formatHeaders(hs []record.Header) string {
	parts := make([]string, 0, len(hs))
	for _, h := range hs {
		parts = append(parts, h.Key+"="+string(h.Value))
	}
	return strings.Join(parts, "\n")
}

// parseHeaders parses a headers string into record.Header values. Three input
// shapes accepted, auto-detected:
//
//   - JSON object: {"k1":"v1","k2":"v2"} — values stringified.
//   - One k=v per line (newline-separated).
//   - Comma-separated k=v pairs: a=1, b=hello world (legacy / single-line).
//
// Empty input yields nil. Whitespace trimmed around k=v pairs. Newline and
// comma separators may be mixed.
func parseHeaders(s string) []record.Header {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	if strings.HasPrefix(s, "{") {
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err == nil {
			out := make([]record.Header, 0, len(m))
			for k, v := range m {
				out = append(out, record.Header{Key: k, Value: []byte(stringifyHeaderValue(v))})
			}
			return out
		}
		// Fall through to k=v parsing if JSON parse failed.
	}
	// Treat newlines and commas equivalently so the multi-line textarea and the
	// legacy comma-separated form both work.
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '\n' || r == ',' })
	out := make([]record.Header, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.IndexByte(p, '=')
		if eq < 0 {
			out = append(out, record.Header{Key: p})
			continue
		}
		out = append(out, record.Header{
			Key:   strings.TrimSpace(p[:eq]),
			Value: []byte(strings.TrimSpace(p[eq+1:])),
		})
	}
	return out
}

// stringifyHeaderValue converts a JSON-decoded value to its string form.
func stringifyHeaderValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// updateProducer handles keys while the producer form is open.
func (m Model) updateProducer(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.blurProducer()
		m.mode = modeNormal
		return m, nil
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyCtrlS:
		// Submit from anywhere (Enter inserts newlines in textareas).
		return m.submitProducer()
	case tea.KeyTab:
		m.focusNext()
		return m, nil
	case tea.KeyShiftTab:
		m.focusPrev()
		return m, nil
	case tea.KeyEnter:
		// Enter submits only when focused on a single-line input. In either
		// textarea (headers, value) it inserts a newline.
		if m.prodFocus != prodFieldHeaders && m.prodFocus != prodFieldValue {
			return m.submitProducer()
		}
	}
	// Forward to the focused field.
	var cmd tea.Cmd
	switch m.prodFocus {
	case prodFieldHeaders:
		m.prodHeadersTA, cmd = m.prodHeadersTA.Update(msg)
	case prodFieldValue:
		m.prodValueTA, cmd = m.prodValueTA.Update(msg)
	default:
		m.prodInputs[m.prodFocus], cmd = m.prodInputs[m.prodFocus].Update(msg)
	}
	return m, cmd
}

func (m Model) submitProducer() (tea.Model, tea.Cmd) {
	r := record.Record{
		Topic:   strings.TrimSpace(m.prodInputs[prodFieldTopic].Value()),
		Key:     []byte(m.prodInputs[prodFieldKey].Value()),
		Value:   []byte(m.prodValueTA.Value()),
		Headers: parseHeaders(m.prodHeadersTA.Value()),
	}
	produce := m.produce
	return m, func() tea.Msg {
		if produce == nil {
			return producedMsg{}
		}
		return producedMsg{err: produce(r)}
	}
}

func (m *Model) focusNext() { m.setFocus((m.prodFocus + 1) % prodFieldCount) }
func (m *Model) focusPrev() { m.setFocus((m.prodFocus - 1 + prodFieldCount) % prodFieldCount) }

func (m *Model) setFocus(i int) {
	for j := range m.prodInputs {
		m.prodInputs[j].Blur()
	}
	m.prodHeadersTA.Blur()
	m.prodValueTA.Blur()
	switch i {
	case prodFieldHeaders:
		m.prodHeadersTA.Focus()
	case prodFieldValue:
		m.prodValueTA.Focus()
	default:
		m.prodInputs[i].Focus()
	}
	m.prodFocus = i
}

func (m *Model) blurProducer() {
	for j := range m.prodInputs {
		m.prodInputs[j].Blur()
	}
	m.prodHeadersTA.Blur()
	m.prodValueTA.Blur()
}

func (m Model) producerView() string {
	// Size dialog ~75% × ~85% of terminal (min 60 cols / 20 lines).
	dlgW := m.width * 75 / 100
	if dlgW < 60 {
		dlgW = 60
	}
	dlgH := m.height * 85 / 100
	if dlgH < 20 {
		dlgH = 20
	}
	innerW := dlgW - 6 // border (2) + padding (2*2)

	// Style focused-field labels in active color; others muted.
	activeLbl := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	mutedLbl := lipgloss.NewStyle().Faint(true)
	for i := range m.prodInputs {
		if i == m.prodFocus {
			m.prodInputs[i].PromptStyle = activeLbl
		} else {
			m.prodInputs[i].PromptStyle = mutedLbl
		}
		m.prodInputs[i].Width = innerW - 12
	}

	// Split remaining vertical room between headers (small) and value (big).
	// Total textarea rows = innerH - title(1) - blanks(3) - 2 single-line rows -
	// 2 textarea labels - footer hint. Headers gets 4 rows; value gets rest.
	totalTA := dlgH - 13
	if totalTA < 8 {
		totalTA = 8
	}
	headerH := 4
	if headerH > totalTA/3 {
		headerH = totalTA / 3
	}
	if headerH < 3 {
		headerH = 3
	}
	valueH := totalTA - headerH
	if valueH < 4 {
		valueH = 4
	}
	m.prodHeadersTA.SetWidth(innerW - 2)
	m.prodHeadersTA.SetHeight(headerH)
	m.prodValueTA.SetWidth(innerW - 2)
	m.prodValueTA.SetHeight(valueH)

	// Title line: mode-aware. Template mode shows source record location.
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	title := titleStyle.Render("Produce a record")
	if m.prodTemplateFrom != "" {
		title = titleStyle.Render("Produce with template") +
			"  " + mutedLbl.Render("from "+m.prodTemplateFrom)
	}

	// Textarea section labels — coloured when their field is focused.
	lbl := func(text string, focused bool) string {
		if focused {
			return activeLbl.Render(text)
		}
		return mutedLbl.Render(text)
	}

	var b strings.Builder
	b.WriteString(title + "\n\n")
	b.WriteString(m.prodInputs[prodFieldTopic].View() + "\n")
	b.WriteString(m.prodInputs[prodFieldKey].View() + "\n\n")
	b.WriteString(lbl("headers:  (one k=v per line, or JSON {\"k\":\"v\"})", m.prodFocus == prodFieldHeaders) + "\n")
	b.WriteString(m.prodHeadersTA.View() + "\n")
	b.WriteString(lbl("value:", m.prodFocus == prodFieldValue) + "\n")
	b.WriteString(m.prodValueTA.View())
	b.WriteString("\n\n")
	hint := "tab/shift-tab next/prev field  •  ctrl+s send  •  enter submits on single-line fields  •  esc cancel"
	b.WriteString(statusStyle.Render(hint))
	if m.status != "" {
		b.WriteString("\n" + m.status)
	}

	dialog := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("205")).
		Padding(1, 2).
		Width(dlgW - 2).
		Render(b.String())
	if m.width > 0 && m.height > 0 {
		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, dialog)
	}
	return dialog
}
