package tui

import (
	"fmt"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"franta/internal/kafka"
)

// clusterMetaMsg carries the result of a DescribeCluster fetch.
type clusterMetaMsg struct {
	meta kafka.ClusterMeta
	err  error
}

// describeClusterCmd fetches cluster metadata + the snapshot topic's partition
// detail off the event loop.
func (m Model) describeClusterCmd(topic string) tea.Cmd {
	fn := m.describeClusterFn
	return func() tea.Msg {
		if fn == nil {
			return clusterMetaMsg{meta: kafka.ClusterMeta{Topic: topic}}
		}
		meta, err := fn(topic)
		return clusterMetaMsg{meta: meta, err: err}
	}
}

// openClusterInfo enters the cluster screen, snapshotting the topic under the
// topics cursor (falling back to the active topic), and kicks off the fetch.
func (m Model) openClusterInfo() (Model, tea.Cmd) {
	if m.describeClusterFn == nil {
		return m, nil
	}
	topic := m.topic // active/consumed topic as fallback
	if name, ok := m.currentTopicName(); ok {
		topic = name
	}
	m.mode = modeClusterInfo
	m.clusterTopic = topic
	m.clusterMeta = nil
	m.clusterErr = ""
	m.clusterBrokerCursor = 0
	m.clusterPartsFocused = false
	return m, m.describeClusterCmd(topic)
}

func (m Model) updateClusterInfo(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyCtrlC:
		return m, tea.Quit
	case tea.KeyEsc:
		m.mode = modeNormal
		return m, nil
	case tea.KeyTab, tea.KeyShiftTab:
		m.clusterPartsFocused = !m.clusterPartsFocused
		return m, nil
	}
	switch msg.String() {
	case "q":
		return m, tea.Quit
	case "1":
		m.clusterPartsFocused = false
		return m, nil
	case "2":
		m.clusterPartsFocused = true
		return m, nil
	case "r":
		m.clusterMeta = nil
		m.clusterErr = ""
		return m, m.describeClusterCmd(m.clusterTopic)
	}
	// Right pane focused → scroll the partition viewport.
	if m.clusterPartsFocused {
		var cmd tea.Cmd
		m.clusterPartsVP, cmd = m.clusterPartsVP.Update(msg)
		return m, cmd
	}
	// Left pane: broker cursor navigation.
	n := 0
	if m.clusterMeta != nil {
		n = len(m.clusterMeta.Brokers)
	}
	switch msg.Type {
	case tea.KeyUp:
		if m.clusterBrokerCursor > 0 {
			m.clusterBrokerCursor--
		}
	case tea.KeyDown:
		if m.clusterBrokerCursor < n-1 {
			m.clusterBrokerCursor++
		}
	case tea.KeyHome:
		m.clusterBrokerCursor = 0
	case tea.KeyEnd:
		if n > 0 {
			m.clusterBrokerCursor = n - 1
		}
	}
	return m, nil
}

// replicationFactor is the replica count of the first partition, or 0 when
// there are no partitions.
func replicationFactor(parts []kafka.PartitionInfo) int {
	if len(parts) == 0 {
		return 0
	}
	return len(parts[0].Replicas)
}

// joinIDs renders a broker-id list as comma-joined decimals.
func joinIDs(ids []int32) string {
	if len(ids) == 0 {
		return "-"
	}
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.FormatInt(int64(id), 10)
	}
	return strings.Join(parts, ",")
}

// renderBrokerList renders one line per broker. The controller row carries a
// trailing ●; the cursor row carries a leading > when the pane is focused.
func renderBrokerList(brokers []kafka.BrokerInfo, controller int32, cursor int, focused bool) string {
	if len(brokers) == 0 {
		return "  (no brokers)"
	}
	var b strings.Builder
	for i, br := range brokers {
		marker := "  "
		if focused && i == cursor {
			marker = "> "
		}
		rack := br.Rack
		if rack == "" {
			rack = "-"
		}
		ctrl := ""
		if br.ID == controller {
			ctrl = " ●"
		}
		if i > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "%s%d %s:%d r:%s%s", marker, br.ID, br.Host, br.Port, rack, ctrl)
	}
	return b.String()
}

// renderPartitionTable renders the partition replication table. Under-replicated
// rows (ISR < replicas) carry a trailing ⚠.
func renderPartitionTable(parts []kafka.PartitionInfo) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-5s %-7s %-12s %s\n", "P", "LEADER", "REPLICAS", "ISR")
	if len(parts) == 0 {
		b.WriteString("(no partitions)")
		return b.String()
	}
	for _, p := range parts {
		warn := ""
		if p.UnderReplicated() {
			warn = " ⚠"
		}
		fmt.Fprintf(&b, "%-5d %-7d %-12s %s%s\n",
			p.Partition, p.Leader, joinIDs(p.Replicas), joinIDs(p.ISR), warn)
	}
	return b.String()
}

// clusterInfoView renders the 2-pane cluster screen: brokers (left) + the
// snapshot topic's partition table (right).
func (m Model) clusterInfoView() string {
	header := statusStyle.Render(m.clusterHeaderLine())

	leftW, rightW, msgsH, detailH := paneSizes(m.width, m.height)
	innerH := msgsH + detailH

	brokersFocused := !m.clusterPartsFocused
	var leftBody string
	switch {
	case m.clusterErr != "":
		leftBody = "failed to load cluster:\n" + m.clusterErr
	case m.clusterMeta == nil:
		leftBody = "loading…"
	default:
		leftBody = renderBrokerList(m.clusterMeta.Brokers, m.clusterMeta.Controller, m.clusterBrokerCursor, brokersFocused)
	}
	leftContent := paneTitle("brokers", brokersFocused) + "\n" + leftBody
	leftBox := paneStyle(brokersFocused).Width(leftW - 2).Height(innerH - 2).Render(leftContent)

	rightTitle := "topic: " + m.clusterTopic
	if m.clusterMeta != nil {
		rightTitle = fmt.Sprintf("topic: %s  (rf:%d parts:%d)",
			m.clusterTopic, replicationFactor(m.clusterMeta.Partitions), len(m.clusterMeta.Partitions))
	}
	rightContent := paneTitle(rightTitle, m.clusterPartsFocused) + "\n" + m.clusterPartsVP.View()
	rightBox := paneStyle(m.clusterPartsFocused).Width(rightW - 2).Height(innerH - 2).Render(rightContent)

	body := lipgloss.JoinHorizontal(lipgloss.Top, leftBox, rightBox)
	hint := "tab/1/2 switch pane  •  ↑/↓ brokers  •  pgup/pgdn scroll parts  •  r reload  •  esc back  •  q quit"
	legend := "● controller   ⚠ under-replicated (ISR<replicas)"
	footer := statusStyle.Render(legend + "  •  " + hint)
	return header + "\n" + body + "\n" + footer
}

func (m Model) clusterHeaderLine() string {
	if m.clusterMeta == nil {
		return "cluster: " + m.cluster
	}
	return fmt.Sprintf("cluster: %s  id:%s  controller:%d  brokers:%d",
		m.cluster, m.clusterMeta.ClusterID, m.clusterMeta.Controller, len(m.clusterMeta.Brokers))
}
