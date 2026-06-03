# franta

A terminal UI for Apache Kafka, inspired by [yozefu](https://github.com/MAIF/yozefu).
Tail topics, filter records with a SQL-ish query language, drill into consumer
groups, and produce messages — with first-class **AWS MSK IAM** auth, Confluent
and Glue **Schema Registry** decoding, and a fast 3-pane TUI.

```
prod-msk / orders
╭───────────────────────────────╮╭─────────────────────────────────────────────────────────────────────────────────────╮
│topics  [sort: name↑]          ││messages                                                                             │
│> audit-log p:24 m:9.8M        ││ PART   OFFSET      KEY                       VALUE                                  │
│  click-stream p:16 m:24.0M    ││ 3      14829113    ORD-9F1A-001              {   "order_id": "ORD-9F1A-001",   "cust│
│  metrics-raw p:32 m:88.0M     ││ 1      14829112    ORD-7B22-007              {   "order_id": "ORD-7B22-007",   "cust│
│  orders p:12 m:1.8M           ││ 0      14829110    ORD-12CD-002              {   "order_id": "ORD-12CD-002",   "cust│
│  payments p:6 m:92.4k         ││ 4      14829104    ORD-44EE-009              {   "order_id": "ORD-44EE-009",   "cust│
│  search-events p:8 m:305.8k   ││                                                                                     │
│  shipments p:3 m:12.0k        ││                                                                                     │
│  users p:1 m:4203             ││                                                                                     │
│                               │╰─────────────────────────────────────────────────────────────────────────────────────╯
│                               │╭─────────────────────────────────────────────────────────────────────────────────────╮
│                               ││detail                                                                               │
│                               ││topic:     orders                                                                    │
│                               ││partition: 3                                                                         │
│                               ││offset:    14829113                                                                  │
│                               ││timestamp: 2026-06-01T09:13:57Z                                                      │
│                               ││key:       ORD-9F1A-001                                                              │
│                               ││headers:                                                                             │
│                               ││  source = web                                                                       │
│                               ││  trace-id = abc123                                                                  │
│                               ││value: { "order_id": "ORD-9F1A-001", ... }                                           │
╰───────────────────────────────╯╰─────────────────────────────────────────────────────────────────────────────────────╯
↑/↓ nav  / f filter  •  P produce-template  •  1/2/3 t tab focus  •  space pause  •  s p g  •  ? help  q quit
```

## Features

- **3-pane layout**: topics (left), messages table (top-right), record detail
  (bottom-right). Tab / `1`/`2`/`3`/`t` switch focus.
- **Active-topic marker**: `●` highlights the topic you're consuming; `>`
  marks the cursor (where Enter would switch). Distinct colours so you never
  lose track of which is which.
- **Live tail** with pause/resume (`space`) — records keep buffering when frozen.
- **Filter DSL** over key / value / `value.<json.path>` / partition / offset /
  timestamp / `header['name']` — `==`, `!=`, `<`, `>`, `<=`, `>=`, `contains`,
  `matches`, combined with `and` / `or` / `not` / `()`. Live parse status, hint
  panel, and match counter on apply.
- **Saved filters**: pre-load named queries from `config.yaml`, save more from
  the TUI with `ctrl+s` in the filter editor (writes to a `filters.yaml`
  side-file so your config comments survive). `F` recalls the picker; `d`
  deletes.
- **Fuzzy topic + group search** (`/`) with bottom-of-screen input, live
  `N/total` counter, persistent `[search: q M/N]` indicator after Enter.
- **Sort cycle** (`o`) on both lists: name↑ / count↓ / parts-or-members↓.
  Indicator in pane title.
- **Progressive load**: the topics + groups lists paint immediately with names
  + partitions / members; per-topic offsets and per-group lag fill in via a
  second-phase batch fetch (generation-guarded so `r` reload supersedes
  in-flight results).
- **Producer dialog**: topic / key / multi-line headers (`k=v` per line *or*
  JSON `{"k":"v"}`) / multi-line value textarea. `P` from a selected record
  pre-fills the form ("produce with template"); `ctrl+s` sends.
- **Consumer groups**: list (left) + lag and members (right). Tab / `1`/`2`
  toggle pane focus; viewport scrolls the detail. Lag column auto-fills.
- **Offset seek** (`s`): `end | beginning | last:N | <duration like 1h> |
  RFC3339`. Per-cluster default via `default_seek` in config. Generation-
  stamped — late records from a superseded position get dropped instead of
  polluting the view.
- **Error dialog**: failures (seek / switch / produce / runtime) raise a
  centred red modal that holds focus until dismissed — they cannot be missed
  flickering past on the footer.
- **Schema Registry decoding** for Confluent (`magic 0x00 + 4-byte schema id`)
  and AWS Glue (`0x03 + compression + 16-byte UUID`):
  - JSON Schema (display passthrough + light validation)
  - Avro (hamba/avro/v2)
  - Protobuf (bufbuild/protocompile + protojson; Confluent message-index and
    Glue lex-sorted FileDescriptor index)
- **Auth**: plaintext, SASL/PLAIN, SASL/SCRAM-SHA-256/512, **AWS MSK IAM**
  (default credential chain, optional region + profile).
- **Headless `consume` mode**: stream filtered records to stdout as JSON.

## Screenshots

### Default 3-pane view

```
prod-msk / orders
╭───────────────────────────────╮╭─────────────────────────────────────────────────────────────────────────────────────╮
│topics  [sort: name↑]          ││messages                                                                             │
│> audit-log p:24 m:9.8M        ││ PART   OFFSET      KEY                       VALUE                                  │
│  click-stream p:16 m:24.0M    ││ 3      14829113    ORD-9F1A-001              {   "order_id": "ORD-9F1A-001",   "cust│
│  metrics-raw p:32 m:88.0M     ││ 1      14829112    ORD-7B22-007              {   "order_id": "ORD-7B22-007",   "cust│
│  orders p:12 m:1.8M           ││ 0      14829110    ORD-12CD-002              {   "order_id": "ORD-12CD-002",   "cust│
│  payments p:6 m:92.4k         ││ 4      14829104    ORD-44EE-009              {   "order_id": "ORD-44EE-009",   "cust│
│  search-events p:8 m:305.8k   ││                                                                                     │
│  shipments p:3 m:12.0k        ││                                                                                     │
│  users p:1 m:4203             ││                                                                                     │
│                               │╰─────────────────────────────────────────────────────────────────────────────────────╯
│                               │╭─────────────────────────────────────────────────────────────────────────────────────╮
│                               ││detail                                                                               │
│                               ││topic:     orders                                                                    │
│                               ││partition: 3                                                                         │
│                               ││offset:    14829113                                                                  │
│                               ││timestamp: 2026-06-01T09:13:57Z                                                      │
│                               ││key:       ORD-9F1A-001                                                              │
│                               ││headers:                                                                             │
│                               ││  source = web                                                                       │
│                               ││  trace-id = abc123                                                                  │
╰───────────────────────────────╯╰─────────────────────────────────────────────────────────────────────────────────────╯
↑/↓ nav  / f filter  •  P produce-template  •  1/2/3 t tab focus  •  space pause  •  s p g  •  ? help  q quit
```

### Topic fuzzy search

```
prod-msk / orders
... (panes elided) ...
[search: audi  1/8] ↑/↓ nav  enter switch  / search  o sort  i internal  r reload  •  space pause  •  s p P g  •  ? help  q quit
```

The bottom-of-screen input filters live; after Enter, the persistent
`[search: …  N/total]` chip stays in the footer until cleared.

### Filter DSL editor

```
filter> header['source'] == "web" and value.amount >= 100   ✓ ok
fields: key, value, value.<json.path>, partition, offset, timestamp, header['name']
ops:    == != < > <= >= contains matches    and / or / not / ()
ex:     header['x-trace-id'] == "abc"   |   value.amount >= 100   |   key contains "user-"
enter apply  •  esc cancel  •  empty input clears the filter
```

Parse status (`✓ ok` / `✗ <error>`) updates as you type; Enter shows the new
match count in the footer status line.

### Producer dialog (template from selected record)

```
╭────────────────────────────────────────────────────────────────────────────────────────╮
│  Produce with template  from orders[3] off:14829113                                    │
│                                                                                        │
│  topic:   orders                                                                       │
│  key:     ORD-9F1A-001                                                                 │
│                                                                                        │
│  headers:  (one k=v per line, or JSON {"k":"v"})                                       │
│  │ source=web                                                                          │
│  │ trace-id=abc123                                                                     │
│                                                                                        │
│  value:                                                                                │
│  │ {                                                                                   │
│  │   "order_id": "ORD-9F1A-001",                                                       │
│  │   "customer": "u42",                                                                │
│  │   "amount": 129.95,                                                                 │
│  │   "items": 3,                                                                       │
│  │   "status": "PAID"                                                                  │
│  │ }                                                                                   │
│                                                                                        │
│  tab/shift-tab next/prev field  •  ctrl+s send  •  enter submits on single-line fields │
╰────────────────────────────────────────────────────────────────────────────────────────╯
```

### Consumer groups

```
consumer groups — prod-msk
╭───────────────────────────────╮╭─────────────────────────────────────────────────────────────────────────────────────╮
│groups  [sort: name↑]          ││detail                                                                               │
│  analyti…  S  m:12  lag:9.2M  ││State:     Stable                                                                    │
│> compact…  E  m:0  lag:?      ││Total lag: 142                                                                       │
│  fraud-d…  S  m:3  lag:142    ││                                                                                     │
│  legacy-…  D  m:0  lag:?      ││Lag:                                                                                 │
│  metrics…  S  m:4  lag:0      ││  orders[0]  committed:14829101  end:14829113  lag:12                                │
│  orders-…  S  m:6  lag:0      ││  orders[1]  committed:14829080  end:14829112  lag:32                                │
│  shipmen…  S  m:2  lag:17     ││  orders[2]  committed:14829007  end:14829105  lag:98                                │
│                               ││                                                                                     │
│                               ││Members:                                                                             │
│                               ││  fd-1-7af2  client:fd-1  host:/10.0.4.12                                            │
│                               ││    assigned: orders:0, orders:1                                                     │
│                               ││  fd-2-9bb1  client:fd-2  host:/10.0.4.18                                            │
│                               ││    assigned: orders:2                                                               │
╰───────────────────────────────╯╰─────────────────────────────────────────────────────────────────────────────────────╯
↑/↓ pgup/pgdn home/end nav  •  tab/1/2 switch pane  •  / search  •  o sort  •  r reload  •  esc back  •  q quit
```

State legend: `S`=Stable, `E`=Empty, `D`=Dead, `P`=PreparingRebalance,
`R`=Rebalancing. `lag:?` means the group's lag wasn't fetched yet (or the
fetch errored for that group); cursor over a row triggers an on-demand
describe and refreshes the value.

> Screenshots are plain-text dumps of the live `View()` output. Regenerate with
> `go test -tags screenshots -run TestGenerateScreenshots ./internal/tui/` — they
> land in `docs/screenshots/*.txt`.

## Install

```bash
go install ./cmd/franta
# or:
go build -o franta ./cmd/franta
```

Requires Go 1.22+.

## Configure

Copy `config.example.yaml` to `~/.config/franta/config.yaml` and edit. Each
cluster sets `brokers`, optional `tls`, an `auth` block, and an optional
`schema_registry` block:

```yaml
default_cluster: prod-msk

clusters:
  local:
    brokers: ["localhost:9092"]
    auth: { type: plaintext }

  prod-msk:
    brokers: ["b-1.prod.msk.amazonaws.com:9098", "b-2.prod.msk.amazonaws.com:9098"]
    tls: { enabled: true }
    auth:
      type: iam
      region: eu-central-1
      # profile: my-aws-profile     # optional; default chain otherwise
    # Used when --from is not on the CLI. Same mini-syntax as the CLI flag.
    # Falls back to "end" if unset.
    default_seek: last:500
    schema_registry:
      type: glue                    # or: confluent
      region: eu-central-1
      registry_name: default-registry
      use_for: both                 # key | value | both
      protobuf:
        single_file: schemas/orders.proto
        well_known_paths: ["/usr/include"]

# Optional. Read-only inline filter library. Use ctrl+s in the TUI filter editor
# to save more; those land in filters.yaml next to this file so these comments
# are preserved. F recalls.
saved_filters:
  - name: errors
    query: 'header[''severity''] == "ERROR" or value.error contains "FAIL"'
  - name: paid-orders
    query: 'value.status == "PAID" and value.amount >= 100'
```

Auth types:

- `plaintext` — no auth
- `plain` — SASL/PLAIN (`username` + `password` or `password_env`)
- `scram` — SASL/SCRAM (`mechanism: SCRAM-SHA-256 | SCRAM-SHA-512`)
- `iam` — AWS MSK IAM (optional `region`, `profile`; uses the AWS default
  credential chain)

Schema Registry kinds: `confluent` (URL + optional basic auth) or `glue`
(AWS region + registry name; uses the IAM auth chain).

### Saved filters

Two sources, merged on load (side-file wins on name collision):

- **`config.yaml`** — top-level `saved_filters:` list. Read-only; safe place
  for filters you want to version with your config.
- **`filters.yaml`** — sits next to `config.yaml`. Owned by the app: the
  `ctrl+s` save action and `d` delete action in the TUI rewrite this file
  atomically. Your `config.yaml` comments are never touched.

In the TUI: `f` (or `/` in the messages pane) opens the filter editor.
`ctrl+s` inside the editor prompts for a name and persists the current
query. `F` opens the saved-filter picker (enter applies, `d` deletes, `esc`
cancels).

## Usage

### Tail a topic

```bash
franta                                # picks default_cluster, opens topic picker
franta my-topic                       # uses default_cluster
franta --cluster prod-msk my-topic
franta --from beginning my-topic      # start from earliest offset
franta --from last:1000 my-topic      # last 1000 records per partition
franta --from 1h my-topic             # records from the past hour
franta --from 2026-05-27T00:00:00Z my-topic  # absolute timestamp
```

### Headless consume (JSON to stdout)

```bash
franta consume --cluster local my-topic
franta consume --cluster local --filter 'value.type == "order" and partition == 0' my-topic
franta consume --cluster local        # interactive topic picker
```

## Keys

### Global

| Key             | Action                                         |
|-----------------|------------------------------------------------|
| `tab` / shift+tab | Cycle pane focus                            |
| `1` / `2` / `3` / `t` | Focus topics / messages / detail        |
| `space`         | Pause / resume tailing                         |
| `s`             | Open seek prompt (`end / beginning / last:N / 1h / RFC3339`) |
| `f`             | Open DSL filter from any pane (alias for messages `/`) |
| `F`             | Recall a saved filter (picker; `d` deletes)    |
| `p` / `P`       | Open producer dialog / produce with template from selected record |
| `g`             | Open consumer groups view                      |
| `?`             | Toggle help overlay                            |
| `esc`           | Cancel a prompt / close a modal / dismiss the error dialog |
| `q` / `ctrl+c`  | Quit                                           |

### Topics pane

| Key | Action                                                          |
|-----|-----------------------------------------------------------------|
| `↑` `↓` `pgup` `pgdn` `home` `end` | Navigate                       |
| `enter` | Switch consumption to highlighted topic                     |
| `/`     | Fuzzy-search the topic list                                 |
| `o`     | Cycle sort: name↑ / msgs↓ / parts↓                          |
| `i`     | Toggle internal topics                                      |
| `r`     | Reload topic list                                           |

### Messages pane

| Key | Action                                              |
|-----|-----------------------------------------------------|
| `↑` `↓` `pgup` `pgdn` | Move cursor                       |
| `/` or `f`            | Open DSL filter                   |
| `F`                   | Saved-filter picker                |

### Filter editor (open via `/` or `f`)

| Key       | Action                                             |
|-----------|----------------------------------------------------|
| `enter`   | Apply (status shows match count)                   |
| `ctrl+s`  | Save the current query under a name → `filters.yaml` |
| `esc`     | Cancel                                             |

### Consumer groups view

| Key | Action                                                          |
|-----|-----------------------------------------------------------------|
| `tab` / `1` / `2` | Switch focus between list and detail               |
| `↑` `↓` `pgup` `pgdn` `home` `end` | Navigate list / scroll detail   |
| `/`              | Fuzzy-search the group list                        |
| `o`              | Cycle sort: name↑ / lag↓ / members↓                |
| `r`              | Reload (refetches list + describes for the cursor) |
| `esc`            | Back to the main view (first `esc` clears an active filter) |

### Producer dialog

| Key | Action                                              |
|-----|-----------------------------------------------------|
| `tab` / shift+tab | Next / prev field                  |
| `ctrl+s`          | Send                              |
| `enter`           | Submit on single-line fields; newline in textareas |
| `esc`             | Cancel                            |

## Filter DSL

```
partition == 0
offset >= 1000
key contains "user-"
key matches "^id-[0-9]+$"
value.payload.status == "ACTIVE"
value.amount >= 100
header['source'] == "web"
header['x-trace-id'] matches "[0-9a-f]{16}"
timestamp >= "2026-05-27T00:00:00Z"
not (partition == 3) and value.ok == "true"
```

Fields: `key`, `value`, `value.<json.path>`, `partition`, `offset`,
`timestamp`, `header['name']`. Operators: `== != < > <= >= contains matches`,
combined with `and`, `or`, `not`, and parentheses.

Literals are quoted strings (`"x"`) or numbers (`42`, `1.5`). The literal type
must match the field: numeric fields (`partition`, `offset`) and numeric JSON
values take number literals; string fields and string/boolean JSON values take
quoted literals (booleans compare as `"true"` / `"false"`). `timestamp` accepts
an RFC3339 string or epoch-millis number. `matches` uses RE2 (Go `regexp`).

## Development

### Pre-commit hook

```bash
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
```

Runs gofmt, `go vet`, `go build`, `go test`, and `golangci-lint` against
staged `.go` files. Skip once with `git commit --no-verify`.

```bash
go test ./...                                              # unit tests
go test -tags=integration ./...                            # round-trip via redpanda (needs Docker)
go test -tags=screenshots -run TestGenerateScreenshots ./internal/tui/   # regenerate screenshots
gofmt -w internal/ cmd/
```

Layout:

- `cmd/franta` — CLI entry point (tail + headless `consume` + topic picker).
- `internal/tui` — Bubble Tea model, panes, prompts, producer / groups views.
- `internal/kafka` — franz-go wrapper: consumer, admin, groups, seek, producer.
- `internal/kafka/auth` — SASL / IAM auth plumbing.
- `internal/query` — filter DSL parser + evaluator.
- `internal/decode` — Schema Registry decoders (Confluent + Glue, JSON / Avro / Protobuf).
- `internal/record` — wire-format-agnostic record + ring buffer.
- `internal/config` — YAML config loader.

## License

MIT.
