# Ratash Compact TUI Contract

The Status Interface is a compact operations console for inspecting the active Profile, selected
Node, traffic, connections, rules, and logs. Its layout uses dense rows, whitespace, and a small
number of separators so it remains useful at 80×24.

## Shared Screen Frame

The persistent top area has two summary rows, one navigation row, and one separator:

~~~text
● CONNECTED  Profile: Daily              Node: Hong Kong 01                  Traffic: ↓ 18.4 MiB/s  ↑ 2.1 MiB/s
Total: ↓ 28.4 GiB  ↑ 3.2 GiB  Memory: 78.4 MiB  Connections: 428  Mihomo: v1.19.28  Core PID: 4821  Mode: RULE  Mixed: OFF  API: UNIX  TUN: ON
1 Proxies   2 Connections   3 Rules   4 Logs                              : commands
───────────────────────────────────────────────────────────────────────────────────────────────
  page content
───────────────────────────────────────────────────────────────────────────────────────────────
  contextual shortcuts                                                [?] Help        [q] Quit
~~~

The top area owns the useful summary that previously required a separate Overview page:

- connection and runtime health;
- active Profile and selected Node;
- selected Node latency;
- current and cumulative download and upload traffic;
- active connection count and Mihomo memory usage;
- pinned Mihomo version, PID, Rule mode, listener state, private API transport, and TUN state.

The navigation row stays stable across all four pages. The active page uses the cyan accent. A
wide degraded header appends the current recovery reason while retaining the primary metrics.

## Compactness Contract

- Zero outer padding and zero decorative card borders.
- One terminal row per list record.
- Secondary columns disappear by priority as width contracts.
- Profiles and lifecycle actions live in bottom-sheet commands.
- The footer contains only controls relevant to the current focus.
- The minimum supported terminal is 80×24.

## Proxies

Proxies keeps the group-to-node selection flow and removes the separate Node Detail region.

~~~text
PROXY GROUPS        │ Nodes (51) · 良心云                     /          Name  Latency
                    │   NODE                         TYPE         STATUS      LATENCY
▌● 良心云 · Selector│ ▌● Hong Kong 01               VLESS        ready       42 ms
  自动选择 · URLTest│    Singapore 02               Shadowsocks  ready       71 ms
  故障转移 · Fallback│   Japan 03                   Trojan       unavailable -
~~~

Node rows contain only:

- Node name;
- Node type when the viewport is wide enough;
- availability status;
- latency.

The selected runtime Node uses `●` and cyan bold text. `◌` marks a pending selection, and `◉`
marks a current Node with a pending selection operation. The list omits Freshness, Probe, sampled
timestamps, traffic attribution, and a duplicate detail panel.

The group column shows every Profile-defined group and omits Mihomo's compatibility `GLOBAL`
group. Selector groups support manual Node changes. URLTest and Fallback groups remain browsable
and expose the Node chosen by Mihomo.

At 90 columns and above, Proxy Groups and Nodes share the page. Below 90 columns, focus selects
which list occupies the page. `h` and `l` move between the two lists.

## Connections

Connections renders the bounded active connection snapshot reported by Mihomo.

~~~text
CONNECTIONS · 428 ACTIVE · 256 SHOWN
  TARGET                         RULE                     CHAIN                  TRAFFIC
▌ api.openai.com:443             DOMAIN-SUFFIX · openai   Singapore 02 → DIRECT  ↓1.8 MiB ↑142 KiB
  149.154.167.51:443             GEOIP · Telegram        Hong Kong 01 → DIRECT  ↓92 KiB ↑18 KiB
~~~

Each row exposes the data needed to understand routing:

- destination hostname or IP and port;
- matched rule and rule payload;
- proxy chain;
- uploaded and downloaded bytes;
- network type on wide terminals.

The projection keeps at most 256 current records, at most 16 chain entries per record, and bounded
text fields. The title shows both Mihomo's active count and the number retained for display.
Connection age and sample timestamps are omitted.

## Rules

Rules keeps its existing columns, search, editor, and selected-rule summary. Its row cursor follows
keyboard and mouse selection through the viewport.

~~~text
RULES (1284) · REVISION 18
  #     TYPE             VALUE                                      TARGET          STATUS
  0001  DOMAIN-SUFFIX    openai.com                                 AI Services     available
▌ 0002  GEOIP            CN                                         DIRECT          available
~~~

## Logs

Logs defaults to Info-level records from Mihomo's Core API, which exposes routing-match messages in
the same stream used by Clash Verge. Process stdout and stderr stay outside the default TUI view.
Query, level filters, follow/pause behavior, columns, and the selected-record summary remain
available. Manual navigation pins the selected record; follow mode returns to the live tail.

~~~text
LOGS · INFO · LIVE · FOLLOWING · DROPPED 0
  TIME          LEVEL  SOURCE   MESSAGE
▌ 12:42:06.118  INFO   core     [TCP] api.openai.com:443 match DomainSuffix(openai.com) using 良心云
~~~

## Viewport Behavior

Proxies, Connections, Rules, and Logs share one row-navigation contract:

1. The highlight moves inside the visible viewport as `j`, `k`, Up, or Down changes selection.
2. The viewport remains fixed while the next selected row is already visible.
3. The viewport advances by one row only when selection crosses its top or bottom boundary.
4. Mouse-wheel navigation uses the same state transitions as keyboard navigation.
5. Refreshes preserve the selected connection by stable connection ID when it still exists.

Each list owns separate selection and viewport state. This prevents the selected row from being
continually re-anchored to the top of the list.

## Responsive Columns

- Proxies below 65 columns hide Node type.
- Connections below 96 columns show target, rule, and chain.
- Connections from 96 to 124 columns add combined traffic.
- Connections at 125 columns and above show network, download, and upload as separate columns.
- Rules below 100 columns hide target validation status text.

## Interaction Model

Global keys:

~~~text
1–4 page       Tab/Shift+Tab focus       h/l column       j/k row
/ search       : command palette         ? help           q quit
Esc back/close
~~~

Page-specific keys appear in the footer. `p` opens Profiles from Proxies, Connections, and Rules;
on Logs it pauses or resumes the stream. The Profiles sheet shows activation state, Profile name,
and refresh errors while omitting freshness and raw refresh timestamps. Keyboard and mouse resolve
to the same typed intents.

## Data Boundary

The Connections page is an active-state view. Each telemetry refresh replaces the retained list
with the current bounded snapshot. Completed connection history requires a separate history store
and retention policy and is outside this Status Interface contract.
