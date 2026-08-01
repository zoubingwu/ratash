# Hopash RS Compact TUI Prototype

> **PROTOTYPE — review artifact.** This document tests one design question before implementation:
> can Hopash make status, proxy selection, and troubleshooting fast with a border-light layout,
> while remaining useful at 80×24?

## Direction

The prototype combines:

- the proxy-oriented information architecture of
  [mihomo-tui](https://github.com/potoo0/mihomo-tui) and
  [clashctl](https://github.com/George-Miao/clashctl);
- the restrained visual hierarchy and dense tables of
  [RustNet](https://github.com/domcyrus/rustnet);
- the list-to-detail troubleshooting flow of
  [Termshark](https://github.com/gcla/termshark);
- the contextual controls and focus model of
  [K9s](https://github.com/derailed/k9s) and
  [Lazygit](https://github.com/jesseduffield/lazygit);
- the border-free compact mode of
  [Posting](https://posting.sh/guide/#choose-your-preferred-ui-style);
- the latency history ideas of
  [Trippy](https://github.com/fujiapple852/trippy).

The target is a quiet operations console. Its structure comes from one accent color, compact rows,
whitespace, and occasional separators.

## Shared Screen Frame

The persistent chrome consumes three rows at the top and two rows at the bottom, including the
separators. An 80×24 terminal therefore keeps 19 rows for page content.

~~~text
● CONNECTED  Daily  [RULE] [TUN ON] [SYS ON]        ↓ 18.4 MB/s  ↑ 2.1 MB/s  428 conn  42 MB
1 Overview   2 Proxies   3 Connections   4 Rules   5 Logs                    : commands
────────────────────────────────────────────────────────────────────────────────────────────────
  page content
────────────────────────────────────────────────────────────────────────────────────────────────
  contextual shortcuts                                                                     q quit
~~~

Rules:

- The first row is always the strongest visual layer.
- The navigation row stays stable across pages.
- The active page uses a cyan segment in the separator directly below its label.
- Profiles, lifecycle actions, refresh, and other low-frequency operations live in the command palette.
- The footer changes with the focused page and column.
- A disconnected or degraded state replaces the first-row metrics with the recovery reason.
- The green status dot is the only persistent green element.

## Compactness Contract

- Zero outer padding and zero decorative card borders.
- One terminal row per table record; details reuse the remaining viewport.
- Page title, query, sort, freshness, and item count share one row.
- The footer shows at most six actions for the current focus. `?` contains the complete key map.
- Empty sections collapse completely and release their rows.
- Secondary columns disappear by priority as width contracts; primary text and numeric columns stay
  aligned.
- A detail region opens only when it adds information to the selected row. `z` temporarily zooms the
  focused region.

## Overview — 120×30

~~~text
● CONNECTED  Daily  [RULE] [TUN ON] [SYS ON]        ↓ 18.4 MB/s  ↑ 2.1 MB/s  428 conn  42 MB
1 Overview   2 Proxies   3 Connections   4 Rules   5 Logs                    : commands
────────────────────────────────────────────────────────────────────────────────────────────────
OVERVIEW                                                       Core up 1h 32m   sampled 2s ago

TRAFFIC · 60s                                                   RUNTIME
Down  18.4 MB/s   ▁▂▃▅▆▇▅▃▄▆▇▆▅▃▂▃▄▅▆▇▆▅▄▃▂▃▄▅                Mihomo      v1.19.28
Up     2.1 MB/s   ▁▁▂▂▃▄▃▂▂▃▄▃▂▁▁▂▂▃▄▃▂▂▁▁▂▂▃▂                Runtime     generation 42
Total  6.8 GB ↓   842 MB ↑                                     Connections 428

PROFILE                                                        HEALTH
Daily                  fresh · refresh in 5h 12m                Controller  healthy
GLOBAL → Hong Kong 01  42 ms · sampled 8s ago                  Traffic     healthy
Rule set               12 local · 1,284 profile                Logs        healthy

RECENT
12:42:18  INFO   Profile Daily refreshed in 684 ms
12:41:53  INFO   GLOBAL switched to Hong Kong 01
12:39:07  WARN   Singapore 03 probe timed out; retry in 30s
────────────────────────────────────────────────────────────────────────────────────────────────
r refresh   p profiles   Enter inspect health   : command   ? help                           q quit
~~~

The Overview shows only signals needed to answer:

1. Is traffic flowing?
2. Which Profile and Node are active?
3. Is any subsystem degraded?

## Proxies — wide layout at 130 columns and above

~~~text
● CONNECTED  Daily  [RULE] [TUN ON] [SYS ON]        ↓ 18.4 MB/s  ↑ 2.1 MB/s  428 conn  42 MB
1 Overview   2 Proxies   3 Connections   4 Rules   5 Logs                    : commands
──────────────────────────────────────────────────────────────────────────────────────────────────────────────
PROXIES  / filter nodes                 Sort: latency                 51 nodes · sampled 8s ago

GROUPS              │ NODES · GLOBAL                                  │ NODE DETAIL
▌ GLOBAL       Auto │ ▌ [HK] Hong Kong 01       42 ms   8.2 MB/s  31 │ Hong Kong 01
  AI Services   SG  │   [SG] Singapore 02       71 ms   1.4 MB/s   8 │ VLESS · ready
  Streaming     JP  │   [JP] Japan 03          109 ms    820 KB/s  4 │ edge.example:443
  Telegram      HK  │   [US] United States 01  168 ms    116 KB/s  2 │
  Gaming        JP  │   [DE] Germany 01        221 ms          —   0 │ LATENCY · 5m
  Fallback     Auto │   [SG] Singapore 03      timeout          —   0 │ 42 ms  ▁▂▂▃▂▁▂▃▂▂
                    │                                                 │ p50 44 · p95 58
                    │                                                 │
                    │                                                 │ TRAFFIC
                    │                                                 │ ↓ 8.2 MB/s · ↑ 610 KB/s
                    │                                                 │ 31 connections
                    │                                                 │
                    │                                                 │ RECENT ERRORS
                    │                                                 │ none in 30m
──────────────────────────────────────────────────────────────────────────────────────────────────────────────
j/k move   Enter select   / search   d details   ? help                                               q quit
~~~

In the rendered TUI:

- The rail marks focus, and the complete focused row receives a muted cyan background.
- The active Node also carries a text marker so state remains legible in monochrome terminals.
- Region identifiers use stable text badges such as [HK] for reliable terminal width.
- The group column is intentionally narrow. Node selection is the primary task.
- Details update with the cursor while focus remains in the active list.

## Connections — 120×30

~~~text
● CONNECTED  Daily  [RULE] [TUN ON] [SYS ON]        ↓ 18.4 MB/s  ↑ 2.1 MB/s  428 conn  42 MB
1 Overview   2 Proxies   3 Connections   4 Rules   5 Logs                    : commands
────────────────────────────────────────────────────────────────────────────────────────────────
CONNECTIONS  / api.openai                  Filter: all   Sort: rate ↓       LIVE · 428 active

PROCESS        DESTINATION                    RULE                 PROXY          RATE       AGE
▌ Chrome       api.openai.com:443             DOMAIN-SUFFIX        Singapore 02   812 KB/s    32s
  Telegram     149.154.167.51:443             GEOIP                Hong Kong 01    42 KB/s     8m
  Code         github.com:443                 MATCH                Hong Kong 01    18 KB/s    46s
  Slack        wss-primary.slack.com:443       DOMAIN-KEYWORD       Singapore 02    12 KB/s     3m
  Dropbox      client.dropbox.com:443          DOMAIN-SUFFIX        Japan 03         6 KB/s    12m

DETAIL · api.openai.com:443
Process  Chrome (4821)     Network  TCP · TLS     SNI  api.openai.com
Rule     DOMAIN-SUFFIX,openai.com,Singapore      Chain  Singapore 02 → DIRECT
Traffic  1.8 MB ↓ · 142 KB ↑                     Opened 32s ago
────────────────────────────────────────────────────────────────────────────────────────────────
Enter details   / filter   x close   ? help                                                    q quit
~~~

The list remains the primary surface. Details occupy a compact lower region and expand only when
the terminal has enough height. Cursor movement pins the selected snapshot; `Esc` resumes live
ordering.

## Rules — 120×30

~~~text
● CONNECTED  Daily  [RULE] [TUN ON] [SYS ON]        ↓ 18.4 MB/s  ↑ 2.1 MB/s  428 conn  42 MB
1 Overview   2 Proxies   3 Connections   4 Rules   5 Logs                    : commands
────────────────────────────────────────────────────────────────────────────────────────────────
RULES  / openai                         Source: all   12 local · 1,284 profile · revision 18

#      SOURCE    TYPE             VALUE                         TARGET          HITS
0001   local     DOMAIN-SUFFIX    openai.com                    AI Services     146
0002   local     DOMAIN-SUFFIX    anthropic.com                 AI Services      38
▌0003  local     DOMAIN-KEYWORD   telegram                      Telegram        892
0004   profile   GEOIP            CN                            DIRECT        18.2k
0005   profile   MATCH                                          GLOBAL         2.1k

SELECTED
DOMAIN-KEYWORD,telegram,Telegram
Effective position 3 · local revision 18 · target available
────────────────────────────────────────────────────────────────────────────────────────────────
Enter inspect   / search   a add   x remove   ? help                                            q quit
~~~

Destructive mutations require a confirmation line. The Rules page uses an inline editor for a
single-line Rule String.

## Logs — 120×30

~~~text
● CONNECTED  Daily  [RULE] [TUN ON] [SYS ON]        ↓ 18.4 MB/s  ↑ 2.1 MB/s  428 conn  42 MB
1 Overview   2 Proxies   3 Connections   4 Rules   5 Logs                    : commands
────────────────────────────────────────────────────────────────────────────────────────────────
LOGS  / content:openai                  [ALL] DEBUG INFO WARN ERROR   LIVE · FOLLOWING · dropped 0

TIME          LEVEL  SOURCE   MESSAGE
12:42:18.204  INFO   core     Profile Daily refreshed in 684 ms
12:42:06.118  DEBUG  core     api.openai.com:443 matched DOMAIN-SUFFIX → AI Services
12:41:53.922  INFO   wrapper  GLOBAL switched to Hong Kong 01
12:39:07.311  WARN   core     Singapore 03 health check timed out after 5000 ms
▌12:38:51.084 ERROR  wrapper  Log stream reconnected after 2 attempts

SELECTED
Source wrapper · sequence 18,492 · runtime generation 42
Log stream reconnected after 2 attempts
────────────────────────────────────────────────────────────────────────────────────────────────
/ query   p pause   f follow   y copy   ? help                                                 q quit
~~~

The filter expression stays on the same row as the page title. The selected record expands into
two detail rows only when needed. Scrolling pins the current record; `Esc` returns to the live tail.

## Command Palette

The palette is a compact bottom sheet aligned to the screen width with one separator.

~~~text
────────────────────────────────────────────────────────────────────────────────────────────────
: pro
  profile switch        Activate a saved Profile
▌ profile refresh       Refresh the active Profile
  proxy test            Probe Nodes in the active Proxy Group
  restart               Restart the Supervisor
  stop                  Stop the Supervisor and Managed Core
Esc close   ↑/↓ move   Enter run
~~~

Dangerous actions use red only on the confirmation verb. Routine actions keep the cyan accent.

## Responsive Layout

### Wide — 130 columns and above

~~~text
GROUPS            │ NODES                                      │ DETAIL
▌ GLOBAL          │ ▌ Hong Kong 01   42 ms   8.2 MB/s   31     │ latency history
  AI Services     │   Singapore 02   71 ms   1.4 MB/s    8     │ traffic and errors
~~~

### Medium — 90–129 columns

~~~text
GROUPS            │ NODES
▌ GLOBAL          │ ▌ Hong Kong 01   42 ms   8.2 MB/s   31
  AI Services     │   Singapore 02   71 ms   1.4 MB/s    8

Enter selects · d opens a temporary detail drawer
~~~

### Narrow — below 90 columns

~~~text
● UP  Daily  [RULE] [TUN] [SYS]             ↓18.4M  ↑2.1M  428
1 View  2 Pxy  3 Conn  4 Rule  5 Log                     :
───────────────────────────────────────────────────────────────
PROXIES / GLOBAL                                      51 nodes
▌ [HK] Hong Kong 01             42 ms      31 conn
  [SG] Singapore 02             71 ms       8 conn
  [JP] Japan 03                109 ms       4 conn

Esc groups   Enter select   d details   / search
~~~

Narrow mode uses drill-down navigation:

~~~text
Groups → Nodes → Node detail
~~~

Escape always moves one level back before it closes the page-level interaction.

## Visual Rules

- Background: near-black.
- Primary accent: cool cyan.
- Normal text: soft gray; labels use dim gray.
- Green: healthy state only.
- Yellow: degraded, stale, pending, or warning.
- Red: failed state and destructive confirmation only.
- Borders: one header separator, optional column separators, and one footer separator.
- Selection: full-row muted cyan background plus a focus rail.
- Tables: one logical item per terminal row.
- Charts: Overview and Node detail only.
- Dynamic metrics: briefly brighten changed digits while the row remains stable.
- Region labels: short fixed-width text badges.

## Interaction Model

Global keys:

~~~text
1–5 page     Tab/Shift+Tab focus     h/l column     j/k row
/ search     : command palette       ? help         q quit
Esc back/close                         z zoom focus
~~~

The footer displays only actions valid for the current focus. Keyboard and mouse resolve to the
same typed intent. A click focuses or selects a row; the wheel scrolls the hovered list.

Troubleshooting navigation is reversible:

~~~text
Connection → matching Rule → selected Proxy → correlated Logs
           Esc restores the previous page, row, and scroll position
~~~

## Product and Data Impact

This prototype deliberately reaches beyond the current four-page Status Interface.

- The Profiles page moves from primary navigation into the command palette and Overview shortcut.
- Connections requires a bounded connection-list projection with process, destination, rule,
  chain, traffic, and age fields.
- Closing a connection requires a new explicit application operation and confirmation flow.
- Per-Node rate and connection counts require Core telemetry attribution.
- Latency history requires a bounded per-Node time series.
- Rule hit counts require Core-side observation; the existing Local Rule Set already provides
  ordered Rule Strings and mutations.
- System Proxy status requires an explicit domain field and platform boundary.

The visual redesign can land before these new data capabilities. Missing fields collapse their
columns.

## Review Checkpoints

Review these decisions before implementation:

1. Five primary pages with Profiles moved into the command palette.
2. A two-row persistent header and one-row contextual footer.
3. Three-column Proxies at 130+, two columns at 90–129, and drill-down below 90.
4. Connections and Rules as first-class troubleshooting pages.
5. Full-row selection, text region badges, and minimal separators.
