<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Render

The skill produces a **maintainer dashboard** as the primary output: an HTML page with five colour-coded sections at the top, a time-trend line chart, and the full per-area tables collapsed underneath. The dashboard is designed to answer "what should I do today" at a glance, with the underlying numbers one click away.

A Rich-rendered terminal-tables variant and a Markdown fallback are produced when the maintainer asks for them (`tables-only` or `markdown` flag, or output is being piped to a non-tty). Those modes render only the per-area tables — they skip the hero cards, recommendation panel, charts, and pressure ranking, since those rely on visual layout that doesn't translate to a terminal.

---

## Why HTML is the default

The previous skill iteration emitted Rich tables as the primary output. Two empirical problems pushed the dashboard to HTML:

1. **The signal is in the recommendations, not the tables.** Maintainers asked variations of "what should I do" — a 17-column table requires the maintainer to visually scan, compare, and infer. The dashboard surfaces the same conclusions explicitly with priority colours.
2. **Trends need pictures.** Closure velocity over time and opened-vs-closed momentum are intuitive as bar / line charts and dense as numeric columns. SVG inline charts solve both.

Rich's per-cell colour markup is still useful for the *details* tables (which the dashboard embeds in collapsed `<details>` blocks), so the colour vocabulary below is shared between HTML and Rich.

---

## Dashboard layout

The HTML page renders sections in this exact order. Section headings and their meanings are stable across runs so a maintainer can scroll-jump consistently.

### 1. Title bar + context line

```
📊 apache/airflow — Maintainer dashboard
Tuesday, May 6, 2026 · 14:33 UTC · viewer @potiuk · 6-week window since 2026-03-24
```

The title is plain text. Context line includes `<weekday>, <month> <day>, <year> · <HH:MM> UTC · viewer @<login> · 6-week window since <cutoff>`. Use the fetch-start `<now>`, not render-end, so a slow run remains a single-moment snapshot.

### 2. Hero cards (4-column grid)

Four equally-sized cards, each one big number with a sub-label:

| Card | Big number | Sub-label | Colour rule |
|---|---|---|---|
| **Repo Health** | the rating label (`✅ Healthy` / `⚠️ Needs attention` / `🔥 Action needed`) | "based on triage backlog + queue size" | green / amber / red, per [`aggregate.md#health-rating`](aggregate.md#health-rating) |
| **Open PRs (non-bot)** | total open count | `<contrib_count> from contributors · <collab_count> collaborator-authored` | blue (informational) |
| **Ready for review** | `len(ready_open)` | `<pct>% of contributor queue` | green |
| **Untriaged non-drafts** | `len(untriaged_nondraft)` | `<X> are >4 weeks old` | red if >0 are >4w, amber if total > 30, green otherwise |

Card layout is responsive: 4-column on wide screens, 2-column on narrow, 1-column on mobile-width. The big number uses 32px font; sub-labels are 12px dim grey.

### 3. What needs attention (action panel)

A vertical list of action cards built from the recommendation rules in [`#recommendation-rules`](#recommendation-rules) below. Each card has a coloured left border (red = high, amber = medium, grey = low), an icon, a one-line title, a 1–2-line detail explanation, and (when applicable) a monospace `code` block holding the exact slash-command the maintainer can paste. When a rule's `action` is `—` (no paste-clean command applies), the card omits the code block and shows title + detail only.

If zero rules fire, render a single low-priority card with a `✨` icon and the body "No urgent actions detected. Queue is in healthy shape — periodic /pr-triage when convenient." Never leave the section visually empty.

The order inside the panel is: high-priority first (sorted by count descending), then medium (same), then low. Within a tier the rule firing order from [`#recommendation-rules`](#recommendation-rules) breaks ties.

### 4. Closure velocity (per-week bar chart)

Title: **Closures per week (oldest → newest)**

A 6-row stacked horizontal bar chart, one row per week (W-5, W-4, W-3, W-2, W-1, this wk).

Each bar is two stacked segments:

- **green** — `merged` count
- **grey** — `closed` count (closed without merging)

Bar width = `(merged + closed) / max_weekly_total * 100%`. Numbers inside each segment when wide enough; otherwise to the right of the bar.

Below the bars: a one-line summary of `6-week total: N · avg N/wk · peak N/wk`.

This panel reads as "how much did we ship per week, and is that trending up or down". Use the labelled date (`05-06`) on the left axis so the maintainer sees the actual calendar weeks, not abstract `W-N` labels.

### 5. Opened vs closed momentum (line chart)

Title: **Opened vs closed PRs (last 6 weeks)**

An inline SVG line chart with two lines:

- **blue line** — count of PRs *opened* per week (createdAt in the window)
- **green line** — count of PRs *closed/merged* per week (closedAt in the window, includes both merge and close-without-merge)

A horizontal grid line at 0 and at the max value. Y-axis labels (3 ticks: 0, mid, max). X-axis labels: week-start dates.

Below the chart, a small "**Net delta**" line:

```
Net delta this week: +12 PRs (102 opened - 90 closed)
6-week net: -45 PRs (1450 opened - 1495 closed) — backlog shrinking
```

The two-line mini-summary translates the chart into the maintainer-relevant question: "is the open-PR backlog growing or shrinking?". Negative net = backlog shrinking (good); positive net = growing (need more close-out velocity).

The line chart is kept inline-SVG (no JavaScript) so it renders in any browser, in a Slack image preview, or in any embedded HTML viewer. Keep the chart 720×220px so it's readable but doesn't dominate the page.

### 6. Ready-for-review trend (multi-line chart)

Title: **Ready-for-review trend (last 6 weeks, top areas)**

An inline SVG line chart with one line per top-pressure area (default: top 5, filtered to areas with ≥ 3 currently-ready PRs). Each line is **cumulative**: at week W it shows the count of PRs that *currently* carry the `ready for maintainer review` label and were labelled on or before W.

Each area's line uses its pressure-band colour (red, amber, or grey per [`aggregate.md#pressure-score`](aggregate.md#pressure-score)). Same SVG dimensions as the opened-vs-closed chart (720×220px). The chart legend in the top-left lists each area name with its line colour swatch.

Below the chart, a per-area summary list:

```text
providers: 46 ready (+8 in last 7d)
task-sdk: 40 ready (+5 in last 7d)
…
```

The "+N in last 7d" surfaces whether the queue is growing faster than maintainers are reviewing — a steadily climbing line means review velocity isn't keeping up.

### 7. Closed by triage reason (stacked bar chart)

Title: **Closed by triage reason (last 6 weeks)**

Six rows, oldest → newest, one row per week. Each row is a horizontally-stacked bar with four coloured segments:

| Segment | Definition | Colour |
|---|---|---|
| **merged** | `merged == true` | green |
| **closed-after-responded** | not merged, was triaged, author responded before close | amber |
| **closed-after-triage-no-response** | not merged, was triaged, author never responded (sweep close) | red |
| **closed-no-triage** | not merged, never triaged (author self-close, etc.) | grey |

Bar width per row = `(sum of all four) / max_weekly_total * 100%`. Numbers inside each segment when wide enough; otherwise to the right of the bar.

Below the bars, a one-line breakdown of the 6-week totals:

```text
6-week breakdown: <merged> merged · <closed-after-responded> engaged-then-closed · <closed-no-response> sweep-closed · <closed-no-triage> no-triage
```

A healthy week is mostly green with thin amber/red segments. A red-dominated week means a stale-sweep landed; a grey-dominated week means contributors are self-cleaning (also healthy, but worth noticing).

### 8. Pressure by area

Title: **Pressure by area** (with a sub-line *"Pressure score = weighted sum of untriaged-old PRs per area. Higher score = more maintainer attention needed."*)

Up to 8 rows, sorted by pressure score descending (filtering areas with < 3 contributor PRs). Each row is a horizontally-laid-out card with a coloured left border (red / amber / grey, severity bands per [`aggregate.md#pressure-score`](aggregate.md#pressure-score)) containing:

- area name (cyan, bold, e.g. `providers`)
- one-line stat: `<contrib_total> contributor PRs · <red>untriaged_4w</red> >4w · <amber>untriaged_1_4w</amber> 1-4w · <grey>untriaged_recent</grey> recent · <green>ready_pending</green> ready for review`
- pressure score (right-aligned)
- the slash-command to focus on this area: `/pr-triage label:area:<X>` (dimmed)

This panel answers "if I have 30 minutes, which area moves the most needles?". Top row is always the highest-leverage focus.

### 9. Triage funnel (4-column hero grid)

A second hero grid, same layout as the top one, showing the funnel-health summary numbers:

| Card | Big number | Sub-label | Colour rule |
|---|---|---|---|
| **Triage Coverage** | `<pct>%` of contributor PRs that have been seen by a maintainer (triaged + ready + draft / contributors) | `<seen> of <total> contributor PRs have been seen by a maintainer` | green ≥ 50, amber 20–49, red < 20 |
| **Author Response Rate** | `<pct>%` of triaged PRs where the author replied | `<responded> of <triaged> triaged PRs got an author reply` | same colour rule |
| **Stalest Bucket** | count of contributor PRs in the `>4w` age bucket | "contributor PRs untouched >4 weeks" | red if > 50, amber if > 20, green otherwise |
| **This Week's Velocity** | this week's `merged + closed` total | `<merged> merged · <closed> closed (avg <N>/wk)` | default (informational) |

This grid completes the dashboard: hero cards at the top (queue size + immediate red flags), recommendations next (what to do), velocity + opened-vs-closed (momentum), pressure by area (where), and triage funnel (process health).

### 10. Detailed tables (collapsed `<details>` blocks)

Two `<details>` elements, each opening into a compact area-grouped table:

- **Triaged PRs — Final State since `<cutoff>`** — same structure as the [Table 1](#table-1--triaged-prs-final-state) section below.
- **Triaged PRs — Still Open** — same structure as [Table 2](#table-2--triaged-prs-still-open) below, possibly compact (drop the 8 age-bucket columns) for screen width.

Both tables are HTML-rendered with the same colour scheme as the dashboard. Maintainers who want the raw per-area numbers click to expand; the default view is the dashboard sections above.

### 11. Legend / methodology

A bordered panel at the bottom explaining all the colours, columns, and computed values. Critical content because the dashboard packs a lot of distinct numbers into small footprints. See [`#legend`](#legend) below for the verbatim block.

---

## Recommendation rules

The "What needs attention" panel is built from this fixed rule set, evaluated in order. Each rule that fires produces one entry in the panel.

`Action` and `Detail` are separate columns by design: `Action` is a literal paste-clean slash-command the maintainer runs (or `—` when no command applies); `Detail` is the prose explanation that goes in the card body. Mixing prose into `Action` would make the slash-command non-paste-clean and re-introduce the editorialising the skill is supposed to avoid.

| # | Trigger | Priority | Icon | Title template | Detail template | Action |
|---|---|---|---|---|---|---|
| 1 | `len(untriaged_old) > 0` (any contributor non-draft >4w) | high | 🔥 | `Triage <N> non-draft contributor PRs older than 4 weeks` | Focus on the >4w bucket — those are the ones rotting longest. | `/pr-triage all PR issues` |
| 2 | `len(untriaged_old) == 0 AND len(untriaged_med) > 0` (1-4w bucket non-empty) | medium | 👀 | `Triage <N> non-draft PRs aged 1-4 weeks` | The 1–4w bucket is the queue's leading edge; staying on top of it stops PRs from rolling into >4w. | `/pr-triage all PR issues` |
| 3 | `len(stale_triaged_drafts) > 0` (drafts triaged ≥ 7d ago, no reply) | medium | 🗑️ | `Close <N> stale-triaged drafts (≥7d, no response)` | Closure path lives under the `stale` flow (sweep step 1a). | `/pr-triage stale` |
| 4 | `len(ready_open) >= 50` | high | 📥 | `<N> PRs labeled "ready for maintainer review"` | The `ready for maintainer review` queue is past the triage stage; it needs maintainer review attention, not triage. | `/maintainer-review ready` |
| 5 | `20 <= len(ready_open) < 50` | medium | 📥 | `<N> PRs in "ready for maintainer review" queue` | Same trigger family as rule 4 — banded by queue size so the priority drops once the queue is comfortable. | `/maintainer-review ready` |
| 6 | `len(responded_no_ready) > 0` (triaged + responded but not ready-for-review) | medium | 🔄 | `<N> triaged PRs have author responses awaiting re-triage` | These will surface as mark-ready-with-ping inside the regular triage sweep. | `/pr-triage all PR issues` |
| 7 | top area's `untriaged_4w + untriaged_1_4w >= 5` | medium | 📍 | `Area "<area>" has <total> contributor PRs (<X> untriaged >4w)` | One area is dominating the untriaged queue; scoping a triage pass to it clears the bulk of the load. | `/pr-triage label:area:<area>` |
| 8 | `velocity_drop > 30` (last_wk total - this_wk total) | low | 📉 | `PR closure velocity dropped <N> this week` | No immediate action — re-check next week to see if the drop persists or was a one-off. | — |
| 9 | top ready-trend area's growth in last 7d ≥ 10 PRs | low | 📈 | `Ready-for-review queue in "<area>" grew by <N> this week` | Growth concentrated in one area suggests it'd benefit from a focused review pass. | `/maintainer-review label:area:<area>` |
| 10 | weekly closed-by-reason `closed_no_response > merged` for 2+ recent weeks | medium | 🧹 | `Stale-sweep is dominating closures (last 2 weeks: <N> sweep-close vs <M> merged)` | Too many PRs are reaching the stale sweep — review the `/pr-triage stale` cadence and whether earlier-stage interventions (mark-ready, ping) are firing. | — |

Rules 1 and 2 are **mutually exclusive** (only one fires depending on whether any >4w PRs exist). Rules 4 and 5 are **mutually exclusive** (banding on `ready_open` count). All other rules can fire independently.

When adding a new rule:

- Prefer count-based triggers over percentage-based ones (counts are easier to reason about for the maintainer).
- The `Action` cell must be a literal slash-command the maintainer can paste, with no parentheticals, prose, or unicode arrows. If the rule has no paste-clean command (e.g. "wait and re-check"), set `Action` to `—` and put the explanation in `Detail`.
- The `Detail` cell explains *why* the rule fired and any context the maintainer needs to act on it; this is where parentheticals and cross-references live.

---

## Colour scheme

Shared between the HTML dashboard and the Rich terminal-tables fallback. HTML uses CSS hex values; Rich uses its semantic colour names. Both palettes are derived from the now-removed `breeze pr stats` / `breeze pr auto-triage` commands so state-meaning carries over from the old CLI.

| Concept | HTML (hex) | Rich | Used in |
|---|---|---|---|
| Area name | `#56d4dd` (cyan) | `bold cyan` | Area column of all tables, area cards in pressure section |
| Triage / Waiting for Author | `#d29922` (amber) | `yellow` | `Triaged` count, `%Responded < 50%`, "Needs attention" health label |
| Responded | `#56d364` (green) | `green` | `Responded` column, `%Responded ≥ 50%`, "Healthy" health label |
| Ready for review | `#56d364` (green, bold) | `bold green` | `Ready` count, `%Ready` columns, "merged" Table 1 column |
| Flagged / failing | `#f85149` (red) | `red` | `Closed` (without merge) column, "Action needed" health label, > 4w untriaged callouts |
| Drafted by triager | `#db61a2` (magenta) | `magenta` | `Drafted by triager` column |
| Drafted age buckets (secondary) | `#6f3a55` (dim magenta) | `magenta dim` | `Drafted <1d` … `Drafted >4w` |
| Author-resp age buckets (secondary) | `#6e7681` (dim) | `dim` | `Author resp <1d` … `Author resp >4w` |
| Contributor (non-collab) | `#76e3ea` (bright cyan) | `bright_cyan` | `Contrib.`, `%Contrib.`, "Open PRs" hero card |
| Informational (counts) | `#76e3ea` (bright cyan) | `bright_cyan` | "Open PRs" hero, neutral big numbers |
| Unknown / ambiguous | `#6e7681` (dim) | `dim` | `?` / `—` cells, low-priority recommendations |
| TOTAL row | `#f0f6fc` on `#21262d` (white on dark grey) | `bold white on grey7` | Footer-before-footer TOTAL row |
| **Velocity bars: merged segment** | `#56d364` (green) | n/a | Per-week bar chart, merged portion |
| **Velocity bars: closed segment** | `#6e7681` (grey) | n/a | Per-week bar chart, closed-without-merge portion |
| **Line chart: opened line** | `#58a6ff` (blue) | n/a | Opened-vs-closed momentum chart |
| **Line chart: closed line** | `#56d364` (green) | n/a | Opened-vs-closed momentum chart |
| **Ready-trend: high-pressure area line** | `#f85149` (red) | n/a | Ready-for-review trend chart, areas with pressure ≥ 30 |
| **Ready-trend: medium-pressure area line** | `#d29922` (amber) | n/a | Ready-for-review trend chart, areas with pressure 15-29 |
| **Ready-trend: low-pressure area line** | `#6e7681` (grey) | n/a | Ready-for-review trend chart, areas with pressure < 15 |
| **Closed-by-reason: merged segment** | `#56d364` (green) | n/a | Closed-by-triage-reason stacked bar chart |
| **Closed-by-reason: closed-after-responded** | `#d29922` (amber) | n/a | Closed-by-triage-reason stacked bar chart |
| **Closed-by-reason: closed-no-response** | `#f85149` (red) | n/a | Closed-by-triage-reason stacked bar chart |
| **Closed-by-reason: closed-no-triage** | `#6e7681` (grey) | n/a | Closed-by-triage-reason stacked bar chart |
| **Recommendation priority — high** | `#f85149` (red border) | n/a | Action card left border |
| **Recommendation priority — medium** | `#d29922` (amber border) | n/a | Action card left border |
| **Recommendation priority — low** | `#6e7681` (grey border) | n/a | Action card left border |

When a percentage is on a colour boundary (e.g. `%Responded` exactly 50%), keep the happier colour — green ≥ 50, amber 20-49, red < 20. The "when in doubt show it as still-ok" convention matches the predecessor breeze commands.

---

## Triage state markers

Distinct from the colour scheme: these are the four conceptual *states* a contributor PR can be in at any moment. The dashboard uses them in the "Triage funnel" hero grid and in the recommendation triggers.

| Marker | Definition | Colour | Maintainer action |
|---|---|---|---|
| `Ready for review` | `ready for maintainer review` label is present | green | run `/maintainer-review` to actually code-review the PR |
| `Responded` | PR is triaged AND author has commented or pushed after the triage comment, AND not yet `Ready` | bright cyan | re-triage; may now qualify for `mark-ready-with-ping` |
| `Waiting for Author` | PR is triaged, no author response — OR — PR is a draft (whether triaged or not) | amber | nothing; author owns the next move (may become a sweep candidate after 7d) |
| `Not yet triaged` | None of the above. Non-draft PR that has never received a quality-criteria comment | blue (or grey) | run `/pr-triage` to give it a first look |

Counting rules are precedence-based — `Ready` takes precedence over the others, then `Responded`, then `Waiting`, with `Not yet triaged` as the fallback. So a single PR is in exactly one bucket; the four counts must sum to the total open non-bot PR count.

---

## Context line

Plain text, before the first hero card:

```text
apache/airflow — 459 open PRs (non-bot) · closed/merged since 2026-03-24 · viewer @potiuk · 2026-05-06 14:33 UTC
```

Structure: `<repo> — <open_count> open PRs (non-bot) · closed/merged since <cutoff> · viewer @<login> · <now>`.

If the closed-since counts came from the lagging search index (see [`fetch.md#known-limitation`](fetch.md#known-limitation)), prepend a one-line caveat before the hero cards:

```text
⚠ Closed-PR table built from GitHub's free-text search of the quality-criteria marker. The index lags — older triaged+merged PRs are likely undercounted. Pass accurate-closed for the hybrid REST + GraphQL path.
```

---

## Table 1 — Triaged PRs, final state

Lives inside the collapsed `<details>` block at the bottom of the dashboard. Title: `Triaged PRs — Final State since <cutoff> (<repo>)`.

One row per area where `triaged_total > 0`, sorted by `triaged_total` descending. `(no area)` goes last. Append a bold **TOTAL** row.

| Column | Source | Colour |
|---|---|---|
| Area | area name without the `area:` prefix | cyan |
| Triaged Total | `triaged_total` | amber |
| Closed | `closed` | red |
| %Closed | `pct_closed` | default |
| Merged | `merged` | green |
| %Merged | `pct_merged` | default |
| Responded | `responded_before_close` | bright cyan |
| %Responded | `pct_responded` | green if ≥ 50, amber if 20–49, red < 20 |

---

## Table 2 — Triaged PRs, still open

Lives inside the second collapsed `<details>` block. Title: `Triaged PRs — Still Open (<repo>)`.

One row per area where `total > 0`, sorted by `total` descending. `(no area)` last. Append a bold **TOTAL** row.

`Total` is a **reference-only** column — it counts every open PR in the area (collaborator + contributor alike). Every other numeric column is **contributor-only** (see [`aggregate.md#counters-per-area`](aggregate.md#counters-per-area)). This keeps draft-rate, triage-rate, and response-rate percentages meaningful: collaborator PRs bypass the triage funnel, so including them in the denominators would systematically understate how much of the contributor queue is ready, drafted, responded, etc.

| Column | Source | Denominator | Colour |
|---|---|---|---|
| Area | area name | — | cyan |
| Total | `total` (all PRs) | — | dim |
| Contrib. | `contributors` | — | bright cyan |
| %Contrib. | `contributors / total` | `total` | default |
| Draft | `drafts` (contributor) | — | default |
| %Draft | `drafts / contributors` | `contributors` | red if > 60%, otherwise default |
| Non-Draft | `non_drafts` (contributor) | — | default |
| Triaged | `triaged_waiting + triaged_responded` | — | amber |
| Responded | `triaged_responded` | — | green |
| %Responded | `triaged_responded / (triaged_waiting + triaged_responded)` | the triaged set | green ≥ 50, amber 20–49, red < 20 |
| Ready | `ready_for_review` | — | green (bold) |
| %Ready | `ready_for_review / contributors` | `contributors` | green ≥ 50, amber 20–49, red < 20 |
| Drafted by triager | `triager_drafted` | — | magenta |
| Drafted `<1d` / `1-7d` / `1-4w` / `>4w` (4 cols, optional) | `draft_age_buckets[bucket]` | — | dim magenta |
| Author resp `<1d` / `1-7d` / `1-4w` / `>4w` (4 cols, optional) | `age_buckets[bucket]` | — | dim |

Column order: Area → Total → Contrib./%Contrib. (area composition) → Draft/%Draft/Non-Draft (where the contributor work sits) → Triaged/Resp./%Resp. (how the triage funnel is going) → Ready/%Ready (what's at the review bar) → Drafted by triager + age buckets.

All numeric columns right-aligned. Keep area name left-aligned.

### Compact mode

If the rendered HTML is being embedded into a narrower context, drop the 8 age-bucket columns (keep through `Drafted by triager`). The dashboard's "Stalest Bucket" hero card still surfaces the >4w count globally.

---

## Legend

Render at the bottom of the dashboard inside a bordered panel. The legend explains every column, colour, and concept the dashboard introduces. Verbose by design — no maintainer should have to memorise the column abbreviations.

```html
<div class="legend">
<strong>Reading the dashboard</strong>

<dl>
<dt>Hero card colours</dt>
<dd>
  <span style="color:#56d364">green</span> = healthy / on-target;
  <span style="color:#d29922">amber</span> = needs attention soon;
  <span style="color:#f85149">red</span> = action needed now;
  <span style="color:#76e3ea">cyan</span> = informational (raw counts).
</dd>

<dt>Recommendation priorities</dt>
<dd>
  Each "What needs attention" card has a coloured left border:
  <span style="color:#f85149">red</span> = high priority (do today),
  <span style="color:#d29922">amber</span> = medium (this week),
  <span style="color:#6e7681">grey</span> = low (background awareness only).
</dd>

<dt>Closure velocity bars</dt>
<dd>
  Per-week stacked bars: <span style="color:#56d364">green</span> = PRs merged that week, <span style="color:#6e7681">grey</span> = PRs closed without merging. Total bar width is normalised to the busiest week in the 6-week window.
</dd>

<dt>Opened-vs-closed line chart</dt>
<dd>
  <span style="color:#58a6ff">Blue line</span> = PRs opened per week (createdAt). <span style="color:#56d364">Green line</span> = PRs closed/merged per week (closedAt). Where blue is above green the backlog grew that week; where green is above blue the backlog shrank. The "Net delta" lines under the chart translate this to actual ± numbers.
</dd>

<dt>Ready-for-review trend (multi-line chart)</dt>
<dd>
  Cumulative count of currently-`ready for maintainer review` PRs by week, one line per top-pressure area. Each line uses its area's pressure-band colour (<span style="color:#f85149">red</span> ≥ 30, <span style="color:#d29922">amber</span> 15–29, <span style="color:#6e7681">grey</span> &lt; 15). A steeply climbing line means review velocity isn't keeping up with triage promotion in that area. The "+N in last 7d" lines below the chart show recent growth pace per area.
</dd>

<dt>Closed by triage reason (stacked bars)</dt>
<dd>
  Per-week stacked bars showing how each week's closures break down by triage outcome:
  <span style="color:#56d364">green</span> = merged (success),
  <span style="color:#d29922">amber</span> = closed after author responded (engaged but didn't ship — design rejection, scope change),
  <span style="color:#f85149">red</span> = closed without author response (sweep-close on abandoned PRs),
  <span style="color:#6e7681">grey</span> = closed without ever being triaged (author self-close or maintainer fast-close).
  Healthy weeks are mostly green; a red-dominated week means a stale-sweep landed; a grey-dominated week means contributors are self-cleaning.
</dd>

<dt>Pressure score</dt>
<dd>
  Per area: weighted sum of urgent contributor PRs. Each PR contributes 0–5 points based on triage state and age (see <a href="aggregate.md#pressure-score">aggregate.md#pressure-score</a>). Higher score → area needs more maintainer attention. Border colour: <span style="color:#f85149">red ≥ 30</span>, <span style="color:#d29922">amber 15–29</span>, <span style="color:#6e7681">grey &lt; 15</span>.
</dd>

<dt>Triage states (used in the funnel cards and recommendation rules)</dt>
<dd>
  <span style="color:#56d364"><strong>Ready for review</strong></span>: PR has the <code>ready for maintainer review</code> label.
  <span style="color:#76e3ea"><strong>Responded</strong></span>: maintainer left a triage comment AND the author replied/pushed after it.
  <span style="color:#d29922"><strong>Waiting for Author</strong></span>: triaged but no author reply, OR draft (any state).
  <span style="color:#58a6ff"><strong>Not yet triaged</strong></span>: non-draft PR that has never received a quality-criteria comment.
</dd>

<dt>Detailed-table columns</dt>
<dd>
  <span style="color:#76e3ea"><strong>Contrib.</strong></span> — non-collaborator-authored PRs (denominator for nearly every contributor-scoped metric).
  <span style="color:#d29922"><strong>Triaged</strong></span> — PRs where a maintainer comment contains <code>Pull Request quality criteria</code> after the last commit.
  <span style="color:#56d364"><strong>Responded</strong></span> — author commented or pushed after the triage comment.
  <span style="color:#56d364"><strong>Ready</strong></span> — PRs carrying the <code>ready for maintainer review</code> label.
  <span style="color:#db61a2"><strong>Drafted by triager</strong></span> — drafts that also have a triage marker (heuristic: <code>isDraft AND is_triaged</code>).
  <span style="color:#6e7681"><strong>&lt;1d / 1-7d / 1-4w / &gt;4w</strong></span> — time since the PR author's last interaction (comment, commit, or PR creation).
</dd>

<dt>Percentage-cell colours</dt>
<dd>
  <span style="color:#56d364">green</span> if ≥ 50%, <span style="color:#d29922">amber</span> if 20–49%, <span style="color:#f85149">red</span> if &lt; 20%. The convention is "happier colour wins on a tie" so 50% reads as green, not amber.
</dd>

<dt>Methodology</dt>
<dd>
  Snapshot taken at the timestamp shown in the context line. Open-PR enumeration via GraphQL search. Closed/merged enumeration via REST <code>/pulls?state=closed</code> paginated until 3 consecutive pages out-of-window. Triage detection: comment by OWNER/MEMBER/COLLABORATOR containing the literal string <code>Pull Request quality criteria</code> after the last commit. Bots filtered at fetch time (<code>*[bot]</code>, <code>dependabot</code>, <code>github-actions</code>).
</dd>
</dl>
</div>
```

The legend is the *only* place the dashboard repeats the meaning of its colour conventions. Everything else relies on visual parsing — which is why the legend is verbose.

---

## End-of-output summary

Close with a single-line summary the maintainer can paste into Slack or a status email:

```text
Summary: 459 open · 37 triaged (8%) · 8 responded (22% of triaged) · 187 ready for review · 3 drafted by triager in last 7d.
```

Format: `Summary: <total> open · <triaged> triaged (<pct>%) · <responded> responded (<pct>% of triaged) · <ready> ready for review · <recent-drafts> drafted by triager in last 7d.`

Plain text (no HTML, no Rich markup). Keep the structure stable across runs — scripts that scrape this line shouldn't break between skill revisions.

---

## Markdown fallback

When rendering to Markdown (output piped to a file, or `markdown` flag), drop the HTML hero cards / SVG charts / coloured borders, and emit the same logical sections in flat Markdown:

- Hero cards → a 4-column GFM table
- Recommendations → an unordered list with `🔥` / `👀` / `📥` / `🔄` / `📍` / `📉` / `✨` icons in front of each item
- Velocity → a bullet list `<date>: <merged>✓ / <closed>✗ (total <N>)`
- Opened-vs-closed → a bullet list `<date>: opened <X> / closed <Y> (net <±N>)`
- Pressure by area → a Markdown table with the same columns
- Triage funnel → another 4-column GFM table
- Detailed tables → unchanged GFM tables (no `<details>` collapse — render expanded)
- Legend → a bullet list

Emoji markers are allowed in Markdown to substitute for colour. They are the *only* place emoji is allowed (the tone-rules section below still bans emoji in comment bodies).

---

## Rich terminal-tables variant

When the maintainer passes `tables-only`, render only the two detailed tables using `rich.console.Console` + `rich.table.Table` (the same layout the now-removed `breeze pr stats` produced). Skip the dashboard sections entirely — Rich can't render the hero cards or charts cleanly in a typical terminal.

```python
from rich.console import Console
from rich.table import Table

console = Console()

t = Table(
    title=f"Triaged PRs — Final State since {cutoff} ({repo})",
    title_style="bold",
    show_lines=True,
    show_footer=True,
)
t.add_column("Area", style="bold cyan", min_width=12, footer="Area")
t.add_column("Triaged Total", justify="right", style="yellow", footer="Triaged Total")
# … (see colour scheme above)

for area in sorted_areas:
    t.add_row(area, str(triaged_total), …)

t.add_row("[bold white]TOTAL[/]", f"[bold white]{n}[/]", …, style="on grey7", end_section=True)
console.print(t)
```

Use `show_lines=True` and `show_footer=True` so the footer mirrors the header on long tables. The `tables-only` mode is the legacy fallback for maintainers who script around the output and don't render HTML.

---

## Tone rules

- **No emoji in HTML body text** outside of recommendation icons and the health-rating label. The icons are functional (priority signals); free-text emoji is noise.
- **No opinions.** The dashboard surfaces deterministic numbers; interpretation belongs to the maintainer reading them. Don't let the renderer add "queue is in good shape" or "need to close stale drafts" sentences. The recommendation panel's `detail` strings explain the *trigger* and the *action* — not editorial.
- **No PR-level drill-in** in any rendered output. If the maintainer wants to zoom in on a specific area, the follow-up is `pr-triage label:area:<X>`, not a stats continuation. Recommendations encode this discipline by always pointing at another skill, never embedding PR numbers.
