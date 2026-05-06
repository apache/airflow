<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Render

Print the two tables, the legend, and a summary line. **Primary output is Rich-rendered colored tables to the terminal** — the same library the now-removed `breeze pr auto-triage` and `breeze pr stats` commands used, so the colour scheme and state-marker vocabulary stay consistent with what the maintainer already knows. Rich handles wide tables correctly (horizontal overflow, per-column wrapping) so Table 2's 20+ columns don't fall apart in a normal terminal.

A Markdown fallback is produced when the maintainer explicitly asks for shareable output (`markdown` flag, or the output is being piped to a non-tty).

---

## Why Rich, not Markdown by default

The first cut of this skill emitted GitHub-flavoured Markdown tables. In practice this produced two problems:

1. Table 2's width (20+ columns) exceeded the rendering width of common Markdown viewers. The table collapsed into a wrapped mess of `|`-separated values that read as prose, not a table.
2. Markdown carries no colour, so state-relevant cells (CI failing, many commits behind, etc.) had no visual cue — the maintainer had to compare numbers by eye.

Rich solves both: its table renderer computes column widths from the actual terminal size, overflows horizontally instead of wrapping (or truncates cells with an ellipsis if set), and supports inline colour markup. Rich is also what the now-removed `breeze pr stats` and `breeze pr auto-triage` commands used, so anyone migrating from the old breeze CLI sees the same colours and state names.

---

## Rich rendering

Use `rich.console.Console` + `rich.table.Table`. The shape mirrors what the now-removed `breeze pr stats` command used, kept verbatim so the output stays familiar to maintainers used to that command.

Minimum table setup:

```python
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

t = Table(
    title=f"Triaged PRs — Final State since {cutoff} ({repo})",
    title_style="bold",
    show_lines=True,
    show_footer=True,
)
t.add_column("Area", style="bold cyan", min_width=12, footer="Area")
t.add_column("Triaged Total", justify="right", style="yellow", footer="Triaged Total")
# … (see colour scheme below)

for area in sorted_areas:
    t.add_row(area, str(triaged_total), …)

# TOTAL row with bold-white style per cell
t.add_row("[bold white]TOTAL[/]", f"[bold white]{n}[/]", …, style="on grey7", end_section=True)

console.print(t)
```

Key settings:

- `show_lines=True` — horizontal separators between rows make the 20-column Table 2 readable.
- `show_footer=True` with a `footer=` on each column — column headers repeat at the bottom for long tables.
- `end_section=True` on the TOTAL row plus `style="on grey7"` — the totals row is visually separated and tinted, matching the look the predecessor `breeze pr stats` command used.

---

## Colour scheme (inherited from the predecessor breeze commands)

Use the same palette the now-removed `breeze pr auto-triage` and `breeze pr stats` commands used so state meanings transfer without relearning. Markup uses Rich's inline `[color]…[/]` syntax.

| Concept | Colour | Used in |
|---|---|---|
| Area name | `bold cyan` | Area column of both tables |
| Triage / Waiting for Author | `yellow` | `Triaged`, `%Responded` when < 100% |
| Responded | `green` | `Responded` column, `%Responded` when ≥ 50% |
| Ready for review | `bold green` | `Ready`, `%Ready` columns, Table 1 `Merged` column |
| Flagged / failing | `red` | `Closed` (when no merge), `%Closed`, CI-failing indicators |
| Drafted by triager | `magenta` | `Drafted by triager` |
| Drafted age buckets (secondary) | `magenta dim` | `Drafted <1d` … `Drafted >4w` |
| Author-resp age buckets (secondary) | `dim` | `Author resp <1d` … `Author resp >4w` |
| Contributor (non-collab) | `bright_cyan` | `Contrib.`, `%Contrib.` |
| Unknown / ambiguous | `dim` | fallback for `?` / `-` |
| TOTAL row | `bold white` on `grey7` | the footer-before-footer TOTAL row |

When a percentage cell is a close call (e.g. `%Responded` of exactly 50%), keep the happier colour — `green` above 50, `yellow` below — matching the "when in doubt, show it as still-ok" convention the predecessor breeze commands used.

---

## Triage state markers

The triage workflow categorises every non-collab PR into one of four states (the same four the now-removed `breeze pr auto-triage` overview table surfaced, and that the [`pr-triage`](../pr-triage/SKILL.md) skill continues to use):

| Marker | Meaning | Colour |
|---|---|---|
| `Ready for review` | PR has the `ready for maintainer review` label | `green` |
| `Waiting for Author` | PR is triaged (quality-criteria comment posted), no author response, OR the PR is draft | `yellow` |
| `Responded` | author commented / pushed after the triage comment | `bright_cyan` |
| `-` | not yet triaged | `blue` |

`pr-stats` surfaces the same buckets at the area level. After Table 2, print a small **state-breakdown panel** that slices the full open-PR set into the four triage-state markers:

```python
state_panel_lines = [
    "Triage-state breakdown:",
    f"  [green]Ready for review[/]    : {ready} PRs",
    f"  [bright_cyan]Responded[/]           : {responded} PRs (triaged, author has replied)",
    f"  [yellow]Waiting for Author[/]  : {waiting} PRs (triaged, no response) + {drafts_not_ready} drafts",
    f"  [blue]- (not yet triaged)[/] : {untriaged} PRs",
]
console.print(Panel("\n".join(state_panel_lines), title="By triage state", border_style="cyan", expand=False))
```

Exact counting rules:

- `ready` — `ready_for_review` label present (takes precedence over the other markers).
- `responded` — triaged with author reply after the triage comment (and NOT marked ready).
- `waiting` — triaged with no author reply (and NOT marked ready). Drafts without a triage marker fall into `waiting` too, matching the "drafts are author's court" logic the predecessor breeze commands used.
- `untriaged` — none of the above. Non-draft PR that hasn't been triaged yet.

Counts must sum to the open-PR total (non-bot). If they don't, print a warning and the discrepancy.

---

## Context line

Before the first table, print one line summarising the scope. This line is plain text (no Rich markup needed — it's header-style, not a table) so it renders identically in the terminal and in the Markdown fallback.

```
apache/airflow — 413 open PRs (non-bot) · closed/merged since 2026-03-11 · viewer @potiuk · 2026-04-22 22:33 UTC
```

Structure: `<repo> — <open_count> open PRs (non-bot) · closed/merged since <cutoff> · viewer @<login> · <now>`.

The `<now>` is the fetch-start timestamp, not the render-end timestamp — a slow run should still be interpretable as a snapshot at a single moment.

If the closed-since counts came from the lagging search index (see [`fetch.md#known-limitation`](fetch.md)), print a one-line caveat between the context line and Table 1:

```
⚠ Table 1 built from GitHub's free-text search of the quality-criteria marker. The index lags — older triaged+merged PRs are likely undercounted. Pass accurate-closed for the hybrid REST + GraphQL path.
```

---

## Table 1 — Triaged PRs, final state

Title: `Triaged PRs — Final State since <cutoff> (<repo>)`

One row per area where `triaged_total > 0`, sorted by `triaged_total` descending. `(no area)` goes last. Append a bold **TOTAL** row.

| Column | Source | Colour |
|---|---|---|
| Area | area name without the `area:` prefix | `bold cyan` |
| Triaged Total | `triaged_total` | `yellow` |
| Closed | `closed` | `red` |
| %Closed | `pct_closed` | default |
| Merged | `merged` | `green` |
| %Merged | `pct_merged` | default |
| Responded | `responded_before_close` | `bright_cyan` |
| %Responded | `pct_responded` | default |

---

## Table 2 — Triaged PRs, still open

Title: `Triaged PRs — Still Open (<repo>)`

One row per area where `total > 0`, sorted by `total` descending. `(no area)` last. Append a bold **TOTAL** row.

`Total` is a **reference-only** column — it counts every open PR in the area (collaborator + contributor alike). Every other numeric column is **contributor-only** (see [`aggregate.md#counters`](aggregate.md)). This keeps draft-rate, triage-rate, and response-rate percentages meaningful: collaborator PRs bypass the triage funnel, so including them in the denominators would systematically understate how much of the contributor queue is ready, drafted, responded, etc.

| Column | Source | Denominator | Colour |
|---|---|---|---|
| Area | area name | — | `bold cyan` |
| Total | `total` (all PRs) | — | `dim` |
| Contrib. | `contributors` | — | `bright_cyan` |
| %Contrib. | `contributors / total` | `total` | default |
| Draft | `drafts` (contributor) | — | default |
| %Draft | `drafts / contributors` | `contributors` | default |
| Non-Draft | `non_drafts` (contributor) | — | default |
| Triaged | `triaged_waiting + triaged_responded` (contributor) | — | `yellow` |
| Responded | `triaged_responded` (contributor) | — | `green` |
| %Responded | `triaged_responded / (triaged_waiting + triaged_responded)` | the triaged set | default |
| Ready | `ready_for_review` (contributor) | — | `bold green` |
| %Ready | `ready_for_review / contributors` | `contributors` | default |
| Drafted by triager | `triager_drafted` (contributor) | — | `magenta` |
| Drafted `<1d` / `1-7d` / `1-4w` / `>4w` (4 cols) | `draft_age_buckets[bucket]` (contributor) | — | `magenta dim` |
| Author resp `<1d` / `1-7d` / `1-4w` / `>4w` (4 cols) | `age_buckets[bucket]` (contributor) | — | `dim` |

Column order: Area → Total → Contrib./%Contrib. (area composition) → Draft/%Draft/Non-Draft (where the contributor work sits) → Triaged/Resp./%Resp. (how the triage funnel is going) → Ready/%Ready (what's at the review bar) → Drafted by triager + age buckets (time-since slices of the active contributor work).

All numeric columns right-aligned. Keep area name left-aligned.

### Wide-table note

21 columns is wide but Rich handles it. Two behaviours to be aware of:

- **Rich computes column widths from the real terminal size.** A narrow terminal will trim to fit. `console.size.width` is available if the skill wants to override — but don't; let Rich decide.
- **Optional compact mode.** If the maintainer passes `compact`, drop the 8 age-bucket columns, keeping only through `Drafted by triager`. The state-breakdown panel still prints. Mention compact mode in the context line when active.

### Markdown fallback

When rendering to Markdown (piped, or `markdown` flag), keep the same column order and colour-meaning mapping but express colour with emoji markers in the percentage cells:

- 🟢 `%Ready` ≥ 50% (green)
- 🟡 `%Responded` < 50% *of triaged* (yellow / waiting)
- 🔴 `%Draft` > 60% in an area (flag)

These emoji markers are the **only** place emoji is allowed (the tone-rules section below still bans emoji in comment bodies / prose). Colour isn't available in Markdown, so a small semantic marker substitutes.

---

## Legend

After both tables and the state-breakdown panel, print a short legend. Keep it under 10 lines — the goal is to let someone cold-read the tables without opening this doc.

```python
legend_lines = [
    "[bold]Column legend:[/]",
    "  [bright_cyan]Contrib.[/]             — PRs by non-collaborator contributors.",
    "  [yellow]Triaged[/]              — PRs where a maintainer posted a quality-criteria triage comment after the last commit.",
    "  [green]Responded[/]            — author commented or pushed a commit after the triage comment.",
    "  [bold green]Ready[/]                — PRs carrying the `ready for maintainer review` label.",
    "  [magenta]Drafted by triager[/]   — PRs converted to draft by a maintainer (heuristic: draft + triaged).",
    "  [dim]Author resp[/] columns    — time since the PR author's last interaction (comment, commit, or PR creation).",
    "  [magenta dim]Drafted[/] columns        — time since the triage comment landed on a draft PR.",
]
console.print(Panel("\n".join(legend_lines), border_style="dim", expand=False))
```

---

## End-of-output summary

Close with a single-line summary the maintainer can use for at-a-glance reporting:

```
Summary: 413 open · 66 triaged (16%) · 3 responded (5% of triaged) · 126 ready for review · 43 drafted by triager in last 7d.
```

Format: `Summary: <total> open · <triaged> triaged (<pct>%) · <responded> responded (<pct>% of triaged) · <ready> ready for review · <recent-drafts> drafted by triager in last 7d.`

This line is plain text (no Rich markup) so it copies cleanly into Slack or a status email. Keep the structure stable across runs — scripts that scrape this line shouldn't break between skill revisions.

---

## Tone rules

- **No emoji in Rich output.** Colour is the visual cue. The only place emoji is allowed is the Markdown-fallback percentage cells described under *Markdown fallback*.
- **No opinions.** The stats describe state; interpretation belongs to the maintainer reading them. Don't add "queue is in good shape" or "need to close stale drafts" sentences.
- **No PR-level drill-in** in the stats output. If the maintainer wants to zoom in on a specific area, the follow-up is `pr-triage label:area:<X>`, not a stats continuation.
