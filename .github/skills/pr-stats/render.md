<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Render

Print the two tables and the legend. Output is Markdown — it renders in the terminal via Claude Code's markdown output, and also pastes cleanly into a GitHub comment or Slack message if the maintainer wants to share.

Tables use GitHub-flavoured Markdown. Right-align numeric columns with `---:` in the header separator.

---

## Context line

Before the first table, print one line summarising the scope:

```
apache/airflow — 312 open PRs · closed/merged since 2026-03-11 · viewer @potiuk · 2026-04-23 10:17 UTC
```

Structure: `<repo> — <open_count> open PRs · closed/merged since <cutoff> · viewer @<login> · <now>`.

The `<now>` is the fetch-start timestamp, not the render-end timestamp — a slow run should still be interpretable as a snapshot at a single moment.

---

## Table 1 — Triaged PRs, final state

Title: `## Triaged PRs — Final State since <cutoff> (<repo>)`

One row per area where `triaged_total > 0`, sorted by `triaged_total` descending. `(no area)` goes last.

| Column | Source | Alignment |
|---|---|---|
| Area | area name without the `area:` prefix | left |
| Triaged Total | `triaged_total` | right |
| Closed | `closed` | right |
| %Closed | `pct_closed` | right |
| Merged | `merged` | right |
| %Merged | `pct_merged` | right |
| Responded | `responded_before_close` | right |
| %Responded | `pct_responded` | right |

Append a bold **TOTAL** row using the overall closed-since totals (unique PR counts, not per-area sum).

### Example row

```
| scheduler   |  24 |   9 | 38% |  15 | 62% |  14 | 58% |
```

Markdown rendering:

```markdown
| Area        | Triaged Total | Closed | %Closed | Merged | %Merged | Responded | %Responded |
|:------------|--------------:|-------:|--------:|-------:|--------:|----------:|-----------:|
| scheduler   |            24 |      9 |     38% |     15 |     62% |        14 |        58% |
| UI          |            18 |      6 |     33% |     12 |     67% |        10 |        56% |
| (no area)   |             5 |      2 |     40% |      3 |     60% |         2 |        40% |
| **TOTAL**   |        **47** |  **17** | **36%** | **30** | **64%** |    **26** |    **55%** |
```

---

## Table 2 — Triaged PRs, still open

Title: `## Triaged PRs — Still Open (<repo>)`

One row per area where `total > 0`, sorted by `total` descending. `(no area)` last.

| Column | Source |
|---|---|
| Area | area name |
| Total | `total` |
| Draft | `drafts` |
| %Draft | `drafts / total` |
| Non-Draft | `non_drafts` |
| Contrib. | `contributors` |
| %Contrib. | `contributors / total` |
| Triaged | `triaged_waiting + triaged_responded` |
| Responded | `triaged_responded` |
| %Responded | `triaged_responded / (triaged_waiting + triaged_responded)` |
| Ready | `ready_for_review` |
| %Ready | `ready_for_review / total` |
| Drafted by triager | `triager_drafted` |
| Drafted `<1d` | `draft_age_buckets["<1d"]` |
| Drafted `1-3d` | `draft_age_buckets["1-3d"]` |
| Drafted `3-7d` | `draft_age_buckets["3-7d"]` |
| Drafted `1-2w` | `draft_age_buckets["1-2w"]` |
| Drafted `2-4w` | `draft_age_buckets["2-4w"]` |
| Drafted `>4w` | `draft_age_buckets[">4w"]` |
| Author resp `<1d` | `age_buckets["<1d"]` |
| Author resp `1-3d` | `age_buckets["1-3d"]` |
| Author resp `3-7d` | `age_buckets["3-7d"]` |
| Author resp `1-2w` | `age_buckets["1-2w"]` |
| Author resp `2-4w` | `age_buckets["2-4w"]` |
| Author resp `>4w` | `age_buckets[">4w"]` |

All numeric columns right-aligned. Keep area name left-aligned. The bold **TOTAL** row aggregates the whole open set with unique-count semantics (see [`aggregate.md#total-row`](aggregate.md)).

### Wide-table note

Table 2 has 20+ columns and won't fit a standard 80-column terminal. Two mitigations:

- **Print Markdown, let the viewer wrap.** GFM tables in a Markdown-aware viewer (Claude Code, GitHub) render horizontally-scrollable, not wrapped.
- **Optional compact mode.** If the maintainer passes `compact`, emit just the first twelve columns (through `%Ready`) — the age buckets drop off. Mention in the context line that compact mode is on.

---

## Legend

After both tables, print a short legend. One line per abbreviated column. Keep it under 10 lines — the goal is to let someone cold-read the tables without opening this doc.

Exact wording:

```markdown
**Column legend:**
- **Contrib.** — PRs by non-collaborator contributors (excludes OWNER/MEMBER/COLLABORATOR authors).
- **Triaged** — PRs where a quality-criteria triage comment was posted by a maintainer after the last commit.
- **Responded** — author commented (or pushed a commit) after the triage comment.
- **Ready** — PRs with the `ready for maintainer review` label.
- **Drafted by triager** — PRs converted to draft by a maintainer after triage.
- **Author resp** columns — time since the PR author's last interaction (comment, commit, or PR creation).
- **Drafted** columns — time since a maintainer converted the PR to draft.
```

If the `drafted_by_triager` classification used the heuristic (not the timeline lookup), append a line:

```markdown
- **Drafted by triager (heuristic)** — approximated as `isDraft && triaged`; a PR that was already draft before triage is counted here too. Run with `drafted-timeline` for the exact count.
```

---

## End-of-output summary

Close with a single-line summary the maintainer can use for at-a-glance reporting:

```
Summary: 312 open · 134 triaged (43%) · 48 responded (36% of triaged) · 29 ready for review · 7 drafted by triager in last 7d.
```

Format: `Summary: <total> open · <triaged> triaged (<pct>%) · <responded> responded (<pct>% of triaged) · <ready> ready for review · <recent-drafts> drafted by triager in last 7d.`

This matches what the maintainer is likely to paste into a weekly status message. Keep the structure stable across runs — scripts that scrape this line shouldn't break between skill revisions.

---

## Tone rules

- **No emoji** in the tables or legend. Same convention as `pr-triage/comment-templates.md#tone-rules`.
- **No opinions.** The stats describe state; interpretation belongs to the maintainer reading them. Don't add "queue is in good shape" or "need to close stale drafts" sentences.
- **No PR-level drill-in** in the stats output. If the maintainer wants to zoom in on a specific area, the follow-up is `pr-triage label:area:<X>`, not a stats continuation.
