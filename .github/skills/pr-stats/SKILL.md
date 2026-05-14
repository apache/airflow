---
name: pr-stats
description: |
  Read-only maintainer dashboard for the open-PR backlog of apache/airflow.
  Surfaces a health rating, prioritised action recommendations, weekly closure
  velocity trends, area pressure ranking, and a triage-funnel breakdown — with
  the underlying area-grouped tables as a collapsible details section.
when_to_use: |
  When the user asks "how is the PR queue doing", "run PR stats", "what should
  I do today", "show me the trends", "where is queue pressure sitting", or any
  variation on "give me the maintainer view of the backlog". Good as a daily
  health check, before or after a triage sweep, or as an input to a planning
  session.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- Placeholder convention:
     <repo>   → target GitHub repository in `owner/name` form (default: apache/airflow)
     <viewer> → the authenticated GitHub login of the maintainer running the skill
     Substitute these before running any `gh` command below. -->

# pr-stats

Read-only skill that answers "what should the maintainer **do** about the
open-PR backlog right now". Primary output is a **dashboard** with five
sections:

| Section | What it shows | Maintainer use |
|---|---|---|
| **Hero cards** | Health rating, total open, ready-for-review count, untriaged-non-drafts (with >4w callout) | At-a-glance status |
| **What needs attention** | Prioritised action recommendations (high/medium/low) with the exact slash command to run | Decide what to spend the next hour on |
| **Closure velocity** | Per-week merged/closed bars over the last 6 weeks, plus avg/peak | Spot slowdowns or burst weeks |
| **Pressure by area** | `area:*` ranking by weighted untriaged-old PR count | Pick a focused triage / review session |
| **Triage funnel** | Triage coverage %, author response rate %, stalest bucket, this-week velocity | See whether the funnel is healthy end-to-end |

The two original tables (**Triaged final-state since cutoff** and **Triaged still-open by area**) are kept as a *collapsible details section* at the bottom of the dashboard for maintainers who want the raw per-area numbers.

The skill is the statistical complement of [`pr-triage`](../pr-triage/SKILL.md) — same repo, same classification logic, no mutations. Running the two in sequence (stats → triage → stats) lets a maintainer measure a sweep's effect; the dashboard's recommendations link directly back to specific `pr-triage` invocations.

Detail files:

| File | Purpose |
|---|---|
| [`fetch.md`](fetch.md) | GraphQL templates for open-PR list and closed/merged-since-cutoff list. |
| [`classify.md`](classify.md) | Triage-status detection (waiting vs. responded vs. never-triaged) — reuses the `Pull Request quality criteria` marker from `pr-triage`. Also defines the per-PR `pressure_weight`. |
| [`aggregate.md`](aggregate.md) | Area grouping, age buckets, totals, percentage rules. Also defines weekly velocity buckets, area pressure scores, and the health-rating thresholds. |
| [`render.md`](render.md) | The dashboard layout (hero / actions / trends / hotspots / details) plus the underlying tables, colour scheme, and recommendation rules. |

---

## Golden rules

**Golden rule 1 — no mutations, ever.** This skill only reads. It must not post comments, add labels, close, rebase, or approve anything. If the maintainer asks for stats and also wants an action, decline the mutation and redirect to `pr-triage`.

**Golden rule 2 — reuse pr-triage's triage-detection.** The "triaged" count and "responded" count depend on the same `Pull Request quality criteria` marker string and the same collaborator set (`OWNER`/`MEMBER`/`COLLABORATOR`) that drive the triage-marker rows in `pr-triage/classify-and-act.md` (rows 3–4 — `already_triaged`). Don't invent a second definition — both skills must agree on "is this PR triaged".

**Golden rule 3 — one GraphQL call per batch, not per PR.** Same rule as `pr-triage/fetch-and-batch.md`. One aliased query covers the open-PR list for a whole page; the closed/merged fetch is paginated by GitHub's search cursor. Never call `gh pr view` per PR.

**Golden rule 4 — include a legend with every render.** The tables are dense (15+ columns on the still-open table). Always print a short legend after the tables explaining the columns — `Contrib.` = non-collaborator, `Responded` = author replied after the triage comment, `Drafted by triager` = PR converted to draft by the viewer, etc. Nobody remembers column abbreviations in isolation. The dashboard's hero cards and recommendation panel are themselves self-explanatory and don't need the legend; the legend is for the collapsed "Detailed tables" section.

**Golden rule 5 — state the input scope up front.** Before rendering, print one line summarising what the stats cover: repo name, total open PR count, closed-since cutoff date, and viewer login. The numbers only make sense in context.

**Golden rule 6 — recommendations are deterministic, not opinions.** Every action surfaced in the "What needs attention" panel comes from a fixed rule in [`render.md#recommendation-rules`](render.md#recommendation-rules). The skill never editorialises ("queue is doing well", "you should focus on X") — it surfaces the rule's trigger and the suggested next-step command. The maintainer reads the trigger and decides; the skill never decides for them. New rules are added by editing the rules table, not by adding free-text inside the renderer.

**Golden rule 7 — actions link to other skills, never mutate.** Every recommendation's `action` field is the *exact* slash-command the maintainer can paste to do the work — almost always `/pr-triage`, `/maintainer-review`, or a focused variant with a label/PR-number filter. The stats skill itself remains pure-read (Golden rule 1); the dashboard makes downstream skills *one paste away* from running.

---

## Inputs

Optional selectors the maintainer may pass:

| Selector | Resolves to |
|---|---|
| *(no args)* | default — all open PRs on `apache/airflow`, closed/merged since the configured cutoff |
| `repo:<owner>/<name>` | override the target repo |
| `since:YYYY-MM-DD` | override the closed-since cutoff (default: 6 weeks ago) |
| `clear-cache` | invalidate the scratch cache before fetching |

No per-PR drill-in — this skill is aggregate-only.

---

## Step 0 — Pre-flight

1. `gh auth status` must succeed; capture the viewer login (needed for the triage-marker check in step 2).
2. Run one GraphQL query that asks both for `viewer { login }` and for `repository(owner, name) { name }` to confirm the repo is reachable. `viewerPermission` is NOT required (this skill doesn't mutate) — skip the write-check that `pr-triage` does.
3. Read or initialise the scratch cache at `/tmp/pr-stats-cache-<repo-slug>.json` (see [`aggregate.md#cache`](aggregate.md#cache)). The cache stores the viewer login and a map of `pr_number → (head_sha, triage_status)` so a re-run inside the same session skips the per-PR enrichment.

A failure at step 1 is a **stop**. Steps 2 and 3 degrade with warnings.

---

## Step 1 — Fetch open PRs

Use the query template in [`fetch.md#open-prs`](fetch.md#open-prs) to get every open PR with the fields needed for classification (labels, `isDraft`, `authorAssociation`, `createdAt`, last commit `committedDate`, last 10 comments for the triage-marker scan).

Paginate until `pageInfo.hasNextPage == false`. Batch size of 50 is safe (the open-PR selection set is lighter than `pr-triage`'s — no `statusCheckRollup`, no `reviewThreads`, no `latestReviews`). For a 300-PR backlog that's six GraphQL calls.

---

## Step 2 — Classify triage status per PR

For each open PR, determine:

- `is_triaged_waiting` — viewer's (or any collaborator's) comment contains the `Pull Request quality criteria` marker, the comment post-dates the PR's last commit, AND the author has NOT commented after it.
- `is_triaged_responded` — same marker found, but the author HAS commented after it.
- `is_drafted_by_triager` — the PR was converted to draft by the viewer at or after the triage comment (from the `ConvertToDraftEvent` timeline, optional — see [`classify.md#drafted-by-triager`](classify.md#drafted-by-triager) for the cheaper heuristic).
- `last_author_interaction_at` — most recent `commit.committedDate` OR author comment `createdAt`, whichever is later.

Cache these per `(pr_number, head_sha)` so a subsequent run skips the scan.

---

## Step 3 — Fetch closed / merged triaged PRs since cutoff

The second table is a separate search. Fetch closed or merged PRs whose comment history contains the triage marker since the configured cutoff date. Use the template in [`fetch.md#closed-merged-triaged-prs`](fetch.md#closed-merged-triaged-prs).

Cutoff defaults to `today - 6 weeks`. The cutoff should be configurable because a maintainer asking "how did last week's sweep do" wants `since:today-7d`, while a monthly report wants `since:today-30d`.

---

## Step 4 — Aggregate by area

Group each PR by every `area:*` label it carries. A PR with `area:UI` and `area:scheduler` contributes to both groups. A PR with no `area:*` labels lands in a pseudo-area `(no area)`.

Per area, compute the counters in [`aggregate.md#counters-per-area`](aggregate.md#counters-per-area): total, drafts, non-drafts, contributors, triaged-waiting, triaged-responded, ready-for-review, drafted-by-triager, plus age-bucket histograms.

Also compute a `TOTAL` row where each PR is counted exactly once (NOT the sum of per-area counters — PRs with multiple `area:*` labels would double-count).

---

## Step 5a — Compute health rating + action recommendations

Pure function of the classified open-PR set. No network.

1. Apply the **health rating** thresholds from [`aggregate.md#health-rating`](aggregate.md#health-rating): each fired threshold is a "issue point". Map total points → `✅ Healthy` / `⚠️ Needs attention` / `🔥 Action needed`.
2. Walk the **recommendation rules** from [`render.md#recommendation-rules`](render.md#recommendation-rules) in declared order. Each rule that fires produces one entry with `priority`, `icon`, `title`, `detail`, `action` (an exact slash command, or `—` when no paste-clean command applies), and a count. `action` and `detail` are kept in separate columns so prose / parentheticals stay out of the slash command.
3. The recommendation list is the input to the dashboard's "What needs attention" panel. If zero rules fire, surface the explicit "no urgent actions detected" panel — never leave the section empty.

---

## Step 5b — Compute weekly velocity buckets

Pure function of the closed/merged-since-cutoff PR set.

For each of the last 6 weeks (rolling, anchored on the fetch-start `<now>`), bucket PRs by `closedAt` and count `merged` and `closed` separately. Also count the triaged-then-merged / triaged-then-closed / triaged-then-responded subsets — those are what feed the trend mini-stats below the velocity bars.

See [`aggregate.md#weekly-velocity`](aggregate.md#weekly-velocity) for the exact bucket boundaries and the avg/peak summary computation.

---

## Step 5c — Compute opened-vs-closed weekly buckets

Pure function of *both* the open-PR set (Step 1) and the closed/merged-since-cutoff PR set (Step 3) — every PR's `createdAt` is checked against each weekly window regardless of current state.

For each of the same six rolling weekly windows, compute:

- `opened` — PR's `createdAt` falls in the window
- `closed_total` — PR was closed/merged in the window (reuses the velocity buckets from Step 5b)
- `net_delta = opened - closed_total`

These per-week numbers feed the dashboard's "Opened vs closed momentum" line chart and the two-line "Net delta" summary below it. See [`aggregate.md#opened-vs-closed-weekly-buckets`](aggregate.md#opened-vs-closed-weekly-buckets) for the exact spec.

---

## Step 5d — Compute ready-for-review trend by top areas

Needs one extra fetch (per [`fetch.md#ready-label-timeline`](fetch.md#ready-label-timeline)): for each currently-`ready for maintainer review` PR, the timestamp of its most recent `LabeledEvent` adding that label. Aliased GraphQL, ~30 PRs per call.

Then for each top-pressure area (top 5 by Step 5f's score, filtered to areas with ≥ 3 currently-ready PRs), compute a 6-bucket cumulative count: `ready_count[a][w] = count of currently-ready PRs in area a where labeled_at <= w.end`.

Feeds the dashboard's "Ready-for-review trend" multi-line chart. See [`aggregate.md#ready-for-review-trend-by-top-areas`](aggregate.md#ready-for-review-trend-by-top-areas) for the exact spec and rendering rules.

---

## Step 5e — Compute closed-by-triage-reason buckets

Pure function of the closed/merged-since-cutoff PR set (Step 3) — reuses the existing per-PR `is_triaged` / `responded_before_close` / `merged` flags.

For each weekly bucket, classify each closed PR into exactly one of four categories: `merged`, `closed-after-responded`, `closed-after-triage-no-response`, `closed-no-triage`. Sum per category per week.

Feeds the dashboard's "Closed-by-triage-reason per week" stacked bar chart. See [`aggregate.md#closed-by-triage-reason-per-week`](aggregate.md#closed-by-triage-reason-per-week) for the category definitions, colour map, and summary line.

---

## Step 5f — Compute area pressure scores

Pure function of the classified open-PR set.

Per area, compute a **pressure score** = weighted sum of urgent PR conditions. The weights are defined in [`aggregate.md#pressure-score`](aggregate.md#pressure-score):

- untriaged non-draft, > 4 weeks old → 5 pts
- untriaged non-draft, 1–4 weeks old → 3 pts
- untriaged non-draft, < 1 week old → 1 pt
- triaged-waiting, > 7 days old → 2 pts (author abandoned, sweep candidate)
- ready-for-review (label present) → 1 pt (queue waiting on maintainer review)
- everything else → 0 pts (drafts the maintainer can ignore until author engages)

Sort areas by score descending; render the top 8 (filtering areas with < 3 contributor PRs as noise) in the "Pressure by area" panel.

---

## Step 6 — Render dashboard

Render the maintainer dashboard per the layout in [`render.md#dashboard-layout`](render.md#dashboard-layout):

1. **Context line** — repo, open count, cutoff, viewer, timestamp.
2. **Hero cards (4)** — health rating, total open, ready count, untriaged-non-draft count.
3. **What needs attention** — recommendation list from Step 5a.
4. **Closure velocity** — weekly bar chart from Step 5b.
5. **Opened vs closed momentum** — line chart from Step 5c.
6. **Ready-for-review trend** — multi-line chart from Step 5d (top areas).
7. **Closed by triage reason** — stacked-bar chart from Step 5e.
8. **Pressure by area** — top areas from Step 5f.
9. **Triage funnel** — coverage %, response rate %, stalest bucket, this-week velocity.
10. **Detailed tables** (collapsed by default):
    1. **Triaged PRs — Final State since `<cutoff>`** — one row per area where `Triaged Total > 0`.
    2. **Triaged PRs — Still Open** — one row per area where `Total > 0`, plus the `TOTAL` row.
11. **Legend** — verbose explanation of every colour, column abbreviation, and computed metric on the dashboard.

The dashboard is **HTML by default** so the colour-coded hero cards, action priority bars, and velocity bars render correctly. A Markdown fallback (and a Rich terminal-tables variant for the detailed-tables section only) is produced when the maintainer passes `markdown` or `tables-only`. See [`render.md`](render.md) for the full layout, the colour scheme, and the recommendation rule definitions.

---

## What this skill does NOT do

- **No mutations.** See Golden rule 1.
- **No per-PR drill-in.** The output is aggregate — if the maintainer wants to inspect a specific PR, they run `pr-triage pr:<N>` or open it in the browser.
- **No author-level stats.** Grouping is by area label, not by author login. A stats-by-author skill is a separate scope.
- **No PR *quality* scoring.** CI pass/fail, diff size, and review-thread counts are all omitted from the aggregate — they belong in the per-PR `pr-triage` view.
- **No long-term historical trends.** The closure-velocity panel covers the last 6 weeks computed from the closed-since-cutoff fetch (one snapshot at fetch time). There is no persistent time-series store; tracking month-over-month is the maintainer's job — re-run the skill at a different `since:` date if needed.
- **No automatic actions from recommendations.** Every "What needs attention" entry is a *suggestion* with a slash-command the maintainer can paste. The stats skill itself never invokes another skill, never adds labels, never closes PRs.

---

## Budget discipline

Typical session against `apache/airflow`:

- 1 pre-flight query (viewer + repo)
- ~6 paginated GraphQL calls for ~300 open PRs (50 per page)
- ~2 paginated calls for closed/merged-since-cutoff (typically 20–80 PRs per week of cutoff)
- No per-PR REST calls — the comment scan for triage markers is done from the `comments(last: 10)` subfield in the open-PR query

Total budget: ~10 GraphQL calls regardless of repo size. Well under 5% of the hourly budget.
