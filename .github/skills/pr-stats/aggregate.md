<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Aggregate

Turn the classified PR set into the two per-area stat objects that [`render.md`](render.md) prints. Pure function of the output of [`classify.md`](classify.md) — no network.

---

## Area grouping

Each PR can carry zero or more `area:*` labels (e.g. `area:UI`, `area:scheduler`, `area:db-migrations`). For stats purposes:

- Strip the `area:` prefix — display column shows `UI`, `scheduler`, etc.
- A PR with **multiple** `area:*` labels contributes to **every** matching area (a cross-cutting PR moves the needle in each area it touches).
- A PR with **no** `area:*` labels goes into a synthetic area named `(no area)`.
- Never expand to other label prefixes (`provider:*`, `kind:*`, `backport-to-*`) — those have their own grouping stories and would dilute the area view.

Order the areas for display by total-count descending, with `(no area)` always last regardless of count.

---

## Counters (per area)

One `_AreaStats` block per area. Only two counters (`total` and `contributors`) cover all PRs; every other counter is **contributor-only** (excludes `OWNER` / `MEMBER` / `COLLABORATOR` authors). The rationale is that collaborator PRs have a different lifecycle — they're not triaged by the pr-triage skill, they don't need "ready for maintainer review" to surface, and their drafts are the author's to manage. Mixing them into the draft / triaged / responded counts dilutes every percentage on the row.

| Field | Count rule | Scope |
|---|---|---|
| `total` | count of PRs in the area | **all** — reference only |
| `contributors` | `authorAssociation NOT IN (OWNER, MEMBER, COLLABORATOR)` | **all** — denominator for the contributor-scoped counters below |
| `drafts` | `isDraft == true` | contributor-only |
| `non_drafts` | `isDraft == false` | contributor-only |
| `triaged_waiting` | classified `triaged_waiting` (see `classify.md`) | contributor-only |
| `triaged_responded` | classified `triaged_responded` | contributor-only |
| `ready_for_review` | label `ready for maintainer review` present | contributor-only |
| `triager_drafted` | classified `drafted_by_triager` | contributor-only |
| `age_buckets` | histogram, key = bucket label from `classify.md#age-bucket` | contributor-only |
| `draft_age_buckets` | histogram over PRs where `drafted_at` is set, same bucket labels | contributor-only |

### Invariants

- `contributors == drafts + non_drafts` (each contributor PR is one or the other)
- `triaged_waiting + triaged_responded <= contributors`
- `ready_for_review <= non_drafts` (a ready PR shouldn't be draft — if the inequality fails, the label is stale; surface a one-line warning but don't correct the data)
- `sum(age_buckets.values()) == contributors`
- `contributors <= total`

Check the invariants at render time and print a one-line warning if any fails — it usually means the fetch shape dropped a field.

---

## TOTAL row

The TOTAL row is NOT the column-wise sum of per-area rows — a PR with two `area:*` labels appears in both area rows, so summing would double-count. Instead:

- Re-walk the classified PR set.
- For each PR, increment every counter exactly once (ignore its area labels entirely).
- Render as a final row, visually separated from the per-area rows.

The TOTAL row's `age_buckets` also re-buckets every PR once. The final TOTAL row is the authoritative "how big is the backlog" view.

---

## Percentage rules

The stats tables show percentages alongside counts for readability. Rules:

- Format: rounded integer with `%` suffix (e.g. `73%`). No decimals — table noise.
- If the denominator is 0, show `-`, not `0%`.
- `%Contrib.` denominator is `total` (how much of the area is contributor-authored).
- `%Draft` denominator is `contributors` (how much of the contributor work is still in draft).
- `%Ready` denominator is `contributors` (how much of the contributor work is at the review bar).
- `%Responded` denominator is `triaged_waiting + triaged_responded` (the triaged set). A PR that was never triaged can't have responded.

The only percentage whose denominator is `total` is `%Contrib.` — that's the one that describes the area composition. Every other percentage describes contributor activity and uses `contributors` as its denominator.

Table 1 has its own percentage set (`%Closed`, `%Merged`, `%Responded`) whose denominators are `triaged_total` for that area (not global, not contributor-scoped — Table 1's whole point is the triaged set).

---

## Closed-since counters (Table 1)

Parallel structure but different fields per area:

| Field | Count rule |
|---|---|
| `triaged_total` | count of closed/merged PRs in the area that were triaged |
| `closed` | `state == CLOSED AND NOT merged` |
| `merged` | `merged == true` |
| `responded_before_close` | triaged PRs whose author commented after the triage comment and before `closedAt` |

Derived:

- `pct_closed = closed / triaged_total`
- `pct_merged = merged / triaged_total`
- `pct_responded = responded_before_close / triaged_total`

Percentages can legitimately sum to > 100% when a PR was both merged and responded; don't force them to add up.

---

## Pressure score

Per area, the dashboard's "Pressure by area" ranking uses a weighted urgency score so areas with stale-and-untriaged contributor PRs surface above areas with healthy queues regardless of raw size.

For each contributor PR in an area, add the matching weight (first-match-wins, top to bottom):

| Condition | Weight | Rationale |
|---|---|---|
| ready-for-review label present | **1** | queue waiting on maintainer review — soft pressure |
| triaged-waiting AND triage comment age ≥ 7 days | **2** | author abandoned; stale-sweep candidate |
| draft (any age, any triage state) | **0** | author's court — not maintainer pressure |
| untriaged non-draft AND `last_author_at` ≥ 28 days | **5** | most urgent — slipped through triage |
| untriaged non-draft AND 7–28 days | **3** | author still likely active; needs triage soon |
| untriaged non-draft AND < 7 days | **1** | recent — give the next sweep a chance |

Collaborator-authored PRs (`OWNER`/`MEMBER`/`COLLABORATOR`) score **0** regardless of state — they have a different lifecycle (see [`#counters-per-area`](#counters-per-area)).

Sort areas by score descending. The dashboard renders the top 8 areas; areas with fewer than 3 contributor PRs are filtered as noise (a tiny area with one stale PR shouldn't dominate the ranking).

The score is also used to bucket the area into a severity colour for the dashboard:

| Score | Severity | Border colour |
|---|---|---|
| ≥ 30 | high | red |
| 15–29 | medium | amber |
| < 15 | low | grey |

Tune the weights here in lockstep with the recommendation rules in [`render.md#recommendation-rules`](render.md#recommendation-rules) — they share the same notion of "what's urgent" and drift between the two would produce contradictory dashboard sections.

---

## Weekly velocity

The dashboard's "Closure velocity" panel buckets the closed-since-cutoff PR set into the last 6 calendar-weeks (rolling, anchored on the fetch-start `<now>`).

For each `w` in `0..5`:

```
window_end   = now - w * 7 days
window_start = window_end - 7 days
```

`w == 0` is the current week (oldest = `<now> - 7d`, newest = `<now>`). `w == 5` is the oldest week in the chart.

Per window, count three metrics:

| Field | Count rule |
|---|---|
| `merged` | PR has `merged == true` AND `closedAt` falls in the window |
| `closed` | PR has `merged == false` AND `closedAt` falls in the window |
| `triaged_then_responded` | PR is triaged (per [`classify.md#triage-marker`](classify.md#triage-marker)) AND `closedAt` falls in the window AND the author commented after the triage comment |

Render the bars oldest → newest (so the eye sweeps left-to-right matching natural time order). Each bar is a stacked `merged` (green) + `closed` (grey) segment, normalised to the maximum total in the 6-week window.

Below the bars, print three summary numbers:

- **6-week total** — sum of `merged + closed` across all six windows.
- **avg/wk** — `total / 6`, rounded.
- **peak** — `max(merged + closed)` across the six windows.

The avg and peak give the maintainer a quick sense of whether this week is normal, slow, or unusually busy.

---

## Opened-vs-closed weekly buckets

The dashboard's "Opened vs closed momentum" line chart needs a parallel bucket counting **PRs opened** per week (in addition to the closures already computed above).

For each of the same six rolling windows (`w` in `0..5`), count:

| Field | Count rule |
|---|---|
| `opened` | any PR (open or closed at fetch time) where `createdAt` falls in the window |
| `closed_total` | `merged + closed` from the velocity bucket above |
| `net_delta` | `opened - closed_total` (positive = backlog growing that week, negative = shrinking) |

`opened` requires combining the open-PR set (Step 1 fetch) and the closed-since-cutoff set (Step 3 fetch) into a single iteration — every PR's `createdAt` is checked against the window regardless of current state. PRs opened *before* the cutoff but closed *within* the window count for the closed bucket but not the opened bucket (their createdAt is out of window).

Below the chart, print two summary lines computed from these buckets:

```text
Net delta this week: ±<N> PRs (<opened> opened - <closed_total> closed)
6-week net: ±<N> PRs (<sum_opened> opened - <sum_closed> closed) — backlog <growing|shrinking|stable>
```

`stable` is used when `|6-week net| < 10`. Anything bigger reads as a real direction.

The line chart itself is rendered via inline SVG in [`render.md`](render.md). The aggregate layer just produces the per-week numbers — chart geometry (line interpolation, axes, gridlines) is purely a render concern.

### Why opened *and* closed (not just net)

Net alone hides activity. A week with 100 opened and 100 closed has the same net as a week with 0 opened and 0 closed, but the maintainer experience is wildly different — the first is a busy week, the second is dormant. Rendering both lines lets the maintainer see "we're keeping up" vs "nothing's happening" at a glance.

---

## Ready-for-review trend by top areas

The dashboard's "Ready-for-review trend" panel shows the cumulative count of currently-`ready for maintainer review` PRs over the last 6 weeks, broken down by the top-N highest-pressure areas (default N = 5; areas with fewer than 3 currently-ready PRs are excluded as noise).

For each PR currently carrying the label, the **labeled-at timestamp** is the `createdAt` of the most recent `LabeledEvent` where `label.name == "ready for maintainer review"` from the PR's timeline (see [`fetch.md#ready-label-timeline`](fetch.md#ready-label-timeline)).

For each top area `a` and each weekly bucket `w` in `0..5`:

```
ready_count[a][w] = count of currently-ready PRs in area a where labeled_at <= w.end
```

This is a **cumulative** count, not a per-week delta — by construction it's monotonically non-decreasing because PRs that lose the label drop out of the *currently-ready* set entirely (they're not in this dataset).

Render the result as a multi-line chart, one line per area, with the area's pressure-band colour from [`#pressure-score`](#pressure-score) (red / amber / grey lines). Each line ends at the current count visible in the dashboard's hero card.

Below the chart, print a one-line per-area summary:

```text
providers: 46 ready (+8 in last 7d)
task-sdk: 40 ready (+5 in last 7d)
…
```

The "+N in last 7d" is the count of PRs labeled within the last week — surfaces whether the queue is growing faster than it can be reviewed.

### Why cumulative, not weekly-delta

A maintainer looking at the trend wants to see "how is the review backlog evolving" — a steadily-growing line means review velocity isn't keeping up with triage promotion. Per-week deltas would show only the additions and obscure that the queue keeps *being* big. The cumulative view answers the actual question.

---

## Closed by triage reason per week

The dashboard's "Closed-by-triage-reason" panel shows the per-week stacked breakdown of closed/merged PRs by triage outcome. Each closed PR falls into exactly one of four categories:

| Category | Definition | Colour |
|---|---|---|
| `merged` | `merged == true` (regardless of triage state) | green |
| `closed-after-responded` | `merged == false` AND `is_triaged` AND `responded_before_close == true` | amber |
| `closed-after-triage-no-response` | `merged == false` AND `is_triaged` AND `responded_before_close == false` | red |
| `closed-no-triage` | `merged == false` AND NOT `is_triaged` | grey |

For each weekly bucket, count PRs in each category. Render as a 6-row stacked horizontal bar (same layout as the velocity chart — newest at the bottom).

The four colours map directly to maintainer outcomes:

- **green** = success path (PR shipped)
- **amber** = engagement-but-no-merge (author responded but PR didn't make it — could be design rejection, scope change, etc.)
- **red** = stale-sweep / abandonment (triaged then ghosted; usually closed via sweep 1a)
- **grey** = no-triage closure (author closed it themselves, or maintainer closed without going through triage)

A healthy week has a tall green segment with thin amber/red/grey segments. A week dominated by red is a triage-followup pile-up; a week dominated by grey is contributors self-cleaning their own PRs (also fine).

Below the bars, print three summary numbers:

```text
6-week breakdown: <merged_total> merged · <closed_after_responded> engaged-then-closed · <closed_no_response> sweep-closed · <closed_no_triage> no-triage
```

This panel makes the *quality* of closures visible — the velocity panel says "how many", this panel says "of what type".

---

## Health rating

Top-of-dashboard hero card. Computed as a count of fired threshold conditions:

| Condition | Issue points |
|---|---|
| Any contributor non-draft PR untriaged AND > 4 weeks old | **2** |
| > 30 contributor non-draft PRs untriaged AND in 1–4 weeks bucket | **1** |
| > 100 PRs labelled `ready for maintainer review` | **1** |
| > 20 stale-triaged drafts (drafts where triage comment ≥ 7 days old AND no author response) | **1** |

Sum the points and map:

| Total points | Label | Colour |
|---|---|---|
| 0 | `✅ Healthy` | green |
| 1–2 | `⚠️ Needs attention` | amber |
| ≥ 3 | `🔥 Action needed` | red |

The `>4w untriaged` condition is weighted 2x because PRs that have slipped past the 4-week mark without triage are the highest-cost failure mode — they make the project look unresponsive even though everything else may be fine. A single `>4w` PR alone reaches "needs attention".

The thresholds are intentionally conservative — most well-tended repos sit at 0 or 1 issue point. If a maintainer sees the rating regularly hitting "Action needed", that's the signal to schedule a focused triage day.

---

## Cache

Persist `area_stats`, `totals`, the per-area pressure scores, the weekly velocity buckets, and the recommendation list into the scratch cache as JSON. The cache entry is keyed by `(fetch_timestamp, cutoff)` — if the maintainer re-invokes with the same cutoff inside the 15-minute freshness window, render from cache without re-fetching.

The cache is advisory for stats. If a consumer (e.g. a wrapping `loop` that re-runs every 30 minutes) wants live numbers, invalidate the cache explicitly with `clear-cache`.
