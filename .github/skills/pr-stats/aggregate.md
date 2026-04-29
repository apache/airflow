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

## Cache

Persist `area_stats` and `totals` into the scratch cache as JSON. The cache entry is keyed by `(fetch_timestamp, cutoff)` — if the maintainer re-invokes with the same cutoff inside the 15-minute freshness window, render from cache without re-fetching.

The cache is advisory for stats. If a consumer (e.g. a wrapping `loop` that re-runs every 30 minutes) wants live numbers, invalidate the cache explicitly with `clear-cache`.
