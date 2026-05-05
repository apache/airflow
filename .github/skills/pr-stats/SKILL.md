---
name: pr-stats
description: |
  Read-only stats on the open-PR backlog of apache/airflow — two area-grouped
  tables (triaged final-state and triaged still-open) with age buckets, so
  maintainers can see where queue pressure sits.
when_to_use: |
  When the user asks "how is the PR queue doing", "run PR stats", "show the
  area breakdown", or "how many PRs are still waiting on authors". Good as a
  health check before or after a triage sweep.
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- Placeholder convention:
     <repo>   → target GitHub repository in `owner/name` form (default: apache/airflow)
     <viewer> → the authenticated GitHub login of the maintainer running the skill
     Substitute these before running any `gh` command below. -->

# pr-stats

Read-only skill that answers "what does the open-PR backlog
*look* like" as two tables:

| Table | Row set | Purpose |
|---|---|---|
| **Triaged final-state** | closed / merged PRs since a cutoff date, broken down by `area:*` label | Shows triage outcomes — what fraction of triaged PRs merged, closed, or got an author response before closing. |
| **Triaged still-open** | all currently-open PRs, broken down by `area:*` label | Shows current queue pressure — triage coverage, author-response rate, ready-for-review count, age buckets. |

The skill is the statistical complement of [`pr-triage`](../pr-triage/SKILL.md) — same repo, same classification logic, no mutations. Running the two in sequence (stats → triage → stats) lets a maintainer measure a sweep's effect.

Detail files:

| File | Purpose |
|---|---|
| [`fetch.md`](fetch.md) | GraphQL templates for open-PR list and closed/merged-since-cutoff list. |
| [`classify.md`](classify.md) | Triage-status detection (waiting vs. responded vs. never-triaged) — reuses the `Pull Request quality criteria` marker from `pr-triage`. |
| [`aggregate.md`](aggregate.md) | Area grouping, age buckets, totals, percentage rules. |
| [`render.md`](render.md) | The two tables — column order, footers, header wording. |

---

## Golden rules

**Golden rule 1 — no mutations, ever.** This skill only reads. It must not post comments, add labels, close, rebase, or approve anything. If the maintainer asks for stats and also wants an action, decline the mutation and redirect to `pr-triage`.

**Golden rule 2 — reuse pr-triage's triage-detection.** The "triaged" count and "responded" count depend on the same `Pull Request quality criteria` marker string and the same collaborator set (`OWNER`/`MEMBER`/`COLLABORATOR`) that drive the triage-marker rows in `pr-triage/classify-and-act.md` (rows 3–4 — `already_triaged`). Don't invent a second definition — both skills must agree on "is this PR triaged".

**Golden rule 3 — one GraphQL call per batch, not per PR.** Same rule as `pr-triage/fetch-and-batch.md`. One aliased query covers the open-PR list for a whole page; the closed/merged fetch is paginated by GitHub's search cursor. Never call `gh pr view` per PR.

**Golden rule 4 — include a legend with every render.** The tables are dense (15+ columns on Table 2). Always print a short legend after the tables explaining the columns — `Contrib.` = non-collaborator, `Responded` = author replied after the triage comment, `Drafted by triager` = PR converted to draft by the viewer, etc. Nobody remembers column abbreviations in isolation.

**Golden rule 5 — state the input scope up front.** Before rendering, print one line summarising what the stats cover: repo name, total open PR count, closed-since cutoff date, and viewer login. The numbers only make sense in context.

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
3. Read or initialise the scratch cache at `/tmp/pr-stats-cache-<repo-slug>.json` (see [`aggregate.md#cache`](aggregate.md)). The cache stores the viewer login and a map of `pr_number → (head_sha, triage_status)` so a re-run inside the same session skips the per-PR enrichment.

A failure at step 1 is a **stop**. Steps 2 and 3 degrade with warnings.

---

## Step 1 — Fetch open PRs

Use the query template in [`fetch.md#open-prs`](fetch.md) to get every open PR with the fields needed for classification (labels, `isDraft`, `authorAssociation`, `createdAt`, last commit `committedDate`, last 10 comments for the triage-marker scan).

Paginate until `pageInfo.hasNextPage == false`. Batch size of 50 is safe (the open-PR selection set is lighter than `pr-triage`'s — no `statusCheckRollup`, no `reviewThreads`, no `latestReviews`). For a 300-PR backlog that's six GraphQL calls.

---

## Step 2 — Classify triage status per PR

For each open PR, determine:

- `is_triaged_waiting` — viewer's (or any collaborator's) comment contains the `Pull Request quality criteria` marker, the comment post-dates the PR's last commit, AND the author has NOT commented after it.
- `is_triaged_responded` — same marker found, but the author HAS commented after it.
- `is_drafted_by_triager` — the PR was converted to draft by the viewer at or after the triage comment (from the `ConvertToDraftEvent` timeline, optional — see [`classify.md#drafted-by-triager`](classify.md) for the cheaper heuristic).
- `last_author_interaction_at` — most recent `commit.committedDate` OR author comment `createdAt`, whichever is later.

Cache these per `(pr_number, head_sha)` so a subsequent run skips the scan.

---

## Step 3 — Fetch closed / merged triaged PRs since cutoff

The second table is a separate search. Fetch closed or merged PRs whose comment history contains the triage marker since the configured cutoff date. Use the template in [`fetch.md#closed-merged-triaged`](fetch.md).

Cutoff defaults to `today - 6 weeks`. The cutoff should be configurable because a maintainer asking "how did last week's sweep do" wants `since:today-7d`, while a monthly report wants `since:today-30d`.

---

## Step 4 — Aggregate by area

Group each PR by every `area:*` label it carries. A PR with `area:UI` and `area:scheduler` contributes to both groups. A PR with no `area:*` labels lands in a pseudo-area `(no area)`.

Per area, compute the counters in [`aggregate.md#counters`](aggregate.md): total, drafts, non-drafts, contributors, triaged-waiting, triaged-responded, ready-for-review, drafted-by-triager, plus age-bucket histograms.

Also compute a `TOTAL` row where each PR is counted exactly once (NOT the sum of per-area counters — PRs with multiple `area:*` labels would double-count).

---

## Step 5 — Render

Emit the two tables in the order defined by [`render.md`](render.md):

1. **Triaged PRs — Final State since `<cutoff>`** — one row per area where `Triaged Total > 0`.
2. **Triaged PRs — Still Open** — one row per area where `Total > 0`, plus the `TOTAL` row.
3. **Legend** — one short paragraph explaining the non-obvious columns.

The tables are Markdown (GitHub-flavoured) so the same output renders cleanly in the CLI, in a Slack paste, or pasted into a GitHub comment.

---

## What this skill does NOT do

- **No mutations.** See Golden rule 1.
- **No per-PR drill-in.** The output is aggregate — if the maintainer wants to inspect a specific PR, they run `pr-triage pr:<N>` or open it in the browser.
- **No timeline / trend charts.** A single snapshot per invocation. Tracking week-over-week is the maintainer's job — re-run the skill at a different `since:` date if needed.
- **No author-level stats.** Grouping is by area label, not by author login. A stats-by-author skill is a separate scope.
- **No PR *quality* scoring.** CI pass/fail, diff size, and review-thread counts are all omitted from the aggregate — they belong in the per-PR `pr-triage` view.

---

## Budget discipline

Typical session against `apache/airflow`:

- 1 pre-flight query (viewer + repo)
- ~6 paginated GraphQL calls for ~300 open PRs (50 per page)
- ~2 paginated calls for closed/merged-since-cutoff (typically 20–80 PRs per week of cutoff)
- No per-PR REST calls — the comment scan for triage markers is done from the `comments(last: 10)` subfield in the open-PR query

Total budget: ~10 GraphQL calls regardless of repo size. Well under 5% of the hourly budget.
