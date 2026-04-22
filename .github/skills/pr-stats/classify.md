<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Classify

Per-PR state determination for the stats tables. Mirrors the triage-detection logic in [`pr-triage/classify.md#c4-already_triaged`](../pr-triage/classify.md) — the two skills must agree on what "triaged" means. Any rule change here must ship simultaneously in `pr-triage`.

Classification is pure function of state from [`fetch.md`](fetch.md) — no network calls, no writes.

---

## Triage marker

A PR is *triaged* when it has at least one comment that:

- is authored by `OWNER` / `MEMBER` / `COLLABORATOR` (`authorAssociation`)
- contains the literal string `Pull Request quality criteria` in `bodyText` (the canonical quality-criteria link text from [`pr-triage/comment-templates.md`](../pr-triage/comment-templates.md))
- has `createdAt` **after** the PR's last commit's `committedDate` (otherwise the triage pre-dates the current code and is stale)

Why "any maintainer", not "viewer only": if another maintainer already triaged the PR, the stats should count it as triaged. Using `viewer` here would under-count triage coverage on a team with multiple active triagers.

---

## Triaged sub-states

Once a PR is triaged, it's either *waiting* for the author or the author has *responded*:

### `triaged_waiting`

- PR is triaged (above)
- The PR's `author.login` has **not** commented after the triage comment's `createdAt`

### `triaged_responded`

- PR is triaged
- The PR's `author.login` has commented at least once after the triage comment's `createdAt`

A PR pushed a new commit after the triage counts as "responded" too — treat a post-triage commit the same as a post-triage comment for this test (the commit's `committedDate` serves as the author-activity timestamp).

---

## Drafted by triager

A PR is *drafted by triager* when the viewer (or any maintainer) converted the PR to draft *after* having posted the triage comment. Two ways to detect this:

### Full signal — `ConvertToDraftEvent`

Query the PR's timeline and find the most recent `ConvertToDraftEvent`:

```graphql
pullRequest(number: $n) {
  timelineItems(last: 50, itemTypes: [CONVERT_TO_DRAFT_EVENT]) {
    nodes {
      ... on ConvertToDraftEvent {
        actor { login }
        createdAt
      }
    }
  }
}
```

If `actor.login` is the viewer (or any maintainer login tracked in the session cache) and `createdAt >= triage_comment_createdAt`, mark the PR as `drafted_by_triager` with `drafted_at = createdAt`.

This is the accurate signal but it's a per-PR query. Run it only when the maintainer asks for the `draft_age_buckets` column (render it in Table 2 by default).

### Cheaper heuristic — "is draft + has triage marker"

If you want to skip the timeline query, approximate: treat the PR as `drafted_by_triager` when both `isDraft == true` and `is_triaged` are true. This misclassifies PRs that were already draft *before* triage (e.g. the author opened as draft and then got triaged for a quality issue), but those are rare enough that the approximation is usually fine for a quick stats run.

Mark which path the skill used in the legend output (`drafted by triager (heuristic)` vs `drafted by triager (timeline-confirmed)`) so the maintainer knows the cost/accuracy trade-off.

---

## Age bucket

The age of a PR for bucketing is the time since the author's *last interaction*:

```
last_author_interaction = max(
    most_recent_comment.createdAt where comment.author.login == pr.author.login,
    last_commit.committedDate,
    pr.createdAt,
)
```

Why `max`: a PR freshly opened without activity still needs *some* age signal — `createdAt` is the floor. A PR where the author commented after pushing a commit should be counted by the comment timestamp, not the commit.

Bucket boundaries (delta from `<now>`):

| Bucket label | Range |
|---|---|
| `<1d` | 0–24h |
| `1-3d` | 24h–72h |
| `3-7d` | 72h–7 days |
| `1-2w` | 7–14 days |
| `2-4w` | 14–28 days |
| `>4w` | over 28 days |

Same boundaries are used for the `draft_age_buckets` column (time since the triager converted the PR to draft).

Keep the bucket labels and boundaries in sync with the column headers in [`render.md`](render.md) — the tables read the labels straight off this list.

---

## Contributor vs collaborator

A PR is by a *contributor* (for the `Contrib.` column) when:

```
authorAssociation NOT IN (OWNER, MEMBER, COLLABORATOR)
```

Everything else (including `FIRST_TIME_CONTRIBUTOR`, `FIRST_TIMER`, `CONTRIBUTOR`, `NONE`) counts as contributor. Bots (`[bot]`-suffixed logins or `dependabot` / `github-actions`) are NOT contributors — they're a separate class and should be excluded from the open-PR stats entirely. Filter bots at fetch time, not at classification time, so the denominator in every percentage excludes them.

---

## Ready for review

The `Ready` column counts PRs carrying the `ready for maintainer review` label. That's it — no state inference. The label is the signal.

---

## Responded before close (Table 1 only)

Table 1's `Responded` column measures, per area, how many triaged PRs got an author reply *before* they were closed or merged. For a PR in the closed-since set:

```
responded_before_close =
    is_triaged AND
    exists(comment by pr.author where comment.createdAt > triage_comment.createdAt AND comment.createdAt <= pr.closedAt)
```

Count the PR as responded if it has the marker AND an author comment between triage and close. `%Responded` = responded / triaged_total for that area.

---

## Re-classification stability

The stats run must produce the same numbers when invoked twice on the same cached state. Keep the classification pure (no time-dependent randomness) and anchor age-bucket cutoffs to `<now>` captured at fetch start, not at render time. Otherwise a slow run drifts PRs across buckets between fetch and render.
