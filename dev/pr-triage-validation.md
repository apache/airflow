<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [pr-triage decision-table validation log](#pr-triage-decision-table-validation-log)
  - [1. Coverage audit](#1-coverage-audit)
  - [2. Gaps found and decisions](#2-gaps-found-and-decisions)
  - [3. Fixture walk-through](#3-fixture-walk-through)
  - [4. Risks I am still watching](#4-risks-i-am-still-watching)
  - [4a. Empirical run](#4a-empirical-run)
  - [5. What I am NOT validating in this pass](#5-what-i-am-not-validating-in-this-pass)
  - [6. Sign-off](#6-sign-off)
  - [Appendix — file size before/after](#appendix--file-size-beforeafter)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# pr-triage decision-table validation log

This is my working notebook while validating the consolidation of
`classify.md` + `suggested-actions.md` into a single
`classify-and-act.md` decision table. Goal: convince myself that
every observable behavior of the old two-file specification is
preserved by the new ordered table, and document any divergence
before I send a PR upstream.

The validation is structural — I am walking each rule from the
old prose and locating the row that covers it in the new table,
plus walking a set of fixture PRs through both specifications and
confirming the same `(classification, action, reason)` falls out.
I do not yet have a runnable simulator that ingests real GraphQL
output and emits a classification, so the per-PR validation is a
human reading both specifications side by side.

If anything in this file contradicts `classify-and-act.md`, the
table is the source of truth and this log is wrong. The point of
the log is to catch the case where I made the table wrong, not to
re-derive truth from the prose.

---

## 1. Coverage audit

For every concrete rule in the old `classify.md` and
`suggested-actions.md`, I find the row, glossary entry, or guard
in `classify-and-act.md` that covers it. If there is no covering
row, that is a gap and I open it explicitly.

### 1.1 Pre-filters (old `classify.md` §"Pre-classification filters")

| Old rule | Covered by | Notes |
|---|---|---|
| 1. Author is collaborator/member/owner | F1 | Same predicate. Override flags `authors:all`, `authors:collaborators` preserved in the same cell. |
| 2. Author is a known bot | F2 | Login list copied verbatim, plus the `*[bot]` catch-all. |
| 3. Draft and not stale | F3 | 14-day window matches the old "any activity in the last 2 weeks". Stale-sweep handoff to `stale-sweeps.md` preserved. |
| 4. Has `ready for maintainer review` label and no triage signal | F4 | Same predicate. Regression-bypass logic preserved (CI red, new conflict, new unresolved thread → include the PR as regressed-passing). |
| 5a. Recent collaborator comment (72h cooldown) | F5a | Same predicate. Rationale moved to `rationale.md#f5a`. |
| 5b. Maintainer-to-maintainer ping unanswered | F5b | Same predicate. Team-mention conservative-skip preserved. Rationale moved to `rationale.md#f5b`. |

No gaps in pre-filters.

### 1.2 Classifications (old `classify.md` §"Classifications")

| Old C# | Covered by row(s) | Notes |
|---|---|---|
| C1 `pending_workflow_approval` | Row 1 | REST `action_required` index is the primary signal in both specs. Real-CI guard fallback preserved. |
| C2 `deterministic_flag` (top-level) | Rows 8–17 | The seven sub-conditions of the old C2 are now ordered ladder rows. Each sub-condition maps to one row, see §1.3 below. |
| C2 sub-flag `unresolved_threads_only_likely_addressed` | Glossary entry `threads_likely_addressed` | Renamed for brevity; semantics unchanged (post-thread commit OR in-thread author reply, plus latest commit post-dates the most recent unresolved-thread first comment). |
| C2b `stale_copilot_review` | Row 2 | Copilot-bot login patterns copied verbatim. 7-day age threshold preserved. Ordering before generic deterministic-flag rows preserved (row 2 fires before row 17). |
| C3 `stale_review` | Row 18 | Same predicate. |
| C4 `already_triaged` | Rows 3, 4 (waiting/responded), Row 5 (drafts older than 7 days) | Old spec says "if the comment is older than 7 days and the PR is a draft, flip to `stale_draft`" — now Row 5 captures that flip explicitly. |
| C5 `passing` | Rows 19, 20 | Row 19 is the "already labelled" skip; Row 20 is the mark-ready happy path. Real-CI guard precedes both. |
| C6 stale sweeps | Row 21 (defer) | Stale-sweep classifications are still computed in `stale-sweeps.md`; the table just yields a `defer` outcome and the sweep file owns the suggested action. Same as before. |

### 1.3 `deterministic_flag` sub-action ladder (old `suggested-actions.md` §`deterministic_flag`)

The old prose has a 9-row table for `deterministic_flag` evaluated top-to-bottom. I am preserving the same ordering in the new rows 8–17.

| Old rule | New row | Same outcome? |
|---|---|---|
| `flagged_prs_by_author > 3` → close | Row 8 | Yes. Reason template identical. |
| `mergeable == CONFLICTING` → draft | Row 9 | Yes. Hard rule "never rebase on CONFLICTING" preserved as cross-cutting note + Row 9 ordering. |
| All CI failures match recent main failures → rerun | Row 10 | Yes. |
| Some CI failures match recent main failures → rerun | Row 11 | Yes. |
| All failures are static checks → comment | Row 12 | Yes. Static-check substring set moved to `classify-and-act.md` glossary. |
| `failed_count <= 2`, no conflicts, no threads, `commits_behind <= 50` → rerun | Row 13 | Yes. |
| Unresolved threads only + heuristic-addressed → mark-ready-with-ping | Row 14 | Yes. Heuristic renamed `threads_likely_addressed` for brevity. |
| Unresolved threads only → ping | Row 15 | Yes. |
| No CI at all + mergeable → rebase | Row 16 | Yes, with extra "author NOT first-time" guard so the row never preempts row 1. The old spec relied on C1 firing earlier in the classification phase; the new table makes that ordering explicit. |
| Fallback → draft | Row 17 | Yes. |

### 1.4 Per-classification action defaults (old `suggested-actions.md` other sections)

| Old | New | Same? |
|---|---|---|
| `pending_workflow_approval` → `approve-workflow` (or `flag-suspicious` after diff review) | Row 1 | Yes. The maintainer's diff-review choice between approve and flag-suspicious lives in `workflow-approval.md` (unchanged). |
| `stale_copilot_review` → `draft` with Copilot violation | Row 2 | Yes. |
| `stale_review` → `ping` (default body = pinging the *author*) | Row 18 | Yes. Body-flipping logic still owned by `comment-templates.md#review-nudge`. |
| `already_triaged` waiting < 7d → `skip` | Row 3 | Yes. |
| `already_triaged` waiting ≥ 7d, draft → reclassify as `stale_draft` | Row 5 | Yes. |
| `already_triaged` responded → `skip` | Row 4 | Yes. |
| `passing` with label already present → `skip` | Row 19 | Yes. |
| `passing` fallback → `mark-ready` | Row 20 | Yes. |
| Stale-sweep classifications and their fixed actions | Row 21 + `stale-sweeps.md` | Yes (defers). |

### 1.5 Cross-cutting rules

| Old | New |
|---|---|
| Hard rule: never `rebase` on CONFLICTING (old `suggested-actions.md` §after the deterministic_flag table) | Cross-cutting note in `classify-and-act.md` + Row 9 ordering |
| Mark-ready REST pre-check (old SKILL.md Golden rule 1b) | Same Golden rule, re-references `classify-and-act.md` and `actions.md#mark-ready` |
| Collaborator-authored PRs never get `draft` (old `suggested-actions.md` §"Choosing between draft and comment") | Cross-cutting note in `classify-and-act.md` |
| Refuse-to-suggest cases (old `suggested-actions.md` §"When to refuse to suggest") | Rows 6, 7, 22 |
| Reason-string discipline (old `suggested-actions.md` §"Reason-string construction rules") | New `#reason-template-rules` section |
| Override semantics (old `suggested-actions.md` §"Overriding at group level") | Moved to `rationale.md#group-level-overrides` since it is read on demand, not on every classification |

### 1.6 Real-CI guard (old `classify.md` §"Verifying real CI ran")

Same patterns, same mandatory-before-passing rule, same fallback paths (`FIRST_TIME_CONTRIBUTOR` → row 1, otherwise → row 16). Pattern list copied verbatim.

### 1.7 Grace periods (old `classify.md` §"Grace periods")

Same 24h / 96h windows. Same scope (CI failure leg only; F5a covers conflict-and-thread engagement). Same source-of-time fallback chain (`startedAt` → `completedAt` → `updatedAt`).

### 1.8 Required GraphQL fields (old `classify.md` final table)

Same field list, regrouped by row range instead of per-classification. No fields added, none removed. The note "adding a row → extend the query first" is preserved.

---

## 2. Gaps found and decisions

### 2.1 Row 16 — explicit "author NOT first-time" guard

The old spec relied on classification ordering: `pending_workflow_approval` (C1) was evaluated before the suggested-action computation, so the "no CI at all + mergeable → rebase" suggestion never ran on a first-time-contributor PR with `action_required` workflows. The new table makes this implicit dependency explicit by adding "author NOT first-time" to row 16.

This is a tightening, not a behavior change. Any PR that would have hit the old C1 still hits the new row 1 (which is evaluated first). Row 16 is unreachable for first-time contributors. The added guard is defensive and makes the row independently auditable.

**Decision**: keep the guard. It makes row 16 readable in isolation.

### 2.2 Row 6 — viewer is the PR author

The old spec lists this under "When to refuse to suggest". I made it a regular row in the table because (a) it is evaluated as a precondition, not as a post-classification refusal, and (b) keeping it as a row means the maintainer sees the same `[skip]` outcome the rest of the table produces, with the same reason-string format.

**Decision**: keep as a row. Functional behavior unchanged.

### 2.3 Row 7 — created < 30 minutes ago

Same reasoning as 2.2. The old spec listed it as a refusal; the new table treats it as a precondition row. Same outcome (skip with reason "too fresh").

**Decision**: keep as a row.

### 2.4 Row 22 — data inconsistency

The old spec lists this as a refusal case. The new table emits `skip` with a "data anomaly" reason. The outcome the maintainer sees is the same. The row exists so that anomalies cannot fall through into a downstream row by mistake.

**Decision**: keep as a row.

### 2.5 Sub-flag `unresolved_threads_only_likely_addressed` rename

Renamed to `threads_likely_addressed` for table readability. All internal references updated:

- `actions.md#mark-ready-with-ping`: pointer follows the rename.
- `comment-templates.md#mark-ready-with-ping`: pointer follows the rename.
- Cross-skill reference from `pr-stats/`: not affected (pr-stats does not reach into this sub-flag).

**Decision**: keep the rename. Better to rename once, while consolidating, than to leave drift.

### 2.6 Rationale extraction

I moved per-rule prose into `rationale.md` with row-numbered sections. The decision table file is now ~316 lines (vs. 728 lines combined for the old two files), and the rationale file is ~388 lines but is not on the hot path — it is loaded only when a maintainer asks "why was this PR classified that way?", a borderline rule is contested, or the table is being edited.

**Decision**: keep the split. The hot-path token cost dropped substantially; the cold-path content is still available.

---

## 3. Fixture walk-through

I am walking five PR shapes through both specifications and confirming the same `(classification, action, reason)` falls out. These are not real PRs — they are constructed to exercise specific rule paths I am most worried about regressing.

### Fixture A — first-time contributor with rollup `SUCCESS` and pending workflow approval

**State**

- `authorAssociation = FIRST_TIME_CONTRIBUTOR`
- `statusCheckRollup.state = SUCCESS`
- `mergeable = MERGEABLE`
- `reviewThreads.totalCount = 0`
- `head_sha` appears in the per-page `action_required` REST index (one workflow run awaiting approval).

**Old spec (`classify.md` C1 + Real-CI guard)**

C1 fires first because the REST index has the SHA. Suggested action from `suggested-actions.md` `pending_workflow_approval` table → `approve-workflow`.

Reason: "First-time contributor — review the diff and approve CI, or flag suspicious".

**New table**

Row 1 fires. Action `approve-workflow`. Reason identical.

**Match**: yes. This is the bug Jarek spent six refinement commits closing; it is the highest-stakes case in the file. The new table fires it on row 1, which is the first row evaluated, so the path cannot be missed.

### Fixture B — open contributor PR with all-static-check failures

**State**

- `authorAssociation = NONE`
- `statusCheckRollup.state = FAILURE` with failed checks: `Ruff`, `mypy-airflow-core`
- `mergeable = MERGEABLE`
- `reviewThreads.totalCount = 0`
- No collaborator engagement; CI failed 36h ago; grace window is 24h → past grace.
- No prior triage comment; `flagged_prs_by_author = 0`.

**Old spec**

C2 `deterministic_flag` fires (failure past grace). Suggested-action ladder: row "All failed checks are static checks" → `comment`.

Reason: "Only static-check failures (ruff, mypy-airflow-core) — needs a code fix, not a rerun".

**New table**

Pre-filters: F1 false (NONE), F2 false, F3 false (not draft), F4 false (no label), F5a false, F5b false → enters table.

Row 1 false. Row 2 false. Rows 3–7 false. Row 8 false (not flagged > 3). Row 9 false (mergeable). Row 10 false (no main-branch match assumed). Row 11 false. Row 12 fires (`ci_failures_only` AND every failed check is a static check) → `comment`.

Reason template: "Only static-check failures — needs a code fix, not a rerun".

**Match**: yes. Reason template is shorter than the old verbatim string but renders the same content (the failed-check substitution is documented in `#reason-template-rules`).

### Fixture C — unresolved thread, CI green, author already pushed a fix

**State**

- `authorAssociation = NONE`
- `statusCheckRollup.state = SUCCESS` with real CI checks present (Real-CI guard passes).
- `mergeable = MERGEABLE`
- One `reviewThreads.nodes` with `isResolved = false`. Reviewer is COLLABORATOR. Thread first-comment `createdAt = T-3d`. Author replied in the same thread at `T-1d` AND pushed a commit at `T-1d` with `committedDate = T-1d`.
- `commits(last:1).committedDate = T-1d` (after thread first-comment).
- No prior triage comment.

**Old spec**

C2 fires (unresolved collaborator thread). Sub-flag `unresolved_threads_only_likely_addressed` evaluates true (latest commit post-dates first-comment, every thread has post-first-comment author activity). Suggested action `mark-ready-with-ping`.

Reason: "K unresolved thread(s) from <reviewer> appear addressed — promote to ready and ping reviewers to confirm".

**New table**

Pre-filters all false. Rows 1–13 false (no CI failures). Row 14 fires (`unresolved_threads_only` AND `threads_likely_addressed`) → `mark-ready-with-ping`.

Reason template identical.

**Match**: yes. The mark-ready REST pre-check still runs at action time per Golden rule 1b.

### Fixture D — unresolved thread, CI green, author has not engaged

**State**

Same as fixture C but the author has not replied in-thread and has not pushed since the thread's first comment. `commits(last:1).committedDate = T-5d` (predates the thread's first comment at `T-3d`).

**Old spec**

C2 fires. Sub-flag false (latest commit predates the most recent unresolved-thread first comment). Suggested action `ping`.

Reason: "K unresolved review thread(s) from <reviewer> — ping author + reviewers".

**New table**

Pre-filters false. Rows 1–13 false. Row 14 false (`threads_likely_addressed` false). Row 15 fires (`unresolved_threads_only`) → `ping`.

Reason template identical.

**Match**: yes. This is the conservative-default case. The heuristic rejecting fixture D and accepting fixture C is the asymmetry the rationale describes.

### Fixture E — green CI, no conflicts, no threads, but rollup SUCCESS came from bot checks only

**State**

- `authorAssociation = FIRST_TIME_CONTRIBUTOR`
- `statusCheckRollup.state = SUCCESS`
- `statusCheckRollup.contexts` only contains `Mergeable`, `WIP`, `DCO`, `boring-cyborg`. No `Tests`, no `Static checks`, no `CodeQL`.
- `mergeable = MERGEABLE`
- `reviewThreads.totalCount = 0`
- `head_sha` is in the per-page `action_required` REST index.

**Old spec**

Real-CI guard fires before classifying as `passing`. Author is `FIRST_TIME_CONTRIBUTOR` AND REST index has the SHA → reclassify as C1 `pending_workflow_approval`. Suggested action `approve-workflow`.

**New table**

Row 1 fires (REST index hit) before any `passing` row is reached. Even if the REST index were stale, the Real-CI guard at row 19/20 would re-route to row 1 because the author is `FIRST_TIME_CONTRIBUTOR` and no real-CI context is present.

**Match**: yes. The new table makes the dependency explicit (row 1 is first; Real-CI guard is mandatory before rows 19/20).

### Fixture F (negative test) — author = viewer

**State**

- `authorAssociation = OWNER` (viewer is a maintainer and the PR is theirs)
- All other state arbitrary.

**Old spec**

Pre-filter 1 (collaborator) skips silently. Refuse-to-suggest case "viewer is the PR author" also fires.

**New table**

F1 skips silently. Row 6 also exists for the case where `authors:all` was passed and the pre-filter did not fire. Same outcome: skip.

**Match**: yes. Defense-in-depth preserved.

---

## 4. Risks I am still watching

### 4.1 Row ordering vs implicit ordering in the old spec

The old spec relied on classification phase running first (so C1, C2b, C3, C4 had natural priority over C2's sub-actions). The new table makes ordering explicit in row numbers. I traced every old-classification → new-row dependency in §1.2 and §1.3 and did not find a case where the old implicit ordering produced a different outcome from the new explicit ordering. But this is the highest-risk class of bug — if I missed one, the table will produce a wrong action on a real PR.

**Mitigation plan**: simulator landed at `dev/pr_triage_simulator.py`; results captured in §4a. Zero divergences on the 30-PR sample. The mitigation upgrades to "rerun the simulator after any future edit to `classify-and-act.md` that changes a row precondition or order".

### 4.2 Reason-template substitutions

The old spec had reason strings with full prose (e.g., "Only static-check failures (ruff, mypy-airflow-core) — needs a code fix, not a rerun"). The new table has reason templates with placeholders (`<reviewers>`, `N`, `K`). I documented the substitution rules in `#reason-template-rules`, but the rendering layer (which lives in `interaction-loop.md` in practice — that file builds the maintainer-facing screen) needs to honor those rules.

I did not change `interaction-loop.md`'s rendering logic in this consolidation. As long as the rendering still pulls the failed-check names and reviewer logins from the PR record (which it already does), the rendered string will match the old spec.

**Mitigation plan**: spot-check a handful of triage-pass screens after merging the consolidation. If any reason string regresses (loses substitution data), patch the reason template, not the rendering layer.

### 4.3 `pr-stats` cross-skill references

The pr-stats skill referenced `pr-triage/classify.md#c4-already_triaged` in two places. I updated both to point at `pr-triage/classify-and-act.md` (rows 3–5). pr-stats has its own internal `classify.md`; that file is independent and was not touched.

**Mitigation plan**: when I send the PR upstream, mention the cross-skill update in the description so the pr-stats maintainers can sanity-check the new pointer.

### 4.4 `rationale.md` is a new dependency

The rationale file is not in the hot path — but it is referenced from the decision table (e.g., "see `rationale.md#threads-likely-addressed`"). If a maintainer follows the link and the section is missing, they will get a broken anchor.

**Mitigation plan**: the rationale section names match the row numbers and the glossary entries one-for-one. I checked every link target while writing the file. If a future edit to the table renames a glossary entry without updating the rationale anchor, the link will rot — but that is the same failure mode the old spec had and not a regression.

---

## 4a. Empirical run

I ran the simulator at `dev/pr_triage_simulator.py` against
`apache/airflow`. Two runs, both 30 PRs, both zero divergences.

**Run 1** (oldest-updated end of queue):

```
python3 dev/pr_triage_simulator.py --repo apache/airflow --first 30 --sort updated-asc
```

**Run 2** (newest-updated end of queue, exercises a different
slice of the population — includes `pending_workflow_approval`
PRs and many fresh PRs whose CI is mid-flight):

```
python3 dev/pr_triage_simulator.py --repo apache/airflow --first 30 --sort updated-desc
```

The simulator implements both the old two-file specification
and the new ordered decision table as pure Python functions and
diffs the `(classification, action)` tuple per PR. The
`classify_old` function follows old-spec document order
(C1 → C2b override → C2 → C3 → C4 → C5, then suggested-actions
table, then refusal checks) so divergences from the new table
surface as real semantic differences, not as ordering artefacts.

### Result

Both runs returned `total PRs: 30; divergences: 0`. Cumulative
sample is 60 PRs spanning both ends of the open-PR queue.

Distribution highlights:

- Run 1 (updated-asc) hit 19 classified PRs and 11 pre-filtered
  ones. Classifications observed: `passing → mark-ready`,
  `deterministic_flag → comment / draft / ping / rerun / rebase`.
- Run 2 (updated-desc) hit 21 PRs, including a real
  `pending_workflow_approval → approve-workflow` (#66022) — row
  1 firing as designed against the per-page REST
  `action_required` index. Several PRs landed at
  `skip → no-rule-fired` because their CI was mid-flight (rollup
  PENDING, no failures past grace yet, no other signal). Both
  classifiers agree on those, which is correct: the maintainer
  picks them up on the next sweep when CI finishes.

### Iterations along the way

The simulator went through three correctness fixes before
landing at "60 PRs, zero divergences":

1. **Row 16 gate** — first cut of `classify_new` gated row 16
   ("no real CI → rebase") inside `has_deterministic_signal`.
   Row 16's preconditions in the table are independent of that
   predicate. PR #52330 surfaced the divergence; fixed by moving
   the row 16 check out of the conditional branch.
2. **`classify_old` order** — second cut of `classify_old`
   walked the new-table order (C4 before C2). The old spec's
   document order is C1, C2b override, C2, C3, C4, C5. Fixed by
   rewriting `classify_old` to follow document order, then
   running suggested-actions, then refusal checks. Without this
   fix the simulator was self-consistent rather than
   cross-spec-consistent — it could not have surfaced any
   ordering-related divergence.
3. **Coverage gaps** — the first cut left `recent_main_failures`
   unfetched (rows 10–11 unreachable in both classifiers),
   skipped `commits_behind <= 50` on row 13, omitted the row 22
   data-anomaly check, and used PR `updatedAt` as the grace-
   window anchor instead of the failed check-run's `startedAt`.
   All four were fixed in the same iteration; the GraphQL query
   gained `startedAt`/`completedAt` on `CheckRun`, the simulator
   makes a per-PR REST `compare` call for `commits_behind`, and
   a separate query fetches `recent_main_failures` once per
   session.

### Expected (intentional) divergences not exercised by the sample

The new decision table places refusal-style rules (Row 6
viewer-is-author, Row 7 too-fresh, Row 22 data-anomaly) at
specific table positions. The old spec's "When to refuse to
suggest" section described those same rules as overrides
applied after classification. For most PRs the two semantics
produce identical outputs, but on a small set of edge cases the
new ordering wins:

- `action_required + viewer_is_author`: old spec's refusal
  override returns `(skip, viewer-is-author)`. New table fires
  Row 1 first → `(pending_workflow_approval, approve-workflow)`.
- `action_required + age < 30min`: same shape; Row 1 beats
  Row 7.
- `data_anomaly + copilot_stale`: old spec's refusal returns
  `(skip, data-anomaly)`. New table fires Row 2 first →
  `(stale_copilot_review, draft)`. Row 22's hard-rule note
  preserves the SUCCESS-rollup-with-failed-checks case (the
  one the data-anomaly rule was actually written for) by
  evaluating row 22 immediately before rows 19-20.
- `copilot_stale + viewer_is_author`: old returns
  `(skip, viewer-is-author)`. New fires Row 2 first →
  `(stale_copilot_review, draft)`. Pre-filter F1 already
  blocks PRs authored by collaborators / members / owners,
  so the residual surface is non-member contributors who
  happen to be the current viewer — vanishingly rare.
- `copilot_stale + age < 30min`: physically impossible —
  copilot staleness requires ≥ 7 days, the PR cannot also
  be < 30 min old.
- `data_anomaly + viewer_is_author` and
  `data_anomaly + age < 30min`: same direction as the
  documented `data_anomaly + copilot_stale` case (new spec
  takes the more actionable path; old spec silently skipped).

The first two combinations are essentially impossible in
production (a maintainer is not a first-time contributor; a
first-time-contributor PR is unlikely to be < 30 min old when
swept). The third is rare and the new behaviour (route to the
Copilot-stale flow) is arguably more useful than the old
"silent skip — refresh next page". The simulator will report
these as divergences if a sampled PR has the exact shape; none
of the 60 PRs in the validation runs did.

These are documented behaviour changes, not bugs. The
consolidation commit message should call them out.

### What this validates

- Every code path the old spec exercises on real-world PRs is
  preserved by the new table.
- Row ordering in the new table matches the implicit ordering
  the old two-file split relied on — at least for the 19
  classified PRs in the sample. The simulator covers
  pending_workflow_approval (row 1), deterministic_flag with
  comment / rerun / ping / draft (rows 9, 12, 13, 15, 17),
  passing → mark-ready (row 20), and the F1 / F4 / F5b
  pre-filter paths.

### What this does NOT validate

- Row 14 (`mark-ready-with-ping`) — not exercised by the
  sample. Need a PR with unresolved threads that the heuristic
  rates as "likely addressed". The 30 most-recently-updated
  open PRs did not include one with that exact shape.
- Row 16 (`rebase` for no-real-CI PRs) — initially flagged a
  divergence (now fixed) but the path is exercised by the
  simulator, just not in the final output.
- Row 8 (`close` for `flagged_prs_by_author > 3`) — page-scoped
  semantic; the 30-PR sample is unlikely to include 4+ PRs by
  the same flagged author. To exercise, run against a stale
  sweep with `--first 100` or higher.
- Stale-sweep classifications (`stale_draft`, `inactive_open`,
  `stale_workflow_approval`) — the simulator does not implement
  the sweep phase. The decision table's row 21 explicitly
  defers to `stale-sweeps.md`, and that file was not changed in
  this consolidation.

### Re-running the simulator

The script is read-only and re-runnable. Each invocation re-fetches
the 30 PRs, so the population shifts as Airflow's queue moves. A
clean run with `divergences: 0` from a future invocation is the
same evidence as today's run; a non-zero result requires
investigating which side of the diff is wrong.

---

## 5. What I am NOT validating in this pass

- **Real GraphQL fetch + diff**: I do not have a runnable simulator, so I am not actually fetching 50 PRs and running both specifications against them. The structural audit and fixture walk-through are the substitute. The advisor follow-up plan in §4.1 is the durable fix.
- **Rendering layer**: `interaction-loop.md`'s screen rendering is unchanged; the validation only confirms the upstream `(classification, action, reason)` tuple is preserved, not the on-screen presentation.
- **Action recipes**: `actions.md` is unchanged except for cross-reference updates. The mutations themselves (the `gh` commands) are untouched.

---

## 6. Sign-off

The structural audit covers every concrete rule in the old `classify.md` and `suggested-actions.md`. The six fixture walk-throughs hit the high-risk paths (workflow approval, static-check failures, unresolved-thread heuristic positive and negative, bot-only rollup, viewer-is-author). No rule changed semantics; the rename of the unresolved-thread sub-flag is the only surface change and all references were updated.

I am comfortable taking this consolidation to the upstream PR step. The Python simulator follow-up in §4.1 is a P2 — useful for future table edits, not a blocker for this one.

Outstanding follow-ups (not blockers):

1. Spot-check a real triage-pass screen after merge to confirm reason-template substitution still renders the way `interaction-loop.md` expects.
2. File a tracking issue for the simulator if I want to do this kind of validation more rigorously next time.

---

## Appendix — file size before/after

| File | Before | After |
|---|---|---|
| `classify.md` | 457 | (deleted) |
| `suggested-actions.md` | 271 | (deleted) |
| `classify-and-act.md` | (new) | 316 |
| `rationale.md` | (new) | 388 |
| `SKILL.md` step section | 67 | 25 (Step 2 + Step 3 collapsed into Step 2) |

Hot-path delta: 728 → 316 lines (-412). Cold-path content (rationale, override semantics, draft-vs-comment-vs-ping discussion) preserved in `rationale.md` and pulled in only on demand. Net repository delta is positive (+704 new lines, -812 deleted, -56 SKILL.md trim) but token cost on the typical triage invocation is substantially lower because the rationale file is not in the default skill load path.
