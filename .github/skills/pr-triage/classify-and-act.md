<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Classify and act

Single-source decision file. Replaces the previous split between
`classify.md` and `suggested-actions.md`. Combines the two steps
that the old SKILL.md called Step 2 (classify) and Step 3
(suggest action) into one ordered decision table.

Reading order:

1. [Pre-filters](#pre-filters) — drop PRs that should never reach
   the maintainer this run.
2. [Decision table](#decision-table) — first-match-wins. Each row
   yields `(classification, action, reason)`.
3. [Precondition glossary](#precondition-glossary) — named
   compound predicates referenced from the table.
4. [Real-CI guard](#real-ci-guard) — mandatory check before any
   row that classifies a PR as `passing`.
5. [Grace periods](#grace-periods) — defers CI-failure flagging.
6. [Required GraphQL fields](#required-graphql-fields) — what the
   batch query must populate for this file to work.

Rationale, heuristic notes, override semantics, and the
draft-vs-comment-vs-ping discussion live in
[`rationale.md`](rationale.md). This file stays normative and
short; it is the only one the skill needs at decision time.

Classification + action selection is a **pure function of state**
populated by the single batched GraphQL query in
[`fetch-and-batch.md`](fetch-and-batch.md). No network calls, no
prompts, no writes.

---

## Pre-filters

Run these **before** the decision table. A PR that matches any
filter is skipped silently from the main triage flow.

| # | Filter | Match condition |
|---|---|---|
| F1 | Author is collaborator/member/owner | `authorAssociation ∈ {OWNER, MEMBER, COLLABORATOR}` (override: `authors:all` or `authors:collaborators`) |
| F2 | Author is a known bot | login is `dependabot`, `dependabot[bot]`, `renovate[bot]`, `github-actions`, `github-actions[bot]`, or matches `*[bot]` |
| F3 | Draft and not stale | `isDraft == true` and any activity within the last 14 days. Stale-sweep classifications in [`stale-sweeps.md`](stale-sweeps.md) may still pull the PR back in. |
| F4 | Already marked ready, no regression | `labels` contains `ready for maintainer review` AND CI green AND `mergeable != CONFLICTING` AND no unresolved threads. Regression (any of: CI red, new conflict, new unresolved thread *after* the label was applied) bypasses this filter — surface as a regressed-passing entry so the maintainer can decide whether to pull the label. |
| F5a | Recent collaborator comment (author cooldown) | Most recent comment is by a `COLLABORATOR`/`MEMBER`/`OWNER`, `createdAt < 72h` ago, AND posted after `commits(last:1).committedDate`. |
| F5b | Maintainer-to-maintainer ping unanswered | Most recent collaborator comment `@`-mentions one or more logins other than the PR author AND none of those mentioned logins have posted on the PR or in `latestReviews` after that comment. Team mentions (e.g. `@apache/airflow-committers`) are conservatively treated as F5b matches. |

F5a and F5b override every signal in the decision table — they
are not weighed against conflicts, failing CI, or unresolved
threads. See [`rationale.md#pre-filter-5-active-maintainer-conversation`](rationale.md#pre-filter-5-active-maintainer-conversation) for the
why.

---

## Decision table

PRs that survive the pre-filters are evaluated against the rows
below **top-to-bottom**. The first matching row wins; stop on
match. Each row produces a `(classification, action, reason)`
tuple consumed by [`interaction-loop.md`](interaction-loop.md).

Reasons in the table are templates; placeholder substitution is
described in [`#reason-template-rules`](#reason-template-rules).
Action verbs are defined in [`actions.md`](actions.md).

| #  | Precondition (all must hold)                                                                  | Classification             | Action                  | Reason template |
|----|-----------------------------------------------------------------------------------------------|---------------------------|------------------------|-----------------|
| 1  | `head_sha` appears in the per-page `action_required` REST index, OR ([`first_time_no_real_ci`](#first_time_no_real_ci))     | `pending_workflow_approval` | `approve-workflow`     | First-time contributor — review the diff and approve CI, or flag suspicious |
| 2  | [`copilot_review_stale`](#copilot_review_stale)                                               | `stale_copilot_review`     | `draft` (include the specific Copilot thread URL in the violation body) | Unaddressed Copilot review ≥ 7 days old — convert to draft |
| 3  | Viewer comment containing the triage marker exists, posted after last commit, age < 7 days, sub-state `waiting` | `already_triaged`         | `skip`                 | Already triaged M days ago — still waiting on author |
| 4  | Same as #3 but sub-state `responded`                                                           | `already_triaged`          | `skip`                 | Already triaged M days ago — author responded, maintainer to re-engage |
| 5  | Viewer triage marker exists, posted after last commit, sub-state `waiting`, age ≥ 7 days, `isDraft == true` | `stale_draft`     | (defer to [`stale-sweeps.md`](stale-sweeps.md) Sweep 1a) | Draft triaged N days ago, no author reply |
| 6  | `viewer == pr.author.login`                                                                   | n/a                        | `skip`                 | You are the PR author — triage skipped |
| 7  | `now - createdAt < 30min`                                                                      | n/a                        | `skip`                 | Too fresh — CI still warming up |
| 8  | `flagged_prs_by_author > 3` AND [`has_deterministic_signal`](#has_deterministic_signal)        | `deterministic_flag`       | `close`                | Author has N flagged PRs — suggest closing to reduce queue pressure |
| 9  | `mergeable == CONFLICTING`                                                                    | `deterministic_flag`       | `draft`                | Merge conflicts with `<base>` — author must rebase locally; convert to draft with merge-conflicts violation |
| 10 | [`ci_failures_only`](#ci_failures_only) AND every failure ∈ `recent_main_failures`             | `deterministic_flag`       | `rerun`                | All N CI failures also appear in recent main-branch PRs — likely systemic, suggest rerun |
| 11 | [`ci_failures_only`](#ci_failures_only) AND any failure ∈ `recent_main_failures`               | `deterministic_flag`       | `rerun`                | K/N CI failures match recent main-branch PRs — likely systemic |
| 12 | [`ci_failures_only`](#ci_failures_only) AND every failed check is a [static check](#static_check) | `deterministic_flag`     | `comment`              | Only static-check failures — needs a code fix, not a rerun |
| 13 | [`ci_failures_only`](#ci_failures_only) AND `failed_count <= 2` AND `commits_behind <= 50`     | `deterministic_flag`       | `rerun`                | N CI failure(s) on otherwise clean PR — likely flaky, suggest rerun |
| 14 | [`unresolved_threads_only`](#unresolved_threads_only) AND [`unresolved_threads_only_likely_addressed`](#unresolved_threads_only_likely_addressed) | `deterministic_flag` | `mark-ready-with-ping` | K unresolved thread(s) from <reviewers> appear addressed (post-review commits / in-thread author replies) — promote to ready and ping reviewers to confirm |
| 15 | [`unresolved_threads_only`](#unresolved_threads_only)                                          | `deterministic_flag`       | `ping`                 | K unresolved review thread(s) from <reviewers> — ping author + reviewers |
| 16 | No real CI ran (see [Real-CI guard](#real-ci-guard)) AND `mergeable != CONFLICTING` AND author NOT first-time | `deterministic_flag` | `rebase`            | No real CI checks triggered, branch mergeable — rebase to re-trigger |
| 17 | [`has_deterministic_signal`](#has_deterministic_signal) (fallback)                             | `deterministic_flag`       | `draft`                | Has quality issues — convert to draft with violations comment |
| 18 | `latestReviews` has CHANGES_REQUESTED AND author committed after AND NOT [`follow_up_ping`](#follow_up_ping) | `stale_review`         | `ping`                 | Author pushed commits after CHANGES_REQUESTED from <reviewers> but no follow-up — ping |
| 19 | All of: `statusCheckRollup.state == SUCCESS`, `mergeable != CONFLICTING`, no unresolved threads, [Real-CI guard](#real-ci-guard) passes, label `ready for maintainer review` already present | `passing` | `skip` | Already marked ready for review |
| 20 | All of: `statusCheckRollup.state == SUCCESS`, `mergeable != CONFLICTING`, no unresolved threads, [Real-CI guard](#real-ci-guard) passes | `passing` | `mark-ready` | All checks green, no conflicts, no unresolved threads — mark for deeper review |
| 21 | Stale-sweep candidate (see [`stale-sweeps.md`](stale-sweeps.md)) AND no row 1–20 matched in this session | `stale_draft` / `inactive_open` / `stale_workflow_approval` | (per sweep) | (per sweep) |
| 22 | Data inconsistency (rollup SUCCESS but `failed_checks` non-empty, or similar). Evaluated **before** rows 19-20 — see [hard rules](#hard-rules-cross-cutting-the-table) | n/a | `skip`                 | Data anomaly — rollup not yet settled, retry next page |

### Hard rules cross-cutting the table

- **Never suggest `rebase` on a CONFLICTING PR.** GitHub's
  update-branch endpoint does a side-merge and refuses on
  conflicts. Row 9 catches this before any rebase row can fire.
  Action-side guard: see the `rebase` recipe in [`actions.md`](actions.md).
- **Never `mark-ready` while workflow approval is pending.**
  Row 1 catches the upstream signal; the
  [`mark-ready` action](actions.md#mark-ready) re-checks the
  REST `action_required` index immediately before mutating
  (Golden rule 1b in [`SKILL.md`](SKILL.md)).
- **Collaborator-authored PRs never get `draft`.** When
  `authors:collaborators` is active, fall back to `comment` with
  the same body. Row 9 / 17 / etc. emit `comment`, not `draft`,
  in that mode.
- **Row 22 fires before rows 19-20.** A PR matching the row 22
  precondition (rollup SUCCESS but `failed_checks` non-empty,
  or similar inconsistency) must not reach the `passing` rows.
  Implementations evaluate row 22's precondition immediately
  before evaluating row 19; the row's table position (last) is
  documentary, not evaluation order.

---

## Precondition glossary

Compound predicates referenced from the decision table. Defined
once here so the table rows stay short and unambiguous.

### `has_deterministic_signal`

At least one of:

- `mergeable == CONFLICTING`
- `statusCheckRollup.state == FAILURE` AND PR is past its
  [grace window](#grace-periods)
- `reviewThreads.totalCount` ≥ 1 with `isResolved == false` AND
  the thread's reviewer is `COLLABORATOR`/`MEMBER`/`OWNER`

### `ci_failures_only`

`has_deterministic_signal` is true AND the *only* signal that
fired is the CI-failure one (no `CONFLICTING`, no unresolved
collaborator threads).

### `unresolved_threads_only`

`has_deterministic_signal` is true AND the *only* signal that
fired is unresolved threads (`statusCheckRollup.state` is
`SUCCESS`, `mergeable != CONFLICTING`).

### `unresolved_threads_only_likely_addressed`

All of:

- The PR's latest `committedDate` is **after** the most recent
  unresolved-thread first-comment `createdAt`.
- For every unresolved thread, either the author has replied
  in-thread (`comments.nodes.author.login == pr.author.login`
  AND `createdAt >` first-comment `createdAt`) OR a commit was
  pushed after the thread's first-comment `createdAt`.

Heuristic, conservative on purpose. Rationale:
[`rationale.md#unresolved_threads_only_likely_addressed-heuristic-detail`](rationale.md#unresolved_threads_only_likely_addressed-heuristic-detail).

### `copilot_review_stale`

The PR has at least one unresolved review thread whose first
comment author matches a Copilot bot login (case-insensitive:
`copilot-pull-request-reviewer[bot]`, `copilot[bot]`,
`github-copilot[bot]`, or any login matching `copilot*[bot]` /
`github-copilot*[bot]`) AND that comment's `createdAt` is ≥ 7
days ago AND no author reply in the same thread or on the PR
after that timestamp.

### `static_check`

Failed check name (case-insensitive substring match either
direction) hits one of: `static check`, `pre-commit`, `prek`,
`lint`, `mypy`, `ruff`, `black`, `flake8`, `pylint`, `isort`,
`bandit`, `codespell`, `yamllint`, `shellcheck`.

### `recent_main_failures`

Cached set of failing check names from the most recent 10
merged PRs on `<base>`. Built by the recent-main-branch-failures
query in [`fetch-and-batch.md`](fetch-and-batch.md).
Cache TTL 4 hours. A check name appearing in ≥ 2 of the 10
sampled PRs is "systemic".

### `flagged_prs_by_author`

Count of PRs by the same author seen on the **current page** that
already matched a `deterministic_flag` row. Per-page only — does
not persist across sessions.

### `first_time_no_real_ci`

Author is `FIRST_TIME_CONTRIBUTOR` or `FIRST_TIMER` AND one of:

- `statusCheckRollup.state` is `EXPECTED` or empty (no contexts), OR
- `statusCheckRollup.state` is `SUCCESS` but no
  `statusCheckRollup.contexts` matches a real-CI pattern (see
  [Real-CI guard](#real-ci-guard)).

Catches the case where the per-page `action_required` REST index
is empty / stale or the run is not yet indexed, but the rollup
shape still indicates real CI has not executed.

### `follow_up_ping`

True when at least one of the following resolves the apparent
`stale_review` (Row 18):

- A comment by the PR author after the most recent
  `CHANGES_REQUESTED` review (`comments(last:10)`) whose body
  `@`-mentions the reviewer login.
- A comment by the reviewer after the author's most recent
  commit (`commits(last:1).committedDate`).

Either signal indicates the conversation is alive and a fresh
ping would talk over an existing nudge. False otherwise.

---

## Real-CI guard

**Mandatory before any row that classifies a PR as `passing`
(rows 19, 20).** Also fires before row 16's "no real CI ran"
detection.

A PR can have `statusCheckRollup.state == SUCCESS` while every
real CI run is held in `action_required`. The rollup aggregates
only completed check-runs; fast bot checks (`Mergeable`, `WIP`,
`DCO`, `boring-cyborg`) succeed unconditionally and pull the
rollup to SUCCESS before real CI is allowed to start.

Walk `statusCheckRollup.contexts.nodes` and confirm at least one
context's name matches a real-CI pattern. If none match,
reclassify:

- If the per-page `action_required` REST index has any runs at
  the PR's head SHA, route to row 1 (`pending_workflow_approval`).
  Catches the case where the GraphQL rollup has not yet
  reflected a workflow run that was approved between fetch
  time and the guard run.
- Otherwise, route to row 16 (`rebase`).

Note: the `FIRST_TIME_CONTRIBUTOR` / `FIRST_TIMER` case is
already handled by row 1's `first_time_no_real_ci` precondition,
so it never reaches the guard. The row 1 / guard split is
belt-and-braces — any first-time PR with no real CI is caught
upstream.

Real-CI patterns on `apache/airflow`:

- `Tests` (exact or as prefix)
- `Tests \(.*\)` (matrix splits)
- `Static checks` / `Pre-commit`
- `Ruff` / `mypy-*`
- `Build (CI|PROD) image`
- `Helm tests`
- `K8s tests`
- `Docs build`
- `CodeQL`
- `Check newsfragment PR number`

Bot/labeler noise (`Mergeable`, `WIP`, `DCO`, `boring-cyborg`,
`probot`, etc.) does NOT count.

---

## Grace periods

Apply only to the CI-failure leg of `has_deterministic_signal`.
Conflicts and unresolved threads do not have a CI-style grace
window — they are gated by pre-filter F5a (the 72-hour
author-response cooldown) instead.

| Condition on the PR | Grace window |
|---|---|
| No collaborator engagement (no review, no comment from a `COLLABORATOR`/`MEMBER`/`OWNER`) | **24 hours** |
| At least one collaborator has commented or reviewed | **96 hours** |

Computed from the most recent failing check's `startedAt` (fall
back to `completedAt`, then to PR `updatedAt`). If still inside
the grace window, treat the CI-failure signal as not-fired for
purposes of the decision table — the PR may still classify on a
conflict or unresolved-thread signal, or fall through to row 20
if it has none.

Record the effective grace result on the PR record so reason
templates can include "CI failed Xh ago, Yh remaining" if
desired.

---

## Reason template rules

Reason strings are rendered verbatim to the maintainer in the
proposal and (for actions that post a comment) included in the
body. Rules:

- One line. Factual. Lead with the signal that fired the rule;
  end with the proposal verb where applicable.
- Substitute placeholders (`<base>`, `<reviewers>`, `N`, `K`,
  `M`) from the PR record. `<reviewers>` is `@login` mentions
  joined with a comma followed by a single space — see the canonical example
  below. Substitution happens at classification time;
  [`interaction-loop.md`](interaction-loop.md) displays the
  already-substituted string verbatim.
- Never editorialise. Never include emoji or scare quotes.
- Never include LLM-generated prose; the templates above are the
  full surface area.

Examples:

> Only static-check failures (ruff, mypy-airflow-core) — suggest comment
>
> 3/5 CI failures also appear in recent main-branch PRs — likely systemic, suggest rerun
>
> Merge conflicts with `main` + 73 commits behind — suggest draft
>
> 2 unresolved review threads from @potiuk, @uranusjr — suggest ping

Avoid:

> This PR has issues — suggest draft
>
> Not good enough — close it
>
> 🚨 Failing CI 🚨

---

## Required GraphQL fields

Adding a new row to the decision table? Cross-check this list
first; if a field isn't already populated, extend the batch query
in [`fetch-and-batch.md`](fetch-and-batch.md) before writing the
classification logic. Golden rule "one query per page" still
applies — rows do not get to reach back for more data.

| Decision rows / preconditions | Required fields |
|---|---|
| F5a, F5b, grace periods | `comments(last:10).nodes.{author.login,authorAssociation,bodyText,createdAt}`, `latestReviews.nodes.{author.login,submittedAt}`, `commits(last:1).nodes.commit.committedDate` |
| Row 1 + Real-CI guard | `statusCheckRollup.state`, `statusCheckRollup.contexts`, `authorAssociation`, `head_sha` (REST `action_required` index keyed by `head_sha`) |
| `copilot_review_stale` (row 2) | `reviewThreads.nodes.{isResolved,comments.nodes.{author.login,createdAt,url}}`, `comments(last:10).nodes.{author.login,createdAt}` |
| `has_deterministic_signal`, `ci_failures_only`, `unresolved_threads_only`, `unresolved_threads_only_likely_addressed` (rows 8–17) | `mergeable`, `statusCheckRollup.{state,contexts}`, `reviewThreads.nodes.{isResolved,comments(first:5).nodes.{author.login,authorAssociation,createdAt}}`, `updatedAt`, `comments(last:10).nodes.{author.login,authorAssociation,createdAt}`, `commits(last:1).nodes.commit.committedDate`, `author.login` |
| Row 18 (`stale_review`) | `latestReviews.nodes.{state,author.login,submittedAt}`, `commits(last:1).nodes.commit.committedDate`, `comments(last:10)` |
| Rows 3–5 (`already_triaged` / `stale_draft` from triage marker) | `comments(last:10).nodes.{author.login,bodyText,createdAt}`, viewer login, `commits(last:1).nodes.commit.committedDate` |
| Rows 19, 20 (`passing`) | `statusCheckRollup.state`, `statusCheckRollup.contexts`, `mergeable`, `reviewThreads.totalCount`, `labels` |

---

## Where the prose went

- Why each rule exists, the heuristic discussion behind
  `unresolved_threads_only_likely_addressed`, the draft-vs-comment-vs-ping
  reasoning, override semantics, and the
  refuse-to-suggest cases all moved to
  [`rationale.md`](rationale.md). Numbering matches the decision
  table rows so a maintainer can jump from a row to its
  rationale in one click.
- Pre-filter rationale (especially the "rush the contributor /
  talk over a maintainer" framing) is in
  [`rationale.md#pre-filter-5-active-maintainer-conversation`](rationale.md#pre-filter-5-active-maintainer-conversation).
