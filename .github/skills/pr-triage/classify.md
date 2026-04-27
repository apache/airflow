<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Classify

This file is the decision matrix that turns the data in-hand
(from [`fetch-and-batch.md`](fetch-and-batch.md)) into one of
the seven classifications the interaction loop groups by.

Classification is **pure function of state** — no network calls,
no prompts, no writes. Every field referenced below is populated
by the single batched GraphQL query; if something looks missing,
it means the query is wrong, not that you should go fetch more.

---

## Pre-classification filters

Run these **before** classifying — a PR that doesn't survive
this filter is not presented to the maintainer at all.

### 1. Author is a collaborator / member / owner

`authorAssociation ∈ {OWNER, MEMBER, COLLABORATOR}`. The triage
skill's job is to clear the non-collaborator queue; a
collaborator's PR is their own to manage. Skip silently.

*(Override: the maintainer may pass `authors:all` or
`authors:collaborators` to opt in. The default is contributors
only.)*

### 2. Author is a known bot

Login matches one of:

- `dependabot`, `dependabot[bot]`
- `renovate[bot]`
- `github-actions`, `github-actions[bot]`
- Any login ending in `[bot]`

Bots have their own lifecycle management. Skip silently.

### 3. Draft *and* not stale

A draft with any activity in the last 2 weeks stays in its
author's court. Include it *only* if stale-sweep classification
matches (see [`stale-sweeps.md`](stale-sweeps.md)). Skip silently
from the main triage flow otherwise.

### 4. Has `ready for maintainer review` label *and* no triage signal

PRs already marked for deeper review are off the triage skill's
menu unless they've regressed (CI failure, new conflict, stale
review). If the label is present *and* CI is green *and* no
conflict *and* no unresolved threads, skip — the review skill
owns them now.

If any triage-relevant signal has appeared *after* the label
was applied (CI turned red, a new conflict, a new unresolved
thread), **do** include the PR — mark it as a regressed
passing PR in the interaction loop so the maintainer can decide
whether to pull the label.

### 5. Active maintainer conversation on the PR

Two sub-cases — if either matches, **skip silently**, regardless
of conflicts, failing CI, or unresolved threads. These two cases
override the deterministic-flag classification because acting on
them would either rush the contributor or talk over a
maintainer-to-maintainer conversation.

#### 5a. Recent collaborator comment (author-response cooldown)

The most recent comment on the PR is by a `COLLABORATOR`,
`MEMBER`, or `OWNER` *and* its `createdAt` is **< 72 hours**
ago *and* it was posted **after** the PR's last commit's
`committedDate`.

**Rationale.** A maintainer just engaged with the PR; the author
deserves at least three days to read, think, and reply before
the triage skill auto-drafts or auto-comments on top of that
conversation. The 72-hour window is intentionally longer than
the 24-hour CI grace below — review-style feedback takes longer
to address than a flaky-CI nudge, and a same-day auto-action
reads as the bot talking over the maintainer.

#### 5b. Latest collaborator comment is a maintainer-to-maintainer ping

The most recent comment on the PR is by a `COLLABORATOR`,
`MEMBER`, or `OWNER` *and* its body `@`-mentions one or more
other GitHub logins **other than the PR author** *and* none of
those mentioned logins have posted a comment or review on the
PR after that ping.

Detect via:

- the comment author's `authorAssociation` ∈
  {`COLLABORATOR`, `MEMBER`, `OWNER`}
- regex on the comment `bodyText` for `@<login>` mentions, then
  remove the PR author's login from the resulting set
- check `comments(last:10).nodes` and `latestReviews.nodes`
  for any subsequent post by any of the remaining logins

**Rationale.** When a maintainer pings other maintainers (e.g.
*"@ash @kaxil could you weigh in on the API shape?"*), the PR
is waiting on **maintainer input**, not on author work.
Auto-drafting it with a "the author should work on comments"
message is wrong on two counts: (a) the contributor isn't the
bottleneck — the maintainer review/conversation is; (b) it
de-focuses the thread away from the maintainer-to-maintainer
discussion the original commenter was trying to start. Silently
skip until one of the pinged collaborators responds, at which
point 5a's 72-hour window starts ticking from *that* reply.

**Out of scope for 5b.** A `@`-mention of a *team* (e.g.
`@apache/airflow-committers`) is conservatively treated as a
maintainer-to-maintainer ping (skip), because we can't cheaply
expand team membership in the batch query and the false-positive
cost (skipping a PR that should have been actioned) is much
lower than the false-negative cost (talking over a real
maintainer call-out).

---

## Classifications

Each PR that survives the pre-filter gets exactly one of the
following classifications. The rules are evaluated **top-to-
bottom** — the first matching rule wins.

### C1. `pending_workflow_approval`

**Condition.** The PR has at least one GitHub Actions workflow
run in the `action_required` state — i.e. waiting for maintainer
approval before CI can execute.

**Detection — the rollup alone is NOT sufficient.** Empirically
on `apache/airflow` (2026-04), a PR can have
`statusCheckRollup.state == "SUCCESS"` while every real CI
workflow (`Tests`, `CodeQL`, newsfragment check, …) is stuck in
`action_required`. The rollup reflects only check-runs that have
actually completed, so early bot-emitted successes (`Mergeable`,
`WIP`, boring-cyborg, DCO) can pull the overall state to SUCCESS
while the real CI hasn't even been allowed to start. Trusting
the rollup alone classifies those PRs as `passing` and leads to
premature `mark-ready`.

The authoritative signal is the REST endpoint:

```
GET /repos/<owner>/<repo>/actions/runs?event=pull_request&status=action_required&per_page=100
```

This is a **repo-level** call that lists every run awaiting
approval across all PRs. One call per sweep (not per PR) covers
a whole page — index the response by `head_sha` and any PR whose
head SHA appears in that index is `pending_workflow_approval`,
regardless of what its rollup says.

Do this call **once per page** and cache the `head_sha → [run_id, …]`
mapping on the session for the duration of the page. Drop the
cache before fetching the next page (workflow-approval state
changes fast and a stale cache can miss a fresh run).

**Fallback signals** (useful when the REST call is unavailable
or returns empty for a SHA that nonetheless looks suspicious):

- rollup state is `EXPECTED` or the rollup is empty, *and* the
  author's `authorAssociation` is `FIRST_TIME_CONTRIBUTOR` /
  `FIRST_TIMER`
- rollup state is `SUCCESS` but every context is a bot/labeler
  (see [`#verifying-real-ci-ran`](#verifying-real-ci-ran)) — the
  "real CI ran" guard described below is the same guard and
  must run as part of classifying `passing`

**Rationale.** These PRs are the skill's single most sensitive
category: approving a workflow lets a first-time contributor's
code run inside Airflow's CI with its secret material. See
[`workflow-approval.md`](workflow-approval.md) for the diff-
review and flag-suspicious protocol.

### C2. `deterministic_flag`

**Condition.** At least one of:

- `mergeable == "CONFLICTING"`
- `statusCheckRollup.state == "FAILURE"` *and* the PR has been
  in this state **past its grace window** (see
  [`#grace-periods`](#grace-periods))
- `reviewThreads.totalCount` has at least one thread where
  `isResolved == false` *and* the thread's reviewer is a
  collaborator/member/owner

**Rationale.** Any of these is a solid, not-fuzzy reason the PR
can't be reviewed yet. The exact *action* to propose depends on
*which* of the three signals fired (conflicts → rebase, only
static-check failures → comment, etc.) — see
[`suggested-actions.md`](suggested-actions.md).

#### Sub-flag: `unresolved_threads_only_likely_addressed`

A boolean computed alongside the C2 classification. It is
`true` when **all** of the following hold:

- The only `deterministic_flag` signal that fired for this PR is
  unresolved review threads (no `CONFLICTING` mergeable state,
  no out-of-grace CI failure).
- Every unresolved thread has post-first-comment activity by
  the PR author — **either** an author reply later in the same
  thread (`reviewThreads.nodes.comments.nodes` has a node where
  `author.login == pr.author.login` and `createdAt >` the first
  comment's `createdAt`), **or** a commit was pushed after the
  thread's first-comment `createdAt`
  (`commits(last:1).committedDate > comment.createdAt`).
- The PR's latest commit `committedDate` is **after** the most
  recent unresolved-thread first-comment `createdAt` — i.e. the
  most recent author push is plausibly the resolution, not
  predating the reviewer's feedback.

The flag is consumed only by `suggested-actions.md` to choose
between the existing `ping` action and the new
[`mark-ready-with-ping`](actions.md#mark-ready-with-ping)
action — it does **not** create a separate classification.

**Heuristic, not authoritative.** The maintainer still confirms
the proposed action and the comment we post invites the
reviewer to push back if the threads aren't actually resolved
(see [`comment-templates.md#mark-ready-with-ping`](comment-templates.md)).
Conservative on purpose: a false-negative degrades to the
existing `ping` behaviour; a false-positive would advance a PR
that isn't actually ready, which is the worse failure mode.

### C2b. `stale_copilot_review`

**Condition.** The PR has at least one unresolved review thread
whose first comment's author matches a GitHub Copilot review-bot
login **and** the comment's `createdAt` is **≥ 7 days** ago
**and** no author comment in the same thread (or on the PR
itself) after that timestamp.

Copilot-bot login patterns to match (case-insensitive):

- `copilot-pull-request-reviewer[bot]` — GitHub's native PR-
  review Copilot (the canonical signal today)
- `copilot[bot]`
- `github-copilot[bot]`
- any login starting with `copilot` or `github-copilot` and
  ending in `[bot]`

Detect the authorship via `reviewThreads.nodes.comments.nodes
.author.login`. The thread's first comment is the one the
automation posted; a later reply (by anyone) doesn't reset the
clock unless it comes from the PR author.

**Rationale.** Copilot-review comments are work items queued
against the author — even if some of the Copilot suggestions
turn out to be wrong or irrelevant, the author is still the
one responsible for replying (accept, reject with a one-line
explanation, or fix). When Copilot comments sit unresolved for
a week the PR has effectively stalled — the author is either
unaware of the feedback or assuming someone else will triage
it. Converting to draft is the softer equivalent of the stale-
draft sweep: it stops the PR from blocking the maintainer
review queue while preserving the conversation for when the
author returns. The suggested action is therefore `draft` with
a dedicated "Unaddressed Copilot review" violation (see
[`suggested-actions.md`](suggested-actions.md) and
[`comment-templates.md#violations-rendering`](comment-templates.md)).

**Ordering.** Evaluate **before** `C2` (`deterministic_flag`)
so the specific Copilot signal wins over the generic
"unresolved review thread" fallback. A PR that also has a
collaborator-sourced unresolved thread still gets drafted with
the Copilot reason — the two violations can be listed together
in the comment body.

**Do not fire when** the Copilot review is still inside its
7-day window — the rule is explicitly *not* 24h/96h like the
CI grace period, because review feedback takes longer to
address and a same-week nudge would be noisy.

### C3. `stale_review`

**Condition.** The PR has at least one `latestReviews` entry
with `state == "CHANGES_REQUESTED"` **and** the author has
pushed commits *after* that review (`committedDate >
review.submittedAt`) **and** no comment from the reviewer or
author pings between them.

Derive the author-pushed-after-review signal from
`commits(last: 1).nodes[0].commit.committedDate` vs the most
recent `CHANGES_REQUESTED` review's `submittedAt`.

Derive the ping signal from `comments(last: 10)`: a comment by
the author after the review that mentions the reviewer login,
or a comment by the reviewer after the new commits, resolves
the stale state.

**Rationale.** The author is ostensibly waiting on a
re-review but never nudged — the ping action (see
[`actions.md#ping`](actions.md)) posts the nudge for them with
the relevant reviewer(s) `@`-mentioned.

### C4. `already_triaged`

**Condition.** The PR has a comment by the viewer login
containing the triage-comment marker string (
`Pull Request quality criteria`, from
[`comment-templates.md`](comment-templates.md)) **and** the
comment's `createdAt` is **after** the PR's last commit's
`committedDate`.

Sub-states:

- **waiting**: no author comment after the triage comment
- **responded**: author has commented after the triage comment

**Rationale.** We already posted the triage comment and the
contributor hasn't addressed it (or has commented asking a
question). Skip from the main flow. If the comment is older
than 7 days and the PR is a draft, flip to
`stale_draft` (see [`stale-sweeps.md`](stale-sweeps.md)).

### C5. `passing`

**Condition.** None of the above fire, **and**:

- `statusCheckRollup.state == "SUCCESS"`
- `mergeable != "CONFLICTING"`
- no unresolved review threads
- `statusCheckRollup.contexts` contains at least one **real**
  CI check (not just bot/labeler contexts)

**Rationale.** This PR is ready for the next stage. The
suggested action is `mark-ready` — add the
`ready for maintainer review` label and leave it to the review
skill.

### C6. Stale sweep classifications

`stale_draft`, `inactive_open`, and `stale_workflow_approval`
are evaluated by [`stale-sweeps.md`](stale-sweeps.md). They
never fire for a PR that already matched C1–C5 in the current
session — the sweep classifications are reserved for PRs that
would otherwise have been skipped by the pre-classification
filter.

---

## Grace periods

Deterministic CI failures are not immediately actionable — the
contributor deserves a chance to notice and fix before the
triage skill flags them. The grace period is computed per-PR
from the most recent failing check's `startedAt` (or the
check-run's `completedAt`, falling back to the PR's
`updatedAt`).

| Condition on the PR | Grace window |
|---|---|
| No collaborator engagement (no review, no comment from a COLLABORATOR/MEMBER/OWNER on the PR) | **24 hours** |
| At least one collaborator has commented or reviewed | **96 hours (4 days)** |

Extended-engagement logic:

- Iterate `comments(last: 10)` plus `latestReviews.nodes` for
  entries whose `authorAssociation` is `COLLABORATOR`,
  `MEMBER`, or `OWNER`.
- If any is present, apply the 96-hour window.

If the failure is *still* fresh (within the grace window), the
PR is **not** classified as `deterministic_flag` on the CI-
failure signal alone. Conflicts and unresolved threads have no
*CI*-grace window — but the **author-response cooldown** in
pre-classification filter [`#5a`](#5-active-maintainer-conversation-on-the-pr)
still applies to them: a fresh maintainer comment (< 72h, after
the last push) silences the auto-action even when conflicts or
unresolved threads are present, on the principle that the author
deserves three days to read maintainer feedback before a bot
talks over it.

Record the "effective grace" result on the PR record so the
suggested-action reason string can reference it ("CI failed 8h
ago, 16h remaining before flagging"). This is a nice-to-have in
the maintainer-facing proposal but not required for correctness.

---

## Verifying "real CI ran"

**Mandatory guard before classifying a PR as `passing`.**

A PR can have `statusCheckRollup.state == SUCCESS` with only
bot/labeler contexts present (no real test checks). This is the
common case when a first-time-contributor PR on a repo with
"Require approval for workflows" enabled: fast bot checks
(`Mergeable`, `WIP`, `boring-cyborg`, `DCO`) complete with
success while the real CI runs (`Tests`, `CodeQL`, `newsfragment
check`, …) sit in `action_required`. GitHub's rollup aggregates
only check-runs that have completed, so the rollup reports
SUCCESS while the real CI has not executed.

**This guard is not optional.** Before a PR is classified as
`passing` (C5), the implementation MUST walk
`statusCheckRollup.contexts.nodes` and confirm at least one
context's name matches a real-CI pattern below. If no real
context is present, reclassify:

- If the author is `FIRST_TIME_CONTRIBUTOR` / `FIRST_TIMER`, or if
  the per-page `action_required` REST call lists any runs at the
  PR's head SHA, classify as `pending_workflow_approval` (C1).
- Otherwise, classify as `deterministic_flag` with a
  `"no CI checks yet — needs rebase to re-trigger"` reason.

Signal: `statusCheckRollup.contexts.nodes` lacks any
`CheckRun` whose `name` matches a real-looking pattern. Real-
CI patterns used on `apache/airflow`:

- `Tests` (exact or as prefix — main test workflow)
- `Tests \(.*\)` (unit/integration tests split by matrix)
- `Static checks` / `Pre-commit`
- `Ruff` / `mypy-*`
- `Build (CI|PROD) image`
- `Helm tests`
- `K8s tests`
- `Docs build`
- `CodeQL`
- `Check newsfragment PR number`

Anything else (`Mergeable`, `WIP`, `DCO`, `boring-cyborg`,
`probot`, etc.) is bot/labeler noise and doesn't count toward
the real-CI check. Maintain this list in
[`suggested-actions.md`](suggested-actions.md) alongside the
category-of-failure table so updates stay in sync.

---

## Data required from the batch query, per classification

This checklist exists to catch the case where a future edit to
the GraphQL query drops a field that classification silently
relies on:

| Classification | Required fields |
|---|---|
| Pre-filter 5 (active maintainer conversation) | `comments(last:10).nodes.{author.login,authorAssociation,bodyText,createdAt}`, `latestReviews.nodes.{author.login,submittedAt}`, `commits(last:1).nodes.commit.committedDate` |
| `pending_workflow_approval` | `statusCheckRollup.state`, `authorAssociation`, `head_sha` |
| `stale_copilot_review` | `reviewThreads.nodes.{isResolved,comments.nodes.{author.login,createdAt,url}}`, `comments(last:10).nodes.{author.login,createdAt}` (to detect author replies after the Copilot comment) |
| `deterministic_flag` | `mergeable`, `statusCheckRollup.{state,contexts}`, `reviewThreads.nodes.{isResolved,comments.nodes.{author.login,authorAssociation,createdAt}}`, `updatedAt`, `comments(last:10).nodes.{author.login,authorAssociation,createdAt}`, `commits(last:1).nodes.commit.committedDate`, `author.login` (for the `unresolved_threads_only_likely_addressed` sub-flag — needs author replies in-thread, hence `comments(first: 5)` per thread instead of `first: 1`, see [`fetch-and-batch.md`](fetch-and-batch.md)) |
| `stale_review` | `latestReviews.nodes.{state,author.login,submittedAt}`, `commits(last:1).nodes.commit.committedDate`, `comments(last:10)` |
| `already_triaged` | `comments(last:10).nodes.{author.login,bodyText,createdAt}`, viewer login, `commits(last:1).nodes.commit.committedDate` |
| `passing` | `statusCheckRollup.state`, `statusCheckRollup.contexts`, `mergeable`, `reviewThreads.totalCount` |

Adding a new classification? Add a row here and extend the
fetch query before writing any classification logic. The golden
rule is still "one query per page" — classifications don't get
to reach back for more data.
