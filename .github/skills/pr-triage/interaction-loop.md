<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Interaction loop

This file documents how the skill **presents** proposals to the
maintainer. The classification (from [`classify.md`](classify.md))
and suggestion (from
[`suggested-actions.md`](suggested-actions.md)) steps are
deterministic; this step is where the maintainer's time is
actually spent. Every optimisation here translates directly
into maintainer velocity.

The core idea:

> Present **groups of PRs with the same suggested action**
> together. The maintainer bulk-confirms the group, pulls
> individual PRs out for closer inspection, or skips the group.

The underlying `breeze pr auto-triage` tool presented PRs one-
at-a-time (sequential mode) or as a TUI list with per-PR keys.
This skill lands between those: sequential per-group, with a
drill-in for the PRs the maintainer wants to eyeball.

---

## Group ordering

After classification and suggested-action computation, partition
all PRs into groups keyed by `(classification, action)`. Present
the groups in this fixed order:

1. `(pending_workflow_approval, approve-workflow)` — safety-
   relevant, one-at-a-time per
   [`workflow-approval.md`](workflow-approval.md), never
   batched.
2. `(deterministic_flag, close)` — destructive, one-at-a-time
   (but share the same group screen so the maintainer sees the
   "queue pressure" signal from multiple PRs by the same
   author at once).
3. `(stale_copilot_review, draft)` — batchable. Drafts PRs whose
   Copilot review has sat unaddressed for ≥ 7 days.
4. `(deterministic_flag, draft)` — batchable.
5. `(deterministic_flag, comment)` — batchable.
6. `(deterministic_flag, rebase)` — batchable.
7. `(deterministic_flag, rerun)` — batchable.
8. `(deterministic_flag, ping)` — batchable (unresolved threads
   from collaborators).
9. `(stale_review, ping)` — batchable.
10. `(passing, mark-ready)` — batchable.
11. `(stale_draft, close)` — batchable but with extra per-PR
    confirm inside the batch (these are rarely wrong but when
    wrong they're very wrong).
12. `(inactive_open, draft)` — batchable.
13. `(stale_workflow_approval, draft)` — batchable.

The ordering is chosen so the maintainer always faces the
riskiest decisions first, while their attention is fresh. The
last few groups (stale sweeps) are mostly auto-apply-all.

Never interleave groups. Finish one before starting the next.
If the maintainer quits mid-group, don't start later groups.

---

## Group presentation

For each group, present one screen of information. The goal is
a decision in under 15 seconds when the suggestion looks right,
or a natural path to per-PR inspection when it doesn't.

```
─────────────────────────────────────────────────────
Group 3 of 8  —  deterministic_flag → draft  —  5 PRs

Common reason: all have failing CI + unresolved review threads
past the grace window.

  #65401  Add new provider foo                @alice    CI✗ thrd:2  +3/-1  1d
  #65417  Fix parsing of baz                  @bob      CI✗ thrd:1  +12/-4 3h
  #65422  Change caching behavior             @carol    CI✗ thrd:3  +8/-2  2d
  #65460  Typo fix in helm chart              @dave     CI✗ thrd:1  +1/-1  6h
  #65471  Add support for new db dialect      @eve      CI✗ thrd:4  +230/-60 4d

Suggested action: convert all to draft with violations comment.

  [A]ll   — apply to all 5
  [E]ach  — walk one-by-one
  [P]NN   — pull NN out for inspection (e.g. P65471)
  [O]verride — use a different action for all 5 (comment / close / skip)
  [S]kip  — leave all 5 alone this sweep
  [Q]uit  — exit session
```

### Columns explained

| Column | Content |
|---|---|
| PR # | number, clickable link to the PR |
| Title | truncated to fit, full title on per-PR expand |
| Author | login, clickable link to GitHub profile |
| CI | `CI✓` passed, `CI✗` failed, `CI?` unknown / empty |
| thrd | unresolved-thread count |
| +/- | additions / deletions from the PR record |
| Age | human-readable "time since last update" |

Optional columns when relevant: `beh:NNN` for commits-behind,
`draft` marker, `flagged:N` for the author's overall flagged-
PR count (shown only when > 3, driving the `close` suggestion).

Keep the row to **one line** per PR. Anything longer makes the
group screen itself a decision bottleneck.

---

## Decision keys

| Key | Action |
|---|---|
| `[A]` | Apply the suggested action to every PR in the group. |
| `[E]` | Walk through the group one PR at a time, per-PR confirm each. |
| `[P]NN` | Pull PR `NN` out of the group into an individual drill-in; the rest of the group remains pending. |
| `[O]` | Override the action for the whole group to a different verb (offered list is the safe-overrides set for this group — see [`#group-action-override`](#group-action-override)). |
| `[S]` | Skip the group — no mutations, all members marked "skipped" in the session. |
| `[Q]` | Quit the session. Emit summary. |

After `[A]` the action is executed for every PR in the group
(see batching rules in [`actions.md#batching-execution`](actions.md)).
After `[E]`, the group becomes a queue; each PR gets its own
individual prompt. After `[P]NN`, PR `NN` gets the individual
flow and the rest of the group remains on screen for a follow-
up `[A]`/`[E]`/`[S]`/`[Q]` decision.

The three destructive groups —
`(deterministic_flag, close)`, `(stale_draft, close)`,
`(pending_workflow_approval, *)` — require a per-PR confirm
inside `[A]`/`[E]` alike. `[A]` on those means "don't drop me
back to the group menu between PRs", not "apply without
confirm".

---

## Individual (drill-in) presentation

When a PR is pulled out of a group (via `[P]`, `[E]`, or
because its group mandates per-PR), present the full detail:

```
─────────────────────────────────────────────────────
PR #65471   "Add support for new db dialect"
Author: @eve  (tier: new, 2 merged / 5 total on this repo)
Age: opened 4d ago, last push 6h ago
Branch: eve-fork:feature/dialect → apache:main (230 / -60, 12 behind)
Labels: area:providers, provider:postgres

CI: FAILURE (4 failed checks)
  - Tests (postgres)                   ← known recent main-branch flake
  - Tests (sqlite)
  - Static checks
  - mypy-providers                     ← only-static-check pattern broken

Unresolved review threads: 4
  - @potiuk (MEMBER): "Why does this touch airflow-core/..."
  - @uranusjr (MEMBER): "Consider using the existing hook abstraction"
  - @eladkal (MEMBER): "Should we add a newsfragment?"
  - @potiuk (MEMBER): "Typo on line 74"

Suggested: draft — "Has quality issues across all three signals"

[Draft comment body preview — click to expand]

Decide:
  [D]raft  [C]omment  [Z]lose  [R]ebase  [F]rerun  [M]ark ready
  [B]ack to group  [S]kip  [O]pen in browser  [W]show full diff
```

The action keys on the per-PR screen are the **full** verb
menu, not restricted to the group's suggested action. The
maintainer can override per-PR to any valid action.

`[W]` fetches and displays the full diff (via
`gh pr diff <N>` — cache in session cache keyed by head SHA).
This is the only moment a diff is read for a non-workflow-
approval PR, and it's gated on the maintainer asking for it.

`[B]` returns to the group screen with PR `NN` marked as
"pulled-out-and-left-pending". The maintainer can come back
to it after finishing the rest of the group.

---

## Group action override

`[O]` on a group prompts the maintainer with a short list of
safe alternatives:

| Group's suggested action | Safe overrides |
|---|---|
| `draft` | `comment`, `rebase`, `skip` |
| `comment` | `draft`, `rebase`, `skip` |
| `rebase` | `comment`, `skip` |
| `rerun` | `comment`, `skip` |
| `mark-ready` | `skip` |
| `ping` | `comment`, `skip` |
| `close` (deterministic_flag) | — (no overrides — use `[E]` to downgrade individually) |
| `close` (stale_draft) | `draft`, `skip` |
| `draft` (inactive_open / stale_workflow_approval) | `comment`, `skip` |

`close` from `deterministic_flag` has no override because its
trigger condition (author has >3 flagged PRs) means the
individual violation list varies per PR; a group-level
`comment` override would post wildly different comments with
the same confirmation. Forcing `[E]` keeps the comment
previews per-PR.

---

## Optimistic lock (re-check before mutate)

Between the fetch (Step 1 / 2) and the mutation (Step 5) the
contributor may have pushed a new commit. Before executing any
action for a given PR, re-check the PR's `head_sha` against
the one captured at fetch time:

```bash
gh api graphql -F owner=<owner> -F repo=<repo> -F number=<N> -f query='
  query($owner: String!, $repo: String!, $number: Int!) {
    repository(owner: $owner, name: $repo) {
      pullRequest(number: $number) {
        headRefOid
        mergeable
        statusCheckRollup: commits(last: 1) {
          nodes { commit { oid statusCheckRollup { state } } }
        }
      }
    }
  }'
```

If `headRefOid` matches, proceed. If it differs:

- Tell the maintainer: *"Contributor pushed a new commit since
  we classified this PR. Re-classifying…"*.
- Re-fetch the full PR record and re-classify.
- If the new classification yields the **same** suggested
  action, carry on.
- If it differs, drop back to the per-PR drill-in with the new
  state and let the maintainer re-decide.

This guard catches the common race and prevents the worst
failure mode ("convert-to-draft-on-a-commit-that-wasn't-broken").
Burn the one extra GraphQL point per action — a bad mutation
costs more.

Batch the re-check queries for `[A]` actions — one aliased
`pullRequest(number: N)` per PR in the group, one round-trip.

---

## Prefetch plan

Whenever a group is presented to the maintainer (an
information-only turn), fire **in the same turn** any follow-up
fetches the next decision will need. Parallel tool calls make
this free — the network round-trip overlaps with the
maintainer's reading time.

Concrete prefetches:

| Currently showing | Prefetch |
|---|---|
| Any group | Next page's PR-list + rollup query (if `has_next_page` and `page_num < max_num / 50`) |
| `pending_workflow_approval` group | `gh pr diff <N>` for the first 2 PRs in the group |
| `deterministic_flag → draft/comment` group, one PR at a time | Failed-job log snippets for the current PR and the next PR in the queue |
| `close` group (per-PR) | Author's full open-PR list (for the "you have N flagged PRs" line in the body) |
| Any per-PR drill-in | Author profile (account age, repo merge rate) if not already cached |

Do **not** prefetch:

- Data for groups the maintainer may not reach this session
  (page 3 when they're on page 1).
- Full diffs for non-workflow-approval PRs unless the
  maintainer actually presses `[W]`.
- Author profiles for PRs in stale-sweep groups — they're being
  closed or drafted with minimal per-PR custom data, so the
  profile costs more than it saves.

When a prefetched result lands before the maintainer acts, store
it in the session cache; when the maintainer eventually triggers
the drill-in, it's instant.

---

## Batch execution status

When `[A]` triggers batched mutations, show live progress as a
short table that updates in-place:

```
Applying action: draft  (5 PRs, parallelism: 5)

  #65401 @alice — posting comment… done
  #65417 @bob   — converting to draft… done
  #65422 @carol — posting comment… failed (PR already closed)
  #65460 @dave  — converting to draft… done
  #65471 @eve   — converting to draft… done  (head SHA changed, re-classified — same action, proceeding)

4 succeeded, 1 skipped. Continue to next group? [Y/q]
```

Failures in a batch don't cascade-abort. The per-PR error is
logged, the batch continues, and the final tally is surfaced
before moving on.

---

## Session summary

On exit (either `[Q]` or after the last group), print a
session summary:

```
Session summary — 2026-04-22 09:42 UTC → 10:07 UTC (25m)

PRs presented:  47
PRs acted on:    22
  - drafted:           5
  - commented:         3
  - closed:            2
  - rebased:           4
  - reruns triggered:  3
  - marked ready:      3
  - pings posted:      2
PRs skipped:     15   (12 already triaged / inside grace, 2 bot, 1 collaborator)
PRs left pending: 10   (reached [Q] before classifying)

Throughput: 22 actions / 25m = 53 PRs/h
```

Write a copy to the session cache under a `last_summary`
key — re-invocations of the skill can reference it with *"last
triage run closed 2h ago, these 12 PRs were skipped then"*.
Don't persist across sessions on disk beyond the cache.

---

## Failure mode: the maintainer disagrees with every suggestion

If the first two groups the maintainer touches are
entirely `[O]`-overridden or `[S]`-skipped, the suggestions
logic is miscalibrated for this session (or a systemic CI
issue has landed). Surface a one-line note:

> Heads-up: the first two groups were overridden. If main-
> branch CI is broken this session, the `rerun` and `rebase`
> suggestions will be noisy. Would you like to skip to the
> stale sweeps? [Y/n]

This is a cheap safety valve against the skill burning through
a frustrated maintainer's morning on stale suggestions. It only
fires once per session and only if the override rate is
high — don't be annoying.
