<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Suggested actions

[`classify.md`](classify.md) answers *what's wrong with this PR*.
This file answers *what should we do about it*.

The output of this step is, per classified PR, a pair
`(action, reason)` where `action` is one of the verbs in
[`actions.md`](actions.md) and `reason` is a one-line string
rendered to the maintainer in the proposal.

Suggestions are evaluated **top-to-bottom**; the first matching
rule wins.

---

## Per-classification default tables

### `pending_workflow_approval`

| Rule | Action | Reason |
|---|---|---|
| *(always)* | `approve-workflow` | "First-time contributor — review the diff and approve CI, or flag suspicious" |

The maintainer chooses between `approve-workflow` and
`flag-suspicious` after inspecting the diff. See
[`workflow-approval.md`](workflow-approval.md) for the
inspection rubric.

### `stale_copilot_review`

| Rule | Action | Reason |
|---|---|---|
| *(always)* | `draft` | "Unaddressed Copilot review ≥ 7 days old — convert to draft so the queue doesn't block on stale automated feedback" |

Body: the `draft` template from
[`comment-templates.md#draft-comment`](comment-templates.md)
with the **"Unaddressed Copilot review"** violation listed.
Reference the specific thread URL in `details` so the author
can jump straight to it.

### `deterministic_flag`

Evaluated top-to-bottom. `failed_count` is
`len(failed_checks)`; `flagged_prs_by_author` is the number of
currently-flagged PRs by the same author seen **this page**.

| Rule | Action | Reason |
|---|---|---|
| `flagged_prs_by_author > 3` | `close` | "Author has N flagged PRs — suggest closing to reduce queue pressure" |
| `mergeable == CONFLICTING` *(any other signal, or none)* | `draft` | "Merge conflicts with `<base>` — GitHub's side-merge can't resolve them, author must rebase locally; convert to draft with merge-conflicts violation" |
| Only CI failures, all failures also appear in recent main-branch failures | `rerun` | "All N CI failures also appear in recent main-branch PRs — likely systemic, suggest rerun" |
| Only CI failures, *some* failures match main-branch failures | `rerun` | "K/N CI failures match recent main-branch PRs — likely systemic" |
| Only CI failures, every failed check is a static check (`ruff`, `mypy-*`, `pre-commit`, etc.) | `comment` | "Only static-check failures — deterministic, needs a code fix, not a rerun" |
| Only CI failures, `failed_count <= 2`, no conflicts, no unresolved threads, `commits_behind <= 50` | `rerun` | "N CI failure(s) on otherwise clean PR — likely flaky, suggest rerun" |
| Unresolved review threads only, CI green, no conflict | `ping` | "K unresolved review thread(s) from <reviewers> — ping author + reviewers with the [`reviewer-ping`](comment-templates.md#reviewer-ping) template" |
| No CI at all (rollup empty / only bot contexts), `mergeable != CONFLICTING` | `rebase` | "No real CI checks triggered, branch mergeable — rebase to re-trigger" |
| *(fallback)* | `draft` | "Has quality issues — convert to draft with comment listing violations" |

**Hard rule: never suggest `rebase` when `mergeable == CONFLICTING`.**
GitHub's "Update branch" endpoint does a side-merge of the base
branch into the head and refuses when the merge doesn't apply
cleanly — exactly the case conflicts create. Empirically on
`apache/airflow`, every rebase attempt on a CONFLICTING PR has
returned "Cannot update PR branch due to conflicts" and wasted
a round-trip. For those PRs, go straight to `draft` with the
`Merge conflicts` violation so the author is pointed at the
local-rebase instructions in their comment body. See
[`actions.md#rebase`](actions.md) for the matching action-side
guard.

Notes:

- "Static check" is matched by name against this substring set:
  `static check`, `pre-commit`, `prek`, `lint`, `mypy`, `ruff`,
  `black`, `flake8`, `pylint`, `isort`, `bandit`, `codespell`,
  `yamllint`, `shellcheck`.
- "Main-branch failures" are the cached failing-check-name set
  from [`fetch-and-batch.md#recent-main-branch-failures`](fetch-and-batch.md).
  Match is case-insensitive substring in either direction.

### `stale_review`

| Rule | Action | Reason |
|---|---|---|
| *(always)* | `ping` | "Author pushed commits after <reviewer>'s CHANGES_REQUESTED review but no follow-up — ping author to resolve (reviewer only if feedback looks addressed)" |

**Default the body to pinging the author**, asking them to
address the outstanding review comments. Only flip to the
reviewer-re-review body variant after the decision rule in
[`comment-templates.md#review-nudge`](comment-templates.md)
confirms the feedback has been addressed in a post-review
commit or resolved with an in-thread reply. See the inspection
steps in that template; a bare "nudge reviewer" default is the
wrong call when the author hasn't actually done the work yet.

If multiple stale reviewers, list them all in the reason and
`@`-mention them all in the generated ping comment.

### `already_triaged`

| Rule | Action | Reason |
|---|---|---|
| Sub-state `waiting`, comment age < 7 days | `skip` | "Already triaged M days ago — still waiting on author" |
| Sub-state `waiting`, comment age ≥ 7 days, PR is draft | (reclassify as `stale_draft`) | — |
| Sub-state `responded` | `skip` | "Already triaged M days ago — author responded, maintainer to re-engage" |

The `responded` sub-state is left to the maintainer's manual
inspection because the author's response might be a question
that needs answering (not a fix push). Do not auto-suggest an
action here.

### `passing`

| Rule | Action | Reason |
|---|---|---|
| PR has `ready for maintainer review` label already | `skip` | "Already marked ready for review" |
| *(fallback)* | `mark-ready` | "All checks green, no conflicts, no unresolved threads — mark for deeper review" |

### Stale sweep classifications

See [`stale-sweeps.md`](stale-sweeps.md) — their suggested
actions are fixed by classification (not state-dependent).

| Classification | Action | Reason |
|---|---|---|
| `stale_draft` (triaged >7d, no reply) | `close` | "Draft triaged N days ago, no author reply — close with stale-draft notice" |
| `stale_draft` (untriaged, >2w old, no activity) | `close` | "Draft inactive for W weeks — close with stale-draft notice" |
| `inactive_open` (>4w, no activity) | `draft` | "Open non-draft inactive for W weeks — convert to draft with resume-when-ready comment" |
| `stale_workflow_approval` (>4w, no approval) | `draft` | "Awaiting workflow approval for W weeks, no activity — convert to draft" |

---

## Reason-string construction rules

The reason is rendered verbatim to the maintainer in the
proposal and — when relevant — included in the posted comment
body. Keep it **one line**, factual, and low on adjectives.

- Lead with *the signal*: what specifically fired the rule (the
  failing-check category, the reviewer's login, the age, the
  flagged-PR count).
- End with *the proposal*: "suggest rerun", "suggest draft",
  "suggest close".
- Never editorialise ("this contributor is clearly…"). The
  reason goes in front of a human who is already frustrated;
  don't add to the frustration.
- Never include LLM output, scare quotes, or emoji.

Examples of good reason strings:

> Only static-check failures (ruff, mypy-airflow-core) — suggest comment
>
> 3/5 CI failures also appear in recent main-branch PRs — likely systemic, suggest rerun
>
> Merge conflicts with `main` + 73 commits behind — suggest rebase
>
> 2 unresolved review threads from @potiuk, @uranusjr — suggest comment

Examples to avoid:

> This PR has issues — suggest draft
>
> Not good enough — close it
>
> 🚨 Failing CI 🚨

---

## Choosing between `draft` and `comment` for the same signals

Both actions post the same violation list. The difference:

- `draft` also flips the PR to draft state — signals "stop
  requesting review, fix these first, mark ready yourself".
- `comment` leaves the PR in its current state — signals "here
  are the issues, continue working, the PR is still up for
  review once addressed".

Heuristics embedded in the rules above:

- Draft is the default for PRs that are "broken enough that
  maintainer review time would be wasted". CI failures +
  unresolved threads + conflicts all together → draft.
- `ping` is preferred when the contributor is clearly still
  iterating (only unresolved threads, no CI red, no conflict) —
  dropping them back to draft would be discourteous, and the
  short reviewer-ping template is proportionate to the actual
  issue (reviewer needs to re-check / author needs to resolve).
  A full violations-list comment would be overkill.
- `comment` is preferred when the issue is narrow and
  deterministic (static-check-only failures) — they fix it and
  push, no extra round-trip needed.

Collaborator-authored PRs (when `authors:collaborators` is
active) always default to `comment` — never `draft` — since
collaborators don't need gentle routing and converting a
colleague's PR to draft is an overreach.

---

## Overriding at group level

The interaction loop lets the maintainer override the suggested
action for an entire group (e.g. "these 5 PRs suggested `draft`
but I want to `comment` them instead — the author is actively
fixing"). See
[`interaction-loop.md#group-action-override`](interaction-loop.md)
for the mechanics — classification stays the same, only the
action switches.

Draft overrides are in scope; **class overrides are not**.
The maintainer cannot tell the skill "pretend this PR is
`passing`" — they would use `mark-ready` directly on the PR
instead, which is a per-PR decision.

---

## When to refuse to suggest

Some situations should produce *no* suggested action:

- The PR's data looks inconsistent (e.g. rollup says `SUCCESS`
  but `failed_checks` is non-empty) — surface the inconsistency
  to the maintainer with a one-line note and default to `skip`.
  Do not guess; data anomalies usually mean GitHub hasn't fully
  settled the rollup yet and a refresh on the next page will
  clear it.
- The PR was created less than 30 minutes ago (`createdAt`
  within the last 30 min) — suggest `skip` with reason "too
  fresh; CI still warming up". This protects new contributors
  from being flagged on checks that simply haven't finished.
- The viewer themselves is the PR author — suggest `skip`
  regardless of state. Triaging your own PR from this skill is
  unintended; mutation APIs will work but the signal is meant
  for outside contributors.
