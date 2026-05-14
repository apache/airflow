<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Stale sweeps

The stale-sweep phase runs after the interactive triage is
done (Step 6 in [`SKILL.md`](SKILL.md)). Its job is to clear
three categories of PRs that have gone silent:

1. **Stale drafts** — drafts that haven't moved in weeks; either
   triaged and ghosted, or never triaged and drifted.
2. **Inactive open PRs** — non-draft PRs that have sat open
   for over 4 weeks with no activity.
3. **Stale workflow-approval PRs** — first-time-contributor
   PRs awaiting workflow approval that have sat for over 4
   weeks without the author pushing new commits.

Each category has deterministic trigger criteria, a fixed
action, and a canned comment. They are surfaced through the
same group-presentation machinery as regular triage (see
[`interaction-loop.md`](interaction-loop.md)) — the maintainer
confirms per group before anything is mutated.

The sweep is opt-in for full runs (`triage` selector) and
mandatory for `stale` runs (which skip the interactive triage
entirely). Both paths go through the same rules below.

---

## Inputs

Each stale sweep needs two timestamps per PR:

- `updated_at` — the PR's `updatedAt` field (already in the
  batch query)
- `last_triage_comment_at` — the `createdAt` of the most
  recent comment by the viewer containing the
  `Pull Request quality criteria` marker, if any

Both come from the same aliased query that drives
classification — no extra fetches. If a PR hasn't been triaged
in the current session and also wasn't triaged in a prior one,
`last_triage_comment_at` is null and the sweep uses `updated_at`
alone.

`<now>` is the session start time (captured in UTC on entry).
Use a single reference moment for the whole sweep so edge
cases (a PR updated mid-session) don't shift.

---

## Sweep 1 — Stale drafts

Two sub-cases, both resulting in `close`:

### 1a. Triaged draft with no author reply ≥ 7 days

**Trigger.**

- `isDraft == true`
- `last_triage_comment_at` is not null
- `<now> - last_triage_comment_at >= 7 days`
- No comment by the author after `last_triage_comment_at`

**Action.** `close` — post the
[stale-draft-close](comment-templates.md#stale-draft-close) comment,
then close. No label (these are not quality-violation closes).

**Reason string.** *"Draft triaged N days ago, no author reply — close with stale-draft notice"*.

### 1b. Untriaged draft with no activity ≥ 2 weeks

**Trigger.**

- `isDraft == true`
- `last_triage_comment_at` is null
- `<now> - updated_at >= 14 days`

**Action.** `close` — post the "untriaged-draft" variant of
[stale-draft-close](comment-templates.md#stale-draft-close), then
close. No label.

**Reason string.** *"Draft inactive for W weeks — close with stale-draft notice"*.

### Group behaviour

Stale-draft closes are **batchable but with per-PR confirm
inside the batch** — the same rule as the
`deterministic_flag → close` group. `[A]ll` walks the list
without re-prompting at the group level, but each PR still
flashes its comment preview and waits for `Y` / `n` before
mutating. See [`interaction-loop.md#decision-keys`](interaction-loop.md).

---

## Sweep 2 — Inactive open PRs

**Trigger.**

- `isDraft == false`
- `<now> - updated_at >= 28 days`
- No other stale classification applies

**Action.** `draft` — convert to draft and post the
[inactive-to-draft](comment-templates.md#inactive-to-draft)
comment. No label.

**Reason string.** *"Open non-draft inactive for W weeks — convert to draft"*.

### Rationale

Closing an inactive *open* PR is more disruptive than closing a
draft (the author actively asked for review at some point).
Converting to draft is the softer equivalent — it stops
blocking the queue, preserves the discussion, and the author
can mark as ready again when they resume.

### Group behaviour

Batchable with simple `[A]ll` (no per-PR confirm inside the
batch). The action is recoverable — the author can revert with
one click — so the looser batching is appropriate.

---

## Sweep 3 — Stale workflow-approval PRs

**Trigger.**

- Classification was `pending_workflow_approval` at fetch time
- `<now> - updated_at >= 28 days`
- PR author has not pushed since (i.e. `head_sha` is the same
  as at last update)

**Action.** `draft` — convert to draft and post the
[stale-workflow-approval](comment-templates.md#stale-workflow-approval)
comment. No label.

**Reason string.** *"Awaiting workflow approval for W weeks, no activity — convert to draft"*.

### Rationale

A first-time-contributor PR that sits waiting for approval for
a month usually means the contributor abandoned the attempt.
Closing feels harsh given they never even got CI feedback;
drafting clears the queue and leaves them the option to
resume.

### Group behaviour

Same as Sweep 2 — simple `[A]ll`.

---

## Order of sweeps

1. Sweep 1a (triaged drafts, 7d)
2. Sweep 1b (untriaged drafts, 2w)
3. Sweep 2 (inactive open, 4w)
4. Sweep 3 (stale WF approval, 4w)

Run 1a before 1b so a draft that's both "triaged 7d ago" and
"never-triaged 2w ago" (the triage comment is recent but the
overall PR is old) is categorised by the more precise trigger.
In practice that overlap is rare, but the order is defined.

---

## What the sweeps do NOT do

- **No force-close for "PR has merge conflicts for N weeks".**
  Merge-conflict staleness is still the author's to fix; we
  close via draft-then-stale-draft-close, not directly.
- **No automatic reopen.** If a sweep closes a PR by mistake,
  the maintainer reopens it manually — the skill never
  reverses its own mutation without a fresh confirmation.
- **No cross-author batching.** The `flag-suspicious` action
  from [`workflow-approval.md`](workflow-approval.md) *does*
  close multiple PRs per author, but it's a separate flow with
  its own safety protocol; the stale sweeps never extend their
  scope beyond "this one PR meets the criteria, close this one
  PR".
- **No sweeping across repos.** Each sweep runs against a
  single `<repo>`. Running the skill against a different repo
  is a separate session with its own cache.

---

## Budget

The sweep adds no new GraphQL calls beyond what classification
already fetched. The timestamps (`updated_at`,
`last_triage_comment_at`) come from the per-page batch query.
The only extra cost is mutations for each confirmed action —
which is the whole point of the sweep.

A typical morning `apache/airflow` sweep surfaces:

- 1–3 triaged drafts hitting the 7-day mark
- 2–5 untriaged drafts hitting the 2-week mark
- 1–3 inactive open PRs
- 0–2 stale workflow-approval PRs

…which is 4–13 mutations total, well under the rate-limit
budget. If a sweep turns up more than 50 candidates, something
is off (a previous sweep was never run; a release freeze
piled up activity) — surface the count and ask the maintainer
whether to continue, don't blast through silently.

---

## Dry-run

With `dry-run` on, every sweep displays its candidate group but
refuses to execute `[A]` or per-PR confirm — the maintainer
sees exactly what *would* happen without mutating anything.
Useful for calibrating the thresholds (if a sweep surfaces a
PR you think shouldn't be stale, you need to change the
timestamps-for-activity calculation, not the thresholds).

The session summary still reports the counts, tagged
`(dry-run — not mutated)`.
