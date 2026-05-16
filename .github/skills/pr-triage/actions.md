<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Actions

Exact recipes for every mutation the skill can execute. Every
action in this file assumes:

- the maintainer has confirmed it,
- the PR's `head_sha` has been re-checked against the value
  captured in Step 1 and matches (optimistic lock — see
  [`interaction-loop.md#optimistic-lock`](interaction-loop.md)),
- the action's comment (if any) has been previewed to the
  maintainer from the appropriate template in
  [`comment-templates.md`](comment-templates.md).

All mutations go through **`gh`**, never through raw `curl` /
`requests`. `gh` carries the maintainer's authenticated token
and retries transient failures correctly.

---

## `draft` — convert to draft and post violations comment

Two mutations, **sequence matters** — convert first, then post
the comment. Posting the comment before converting leaves the
comment on a non-draft PR if the conversion fails.

```bash
# 1. Convert to draft (GraphQL mutation — Airflow's `breeze` used
#    `convertPullRequestToDraft`; `gh pr ready <N> --undo` is the
#    CLI equivalent).
gh pr ready <N> --repo <repo> --undo

# 2. Post the violations comment
gh pr comment <N> --repo <repo> --body-file /tmp/pr-<N>-draft-body.md
```

Build `/tmp/pr-<N>-draft-body.md` from the `draft` template in
[`comment-templates.md`](comment-templates.md#draft-comment).
Write the file, `gh pr comment --body-file`, then delete the
temp file in the same turn. Body-file mode avoids shell-escape
issues for long markdown bodies.

On the `gh pr ready --undo` failing: surface the error, **do
not** post the comment. A comment that says "converted to draft"
on a still-open PR is a worse state than no comment at all.

### If the PR is already a draft

Skip the `gh pr ready --undo` step. Post only the comment. The
decision table in [`classify-and-act.md`](classify-and-act.md)
should have chosen `comment` instead in this case, but
double-check here as a guard.

### Collaborator-authored PRs

Do not draft a collaborator's PR. If somehow the action landed
as `draft` for a collaborator, fall back to `comment` with the
same body — no draft flip.

---

## `comment` — post violations / stale-review / ping comment

A single mutation. The template depends on the upstream
classification:

| Upstream | Body source |
|---|---|
| `deterministic_flag` with action `comment` | [`comment-templates.md#comment-only`](comment-templates.md) |
| `stale_review` with action `ping` | [`comment-templates.md#review-nudge`](comment-templates.md) |
| `deterministic_flag` (explicit ping action) | [`comment-templates.md#reviewer-ping`](comment-templates.md) |

```bash
gh pr comment <N> --repo <repo> --body-file /tmp/pr-<N>-comment.md
```

For a `ping` action, `@`-mention every stale reviewer plus the
PR author in the body — do not let the ping go without naming
the people it's for.

---

## `close` — close with comment and quality-violations label

Three mutations. Comment first (so the contributor sees the
reasoning), then close, then label. Closing without commenting
is perceived as hostile — do not do it.

```bash
# 1. Post the close comment
gh pr comment <N> --repo <repo> --body-file /tmp/pr-<N>-close.md

# 2. Close the PR
gh pr close <N> --repo <repo>

# 3. Add the quality-violations label (if the label exists on the repo)
gh pr edit <N> --repo <repo> --add-label "closed because of multiple quality violations"
```

Body template: [`comment-templates.md#close`](comment-templates.md).

If the label is missing (per `prerequisites.md#3`), skip the
label step with a one-line warning; the close + comment is
still valid.

`close` is always a **per-PR** action, never batched. Even
inside a `close` group, the maintainer confirms each PR
individually — a wrongly-closed PR is the hardest mistake to
recover from.

---

<a id="mark-ready"></a>

## `mark-ready` — add `ready for maintainer review` label

**Mandatory pre-mutation check.** Before adding the label, the
implementation MUST verify there are no GitHub Actions workflow
runs awaiting approval for the PR's head SHA. The classifier's
rollup-state and real-CI-context checks
(see [`classify-and-act.md#real-ci-guard`](classify-and-act.md)) are a
first line of defense; this REST check is the authoritative
second line that catches the case where the classifier was
right at fetch time but a new push or a freshly-indexed run
appeared since.

Reason: a PR whose real CI is held in `action_required` can have
`statusCheckRollup.state == SUCCESS` from fast bot checks
(`Mergeable`, `WIP`, `DCO`, `boring-cyborg`) while `Tests`,
`CodeQL`, and `Check newsfragment PR number` have not run.
Labelling such a PR "ready for maintainer review" is premature —
the maintainer queue fills with PRs whose CI has not actually
executed.

```bash
# Pre-check: index action_required runs repo-wide, then look up head SHA
head_sha=$(gh api "repos/<owner>/<repo>/pulls/<N>" --jq '.head.sha')
pending=$(gh api "repos/<owner>/<repo>/actions/runs?head_sha=${head_sha}&status=action_required&per_page=10" \
  --jq '.workflow_runs | length')
if [ "$pending" -gt 0 ]; then
  echo "refuse mark-ready: <N> has ${pending} workflow run(s) awaiting approval at ${head_sha}" >&2
  # Reclassify: this PR is really pending_workflow_approval, route accordingly.
  exit 2
fi

# Guard passed — apply the label.
gh pr edit <N> --repo <repo> --add-label "ready for maintainer review"
```

When the guard refuses, the implementation should **reclassify
the PR as `pending_workflow_approval`** (see
[`classify-and-act.md#decision-table`](classify-and-act.md), row 1) and
route to the workflow-approval flow rather than silently dropping
the mutation.

No comment is posted — the label is the signal. If the label
doesn't exist (per `prerequisites.md#3`), stop and surface the
error; this is the only action of the skill whose sole purpose
*is* the label, so there's no graceful degradation.

---

<a id="mark-ready-with-ping"></a>

## `mark-ready-with-ping` — promote a likely-addressed PR + ping reviewers

A composite of `mark-ready` plus a `ping` comment. Used when
the only `deterministic_flag` signal is unresolved review
threads **and** the
[`unresolved_threads_only_likely_addressed`](classify-and-act.md#unresolved_threads_only_likely_addressed)
sub-flag is true (the author has engaged with every unresolved
thread via a post-comment commit or an in-thread reply).

**Same mandatory pre-mutation guard as `mark-ready`.** Run
the `action_required` REST check first and refuse — by
reclassifying as `pending_workflow_approval` — if any workflow
run is awaiting approval. The reasoning in the
[`mark-ready`](#mark-ready--add-ready-for-maintainer-review-label)
section above applies identically here.

Order of operations: **post the comment first, then add the
label.** The comment names the reviewers and asks them to
re-look at the threads; the label then routes the PR into the
review queue. If the label is added first, the reviewers see a
"ready for maintainer review" notification before the comment
that explains *why* it was promoted, which reads as the bot
mark-ready'ing without context.

```bash
# 1. Pre-check: refuse if any workflow run is awaiting approval (same as mark-ready).
head_sha=$(gh api "repos/<owner>/<repo>/pulls/<N>" --jq '.head.sha')
pending=$(gh api "repos/<owner>/<repo>/actions/runs?head_sha=${head_sha}&status=action_required&per_page=10" \
  --jq '.workflow_runs | length')
if [ "$pending" -gt 0 ]; then
  echo "refuse mark-ready-with-ping: <N> has ${pending} workflow run(s) awaiting approval at ${head_sha}" >&2
  # Reclassify: this PR is really pending_workflow_approval.
  exit 2
fi

# 2. Post the ping comment (mark-ready-with-ping template).
gh pr comment <N> --repo <repo> --body-file /tmp/pr-<N>-mark-ready-with-ping.md

# 3. Apply the label.
gh pr edit <N> --repo <repo> --add-label "ready for maintainer review"
```

Body template:
[`comment-templates.md#mark-ready-with-ping`](comment-templates.md).

The body must `@`-mention every reviewer whose unresolved
thread we believe was addressed (so they get the notification)
and `@`-mention the PR author once at the top (so the
contributor sees the rationale). The list of reviewers comes
from the unresolved-thread reviewers captured during
classification.

If the label step fails after the comment is already posted,
**do not retry the comment** — surface the label-add error
to the maintainer and let them apply the label manually. A
duplicate ping comment is the worse of the two failure modes.

If the comment step fails (network blip, rate-limit), do not
proceed to the label — the maintainer would then see a PR
labelled `ready for maintainer review` with no explanation of
how it got there.

### Falling back to plain `ping`

If the post-confirmation drill-in (e.g. the maintainer pulled
the PR out of the group with `[P]ick`) reveals that the
threads are *not* actually addressed, the maintainer can
override the action to `ping`. The override drops the label
step entirely and posts the regular
[`reviewer-ping`](comment-templates.md#reviewer-ping) body
instead. See
[`interaction-loop.md#group-action-override`](interaction-loop.md).

---

## `rerun` — rerun failed CI workflow runs

Multi-step. We need to find the workflow runs for this PR's
head SHA, then rerun the failed ones.

```bash
# 1. List runs for this SHA
gh run list --repo <repo> --commit <head_sha> \
  --limit 50 \
  --json databaseId,name,status,conclusion

# 2. For each run where conclusion == "failure", rerun failed jobs
gh run rerun <run_id> --repo <repo> --failed
```

`--failed` reruns only the failed jobs in that run, which is
what the original `breeze` tool does. If you use plain
`gh run rerun` (no `--failed`) it reruns the whole workflow —
expensive and unnecessary.

### In-progress runs

If every failed run has `status != completed`, there's nothing
to rerun via `--failed`. Fall back to cancelling and restarting
the in-progress runs:

```bash
gh run list --repo <repo> --commit <head_sha> --status in_progress \
  --json databaseId --jq '.[].databaseId' |
  while read run_id; do
    gh run cancel "$run_id" --repo <repo>
    gh run rerun "$run_id" --repo <repo>
  done
```

Use this only when the `--failed` path turned up nothing —
cancelling in-progress runs discards current work.

### No runs found at all

Surface to the maintainer: "No workflow runs found for this
SHA — the PR may need a push or a rebase to re-trigger CI".
Fall through to suggesting `rebase` for next time.

---

## `rebase` — update the PR branch with base

**Never attempt this action when `mergeable == CONFLICTING`.**
GitHub's update-branch endpoint does a side-merge of the base
branch into the PR head; the merge fails deterministically
when the conflicts can't be auto-resolved, returns `422`, and
burns a round-trip. The skill empirically hit this on every
conflicting PR it tried during testing on `apache/airflow`.
The decision table in [`classify-and-act.md`](classify-and-act.md)
routes CONFLICTING PRs to `draft` (row 9) instead — if a `rebase`
action arrives here despite that, treat the conflict state itself
as a hard refuse.

Pre-flight guard:

```bash
merg=$(gh api graphql -F n=<N> -f query='
  query($n: Int!) {
    repository(owner:"<owner>",name:"<repo>") {
      pullRequest(number: $n) { mergeable }
    }
  }' --jq '.data.repository.pullRequest.mergeable')
if [ "$merg" = "CONFLICTING" ]; then
  echo "refuse: CONFLICTING — route to draft instead" >&2
  exit 2
fi
```

When the guard passes, single mutation via `gh`:

```bash
gh pr update-branch <N> --repo <repo>
```

This requires `gh` 2.20+. On older `gh`, fall back to:

```bash
gh api -X PUT repos/<owner>/<repo>/pulls/<N>/update-branch
```

GitHub replies with `202 Accepted` for a successful update — it
merges (or rebases, per repo settings) the base into the PR
branch. If the call still 422s despite a non-CONFLICTING
`mergeable` state (rare — usually means GitHub recomputed the
mergeable state between our guard and the call), surface the
error and **do not retry**; route to `draft` with the merge-
conflicts violation. Never burn successive round-trips on the
same PR in one session.

No comment is posted for `rebase` by default. The contributor
will see the merge commit (or rebased branch) in their PR.

---

## `ping` — nudge stale review / unresolved thread

Alias for `comment` with the `review-nudge` or `reviewer-ping`
body template, but distinct as an action so the maintainer can
confirm it separately from the generic `comment` action.

```bash
gh pr comment <N> --repo <repo> --body-file /tmp/pr-<N>-ping.md
```

**Pick the body variant deliberately — default to pinging the
author.** The skill has two body families:

- [`comment-templates.md#review-nudge`](comment-templates.md) —
  for `stale_review` (a `CHANGES_REQUESTED` review with newer
  author commits and no follow-up).
- [`comment-templates.md#reviewer-ping`](comment-templates.md) —
  for `deterministic_flag` → `ping` (unresolved review thread
  from a collaborator).

Each family has an **author-primary** variant (the default) and
a **reviewer-re-review** variant. Before drafting, inspect the
review thread + the post-review diff using the decision rule in
[`comment-templates.md#review-nudge`](comment-templates.md). Use
the reviewer-re-review variant **only** when that inspection
confirms the feedback has been addressed in a post-review
commit or resolved with an author reply in-thread; otherwise
stay with the author-primary variant so the to-do stays on the
correct desk.

The template **must** include `@`-mentions of every stale
reviewer *and* the PR author when using the reviewer-re-review
variant. In the author-primary variant, mention the author
first (they're the one who needs to act) and list the reviewers
as `<reviewers>` so they see the notification but the
responsibility is clearly on the author.

---

## `approve-workflow` — approve pending CI runs for first-time contributor

Two steps. **Inspect the diff first** — see
[`workflow-approval.md`](workflow-approval.md) for the safety
protocol. Only after the maintainer confirms the diff looks
non-malicious, approve:

```bash
# List pending workflow runs for this PR
gh api repos/<owner>/<repo>/actions/runs \
  -X GET \
  -f head_sha=<head_sha> \
  -f status=action_required \
  --jq '.workflow_runs[].id' |
  while read run_id; do
    gh api -X POST "repos/<owner>/<repo>/actions/runs/${run_id}/approve"
  done
```

No comment is posted for `approve-workflow`. Approval is
invisible to the contributor except for CI now running, which
is what they wanted.

### If the maintainer flagged suspicious

Route to `flag-suspicious` below — do **not** approve.

---

## `flag-suspicious` — close all open PRs by the author

The heaviest action in the skill. Reserved for PRs whose diff
contains clear tampering indicators (secret exfiltration, CI
pipeline modifications, `.env` writes, curl-to-shell patterns
introduced outside legitimate tool updates). See
[`workflow-approval.md#what-counts-as-suspicious`](workflow-approval.md)
for the signal list.

Scope: close **all** currently-open PRs authored by the
suspicious author, attach the `suspicious changes detected`
label, post a short explanatory comment. This is the action the
original `breeze` tool performed on the "flag as suspicious"
path.

```bash
# 1. List open PRs by the author
gh pr list --repo <repo> --author <author_login> --state open \
  --json number --jq '.[].number'

# 2. For each PR, in parallel — close + label + comment
for pr in $PR_NUMBERS; do
  gh pr comment "$pr" --repo <repo> --body-file /tmp/pr-<pr>-suspicious.md
  gh pr close "$pr" --repo <repo>
  gh pr edit "$pr" --repo <repo> --add-label "suspicious changes detected"
done
```

Body template: [`comment-templates.md#suspicious-changes`](comment-templates.md).

The comment is deliberately short and non-accusatory — the
action is the message, the comment is just the receipt.

**Require per-author confirmation**, not per-PR: the maintainer
confirms once for "close all N PRs by @<author>", then the
skill executes the whole set. This is the one time batch
execution is appropriate for destructive actions, because the
whole point is "this author's activity is being treated as a
unit". Sending N individual confirm prompts would dilute the
decision.

---

## Order-of-operations recap for destructive actions

For every action that includes a comment, post the comment
**before** the state change that hides it:

| Action | Order |
|---|---|
| `draft` | convert to draft → post comment |
| `comment` | post comment |
| `close` | post comment → close → label |
| `flag-suspicious` | post comment → close → label *(per PR in the batch)* |
| `mark-ready` | label only |
| `mark-ready-with-ping` | post comment → label |
| `rerun` | rerun (no comment) |
| `rebase` | update-branch (no comment) |
| `ping` | post comment |
| `approve-workflow` | approve (no comment) |

The `draft` case is the exception to "comment before state
change" because drafts still show comments fine. The `close`
case must be comment-first because closed-PR comments are
visible but the "PR closed" notification beats the comment
otherwise and the contributor reads the wrong order.

---

## Batching execution

When the maintainer accepts `[A]ll` on a group:

- Issue the mutations **in parallel** across PRs using parallel
  tool calls. `gh` is thread-safe from separate processes and
  the rate limit for mutations is per-request, not per-second
  batch.
- Cap parallelism at **5 concurrent mutations** to keep
  spurious errors from swamping the maintainer's screen.
- For `close` groups, the cap is **1** (sequential) even on
  `[A]ll` — we still walk them one-at-a-time, just without the
  per-PR confirm.

Update the session cache after each batch completes, not after
each mutation — a half-completed cache is a confusing debugging
artifact.

---

## Error handling

Mutations can fail for a handful of reasons. Handle them
specifically, not generically:

| Error | Handling |
|---|---|
| `HTTP 401/403` on a previously-working token | Stop the session, surface "token expired or permissions changed" |
| `HTTP 422` with "PR is already closed" | Log and continue (someone else closed it between our fetch and mutate) |
| `HTTP 422` with "label already applied" | Log and continue (idempotent) |
| `HTTP 404` on a PR number | Log and continue (PR was deleted — rare) |
| `HTTP 5xx` | Retry once after 2 seconds; on second failure, surface and continue with next PR |
| GraphQL error with `RATE_LIMITED` / `X-RateLimit-Remaining: 0` | Stop, surface remaining-quota info, let the maintainer decide whether to continue |

Do not wrap the entire session in a blanket `except`. Let
bugs surface.
