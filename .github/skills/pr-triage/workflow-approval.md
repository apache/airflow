<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Workflow approval

First-time contributor PRs need manual approval before GitHub
Actions runs their workflows — this is GitHub's built-in
protection against contributors using CI as a crypto-mining
playground or exfiltrating secrets.

The `pending_workflow_approval` classification surfaces these
PRs. For each one, the maintainer must decide:

1. **Approve the workflow** — `approve-workflow` action, CI
   runs, triage continues on a subsequent sweep.
2. **Flag as suspicious** — `flag-suspicious` action, closes
   **all** open PRs by the same author and labels them.
3. **Skip** — leave the PR as-is for another maintainer.

The presentation is a **list-then-select** flow: print the full
inspection rubric for *every* PR in the group up front, then
present a single selection screen that lets the maintainer pick
which subset to approve in one go. The maintainer must type
indices explicitly — there is no "approve all" shortcut, and
the default for any PR with a suspicious-pattern match is
*never* approve. A single missed malicious approval lets
untrusted code run against the project's CI secrets, so the
selection step is the safety gate. The detail-print step
ensures the selection is informed.

---

## Inspection rubric

For each `pending_workflow_approval` PR the skill presents:

- PR title, author login, author profile signals (account age,
  global PR merge rate, contributions to other repos)
- the list of changed files
- a summary of the diff (additions / deletions per file)
- any workflow file (`*.yml` / `*.yaml` under `.github/`) that
  was touched
- **full diff** for workflow files, if any
- **full diff** for CI-adjacent files (`Dockerfile*`, `*.sh`,
  `scripts/ci/*`, `setup.cfg`, `pyproject.toml`, `.github/**`)
- **excerpt** of the rest of the diff — first 50 lines per
  non-CI file, plus any line matching
  [`#what-counts-as-suspicious`](#what-counts-as-suspicious)
  below

The full diff for non-CI files is **not** shown by default —
the maintainer is approving *workflow execution*, not reviewing
*code quality*. The point is "this change is not trying to
abuse CI".

Fetch the diff in one call:

```bash
gh pr diff <N> --repo <repo>
```

Parse it with the following filter logic before presenting:

1. Split into per-file hunks (`diff --git a/… b/…`).
2. For each hunk, classify the file:
   - **CI-adjacent** if path matches `.github/**`, `Dockerfile*`,
     `scripts/ci/**`, `*.sh`, `setup.cfg`, `pyproject.toml`.
   - **Other** otherwise.
3. For CI-adjacent files, include the full hunk.
4. For other files, include:
   - the first 50 added/removed lines, **plus**
   - every line matching the suspicious-pattern regexes below
     (whether or not it's in the first 50 lines).

Cap total presented diff at 2000 lines. If the diff is larger
than that, surface "diff is large (N lines) — review full diff
with `gh pr diff <N>`" and show only the CI-adjacent portion.

---

## What counts as suspicious

A non-exhaustive list of patterns that should flip the
maintainer's default from *approve* toward *flag-suspicious*.
These are surfaced inline during inspection — every match is
highlighted with its file + line number.

### Secret exfiltration

- Any change in `.github/workflows/**` that adds an outbound
  network call (`curl`, `wget`, `httpx`, raw `requests.post`)
  referring to a non-GitHub host
- Any change adding `${{ secrets.* }}` into an environment
  variable that's written to a log, file, or HTTP payload
- New `upload-artifact` actions pointing at `~/.aws`, `~/.ssh`,
  `/etc/passwd`, env dumps, or git config
- `echo` / `printenv` into any file inside the workflow
- Base64-encoding of env vars or secrets

### CI pipeline tampering

- Removal or `continue-on-error: true` on existing security-
  relevant workflow jobs (static checks, license check, DCO)
- Injection of `run: curl …| sh` or `… | bash` anywhere in the
  workflow body
- Changes to `permissions:` that escalate (`write-all`,
  expanded `contents`/`secrets` access)
- `pull_request_target` added where the workflow was previously
  `pull_request` (this is the canonical GHA privilege-
  escalation vector)
- New `workflow_dispatch` inputs that eval user content
- New GitHub Actions dependencies (`uses:`) pinned to a moving
  tag (`@main`, `@master`, `@latest`) instead of a SHA
- Self-hosted runner additions (`runs-on: self-hosted` newly
  introduced)

### Build-time tampering

- Modifications to `Dockerfile*` that add `curl | sh` /
  `npm install` from an arbitrary URL / `pip install` from a
  non-PyPI index that's not one of Airflow's known ones
- Changes to `setup.cfg` / `pyproject.toml` that add a
  `install_requires` referencing a typosquat-looking name
- Changes to `scripts/ci/**` that execute downloaded payloads

### Obfuscation tells

- Long base64-looking strings in a script file
- `eval` / `exec` of a variable that wasn't derived from a
  constant
- Bash dynamic constructs (`${IFS//…//}`, `$(printf …)` with
  hex-encoded literals)
- Unicode confusables in identifier names (use a quick check:
  any file with a non-ASCII character in its path or hunk)

### Non-suspicious but worth noting

Not causes for flagging, but surface to the maintainer anyway:

- Very large diff (>1000 lines) from a first-time contributor —
  this isn't malicious but it may be an indicator the PR was
  opened as a first step in a dependency-hijack attempt
- New contributor whose account is < 7 days old — GitHub's own
  warning already flags this, just echo it
- PR that modifies workflows **only** (no code change) — often
  legitimate, but worth the extra scrutiny

---

## Presenting the inspection to the maintainer

Two-phase layout. Phase 1 prints every PR's inspection block
back-to-back (no prompts in between — the maintainer scrolls
through them). Phase 2 is a single selection screen with the
group summary, where the maintainer picks indices.

### Phase 1 — print every inspection block

For each PR in the group, in the order returned by the
classifier (which is age-ascending so the freshest PRs land
first), print:

```
─────────────────────────────────────────────────────
[N] PR #NNNNN  "<title>"
Author: @<login>  (account: D days old, R repos, M merged PRs)
AuthorAssociation: FIRST_TIME_CONTRIBUTOR

Changed files (F):
  .github/workflows/new.yml           (+42 / -0)    ← WORKFLOW
  scripts/ci/deploy.sh                (+10 / -2)    ← CI
  airflow-core/src/airflow/x.py       (+3 / -1)

Suspicious-pattern matches: <count>
  - .github/workflows/new.yml:15 — "curl … | sh"
  - scripts/ci/deploy.sh:8        — echoes ${{ secrets.AWS_KEY }}

[diff excerpts — CI-adjacent full, other trimmed]
```

`[N]` is the 1-based selection index used in Phase 2. Print all
blocks before showing the selection screen — do not ask the
maintainer to confirm anything between them.

When a PR has no suspicious-pattern matches and the changed
files are all "other" (no CI-adjacent changes), say so
explicitly inline: *"No CI-adjacent changes and no suspicious
patterns matched"*. That's the green-light pre-classification.

### Phase 2 — selection screen

After all blocks are printed, render the summary table and
prompt for selection:

```
─────────────────────────────────────────────────────
pending_workflow_approval — N PRs · choose what to approve
─────────────────────────────────────────────────────

  [1] #65401  @alice    0 matches  no CI changes      ← default APPROVE
  [2] #65417  @bob      0 matches  no CI changes      ← default APPROVE
  [3] #65422  @carol    2 matches  workflow + script  ← default SKIP
  [4] #65445  @dave     0 matches  workflow only      ← default SKIP
                                   (CI changes — type to override)

Approve (indices, e.g. "1,2" or "default"):
Flag-suspicious (close ALL PRs by these authors):
Skip (leave for next sweep):  [implicit for any unlisted index]

  [Q]uit  — exit triage session
```

Defaults are encoded per row, not preselected — the
maintainer always has to type the indices to act. The
"default" line on each row is *guidance*, not auto-fill.

Default rules:

- 0 suspicious-pattern matches **and** no CI-adjacent file
  change → default **APPROVE**.
- Any suspicious-pattern match, or any CI-adjacent file change
  (even with 0 matches) → default **SKIP**.
- A row never defaults to FLAG. The maintainer always picks
  flag explicitly.

Input handling:

- The maintainer types comma-separated indices on each line.
  Whitespace is tolerated. Ranges (`1-3`) are accepted.
- The literal token `default` on the *Approve* line means
  "approve every row whose default was APPROVE". It is
  rejected on the *Flag* and *Skip* lines.
- An index can appear on at most one line — the same PR can't
  be both approved and flagged. Reject the input with a one-
  line error and re-prompt if it does.
- Empty *Approve* line + empty *Flag* line is allowed and
  means "skip everything in this group" (equivalent to
  pressing the old `[S]` key).
- `[Q]` quits the session immediately, regardless of the lines
  above. Pending input is discarded.

After the maintainer submits, print a one-screen confirmation
with the resolved verb per PR and the explicit list of
authors-to-be-affected for any flag, then ask `proceed?
[y/N]`. `y` runs all selected actions in sequence (approve
first, flag last); anything else discards the selection and
re-shows the selection screen with the same indices pre-
filled in the input lines so the maintainer can edit.

Never auto-approve. The skill's job is to surface signal, not
to decide. Every approval reaches a human via the explicit-
indices step.

---

## Execution after the decision

After the confirmation `y`, run the selected actions in this
fixed order so the cheapest, most reversible mutations land
first:

1. **Approve indices**: for each PR, run
   [`actions.md#approve-workflow`](actions.md) against the PR's
   head SHA. On success, update the session cache with
   `action_taken: "approve-workflow"` so the PR doesn't
   resurface in this session.
2. **Flag-suspicious indices**: for each PR, run
   [`actions.md#flag-suspicious`](actions.md) against the
   *author*, not just the PR. The flag is an author-level
   decision — all their currently-open PRs close with the
   `suspicious changes detected` label. The body comes from
   [`comment-templates.md#suspicious-changes`](comment-templates.md).
   Two flagged PRs by the same author collapse to a single
   author-level flag (don't double-close their PRs).
3. **Skip indices** (and any unlisted index): no mutations, no
   cache update, so another maintainer running the skill later
   picks them up fresh.

If any individual `approve-workflow` or `flag-suspicious` call
fails (network, permission, race), surface the error with the
PR number and continue with the rest of the queue. Never abort
the whole batch on one failure — the maintainer already
authorized each item, and partial completion is the same shape
as a per-PR session getting Ctrl-C'd between PRs.

`[Q]` (whether typed at the selection screen or the
confirmation prompt) leaves the session and prints the
summary as if every unprocessed item was skipped.

---

## If the viewer is only `TRIAGE`-level

The `approve` REST endpoint requires at least `WRITE`. A viewer
with `TRIAGE` can read and classify these PRs but cannot
approve. In that case:

- Phase 1 (printing the inspection blocks) runs unchanged so
  the TRIAGE-level maintainer can still spot suspicious
  patterns.
- Phase 2 swaps the *Approve* line label to
  *Request approval (indices)*. Indices listed there generate
  a short message the triager can post in
  `#airflow-maintainers` (or wherever the team coordinates),
  one PR per line, and log each as "pending WRITE-level
  approval" in the session.
- *Flag-suspicious* is still available — closing and labeling
  require WRITE on the PR, but a TRIAGE viewer does have WRITE
  on labels and can close PRs via `gh pr close`. If a TRIAGE
  viewer hits a permission error on the close, surface it and
  stop the flag step (other approve / skip indices already
  ran).

Cache `viewer.permission` from the pre-flight query — don't
re-check per PR.

---

## What this flow is NOT

- **Not a full code review.** The diff inspection looks for
  CI-abuse patterns, not for bugs or style. A bad bug that's
  clearly non-malicious still gets `approve` — the code review
  belongs in the separate review skill after CI has run.
- **Not an author judgment.** A new account is not suspicious
  on its own; the decision hangs on the diff. A 1-day-old
  account opening a typo-fix PR is fine.
- **Not reversible by the skill.** Once `approve-workflow` has
  been confirmed, CI runs against the contributor's code with
  full secret access. If the maintainer has doubts, the
  `S`kip path is always available — another pair of eyes can
  re-run the skill later.
