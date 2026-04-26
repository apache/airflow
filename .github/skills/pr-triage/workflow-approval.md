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

The decision is **always per-PR**, never batched — workflow
approval is the one category where batch-confirm is forbidden
regardless of group size, because a single missed malicious
approval lets untrusted code run against the project's CI
secrets. See the group-presentation rule in
[`interaction-loop.md#group-action-override`](interaction-loop.md).

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

Layout (one PR at a time):

```
─────────────────────────────────────────────────────
PR #NNNNN  "<title>"
Author: @<login>  (account: N days old, K repos, M merged PRs)
AuthorAssociation: FIRST_TIME_CONTRIBUTOR

Changed files (N):
  .github/workflows/new.yml           (+42 / -0)    ← WORKFLOW
  scripts/ci/deploy.sh                (+10 / -2)    ← CI
  airflow-core/src/airflow/x.py       (+3 / -1)

Suspicious-pattern matches: <count>
  - .github/workflows/new.yml:15 — "curl … | sh"
  - scripts/ci/deploy.sh:8        — echoes ${{ secrets.AWS_KEY }}

[diff excerpts — CI-adjacent full, other trimmed]
─────────────────────────────────────────────────────

Decide:
  [A]pprove workflow — CI runs, triage continues next sweep
  [F]lag suspicious — close ALL N open PRs by this author, add 'suspicious changes detected' label
  [S]kip             — no action, decide later
  [Q]uit
```

When there are no suspicious-pattern matches and the changed
files are all "other" (no CI-adjacent changes), say so
explicitly: *"No CI-adjacent changes and no suspicious patterns
matched"*. That's the green-light case for `approve`.

The maintainer's default keystroke should match the pattern
count:

- 0 matches, no CI changes → default `A` (approve)
- 1+ matches or any CI change → no default, maintainer must
  type the letter

Never auto-approve. The skill's job is to surface signal, not
to decide. The decision reaches a human.

---

## Execution after the decision

- **[A]pprove**: run [`actions.md#approve-workflow`](actions.md)
  against the PR's head SHA. On success, update the session
  cache with `action_taken: "approve-workflow"` so the PR
  doesn't resurface in this session.
- **[F]lag**: run [`actions.md#flag-suspicious`](actions.md)
  against the *author*, not just the PR. The flag is an
  author-level decision — all their currently-open PRs close
  with the `suspicious changes detected` label. The body comes
  from
  [`comment-templates.md#suspicious-changes`](comment-templates.md).
- **[S]kip**: no mutations, no cache update (so another
  maintainer running the skill later picks it up fresh).
- **[Q]uit**: leave the session, print the summary.

---

## If the viewer is only `TRIAGE`-level

The `approve` REST endpoint requires at least `WRITE`. A viewer
with `TRIAGE` can read and classify these PRs but cannot
approve. In that case:

- Present the inspection as normal (so the TRIAGE-level
  maintainer can still flag suspicious patterns).
- Replace the `[A]pprove` option with
  `[R]equest approval from a WRITE-level maintainer` — the
  skill composes a short message the triager can post in
  `#airflow-maintainers` or wherever the team coordinates, and
  logs the PR as "pending WRITE-level approval".
- `[F]lag suspicious` is still available — closing and labeling
  require WRITE on the PR but a TRIAGE viewer does have WRITE
  on labels and can close PRs via `gh pr close`. If a TRIAGE
  viewer does hit a permission error on the close, surface and
  stop.

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
