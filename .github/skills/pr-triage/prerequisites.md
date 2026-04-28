<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Prerequisites

Before running any PR mutation the skill must confirm the
maintainer has the access needed to carry it out. A failure in
any of the blocking checks below is a **stop** — surface it to
the maintainer with the exact remediation command and do *not*
proceed to fetch PR data.

Keep this check cheap: a pre-flight that itself costs 5 GraphQL
calls defeats the whole rate-limit strategy of this skill.

---

## 1. `gh` CLI authenticated (blocking)

```bash
gh auth status
```

Pass condition: exit code `0` and the output lists an account
with `api.github.com` scope covering `repo` and `workflow`.
Capture the active login from the output for later — it is the
viewer login referenced throughout the other files.

On failure, stop and say:

> `gh` is not authenticated. Run `gh auth login` with SSH or
> HTTPS and the `repo, workflow` scopes, then re-invoke the
> skill.

This is the only check that *must* go through `gh auth status` —
do not try to parse tokens from the environment. A maintainer
running through `gh` also gets the TTY login prompt handled for
them when the token expires.

---

## 2. Viewer has collaborator access to `<repo>` (blocking for mutations)

Issue **one** GraphQL query that asks GitHub about both the
repository and the viewer's permission in that repository. Do
not issue two separate `gh api` calls:

```graphql
query($owner: String!, $repo: String!) {
  viewer { login }
  repository(owner: $owner, name: $repo) {
    name
    viewerPermission   # READ / TRIAGE / WRITE / MAINTAIN / ADMIN / null
  }
}
```

Pass condition: `viewerPermission` is `WRITE`, `MAINTAIN`, or
`ADMIN`. `TRIAGE` is sufficient for label/close/draft operations
but **not** for workflow approval — if the viewer is only
`TRIAGE`, note that `pending_workflow_approval` PRs will surface
in the proposal but the `approve-workflow` action will fall back
to "ask a WRITE-level maintainer".

On failure (`READ` or `null`), stop and say which repo the
viewer lacks access to, plus the recommended next step
(*"ask to be added as a collaborator"* or *"check you're
logged in as the right account"*).

Cache the result of this query in the session scratch file so
repeated invocations within the same working session don't
re-check.

---

## 3. Required labels exist on `<repo>` (non-blocking — degrade)

The skill uses three triage-specific labels:

| Label | Used by |
|---|---|
| `ready for maintainer review` | `mark-ready` action |
| `closed because of multiple quality violations` | `close` action when author has >3 flagged PRs |
| `suspicious changes detected` | `flag-suspicious` action from workflow approval |

Check them in the same query as step 2 by appending aliased
`label(name: "...")` lookups:

```graphql
query($owner: String!, $repo: String!) {
  viewer { login }
  repository(owner: $owner, name: $repo) {
    viewerPermission
    ready:        label(name: "ready for maintainer review") { id }
    closed_quality: label(name: "closed because of multiple quality violations") { id }
    suspicious:   label(name: "suspicious changes detected") { id }
  }
}
```

For each missing label, emit a single-line warning. The skill
**does not** auto-create labels — creating labels is a
repository-admin decision and silently adding them would
surprise the maintainers who manage the label set. Instead, the
relevant action falls back to "post the comment, skip the
label add, log a one-line warning that the label is missing".

On `apache/airflow`, all three labels are expected to exist and
a missing label is itself an anomaly worth flagging.

---

## 4. Session scratch cache available (non-blocking)

The scratch cache lives at
`/tmp/pr-triage-cache-<repo-slug>.json` where `<repo-slug>` is
`<owner>__<name>` (e.g. `apache__airflow`). It stores:

- viewer login and `viewerPermission` (so we don't re-check in
  the same session)
- `(pr_number, head_sha) -> classification` for the PRs already
  seen this session
- `(pr_number, head_sha) -> last_action` for the PRs already
  acted on
- a `label_ids` map so we don't re-resolve label node IDs per
  action

If the file is missing, initialise it empty. If the file is
corrupted (invalid JSON, wrong schema), delete it and warn the
maintainer — it's purely a performance cache, losing it is
harmless. Never block on cache read/write errors.

The session cache is invalidated by passing `clear-cache` on
invocation, and individual entries are invalidated naturally by
the `head_sha` key — a contributor pushing a new commit will
produce a new SHA and a cache miss.

Do **not** use this cache across pull-request runs for
decisions — always re-enrich the current page before acting on
it. The cache's job is to skip *classification*, not to skip
*verification*.

---

## 5. `gh` subcommand availability (non-blocking)

Verify that the `gh` install supports the subcommands the skill
uses:

```bash
gh run --help   # needs `approve` and `rerun`
gh pr --help    # needs `comment`, `close`, `edit`, `ready`, `update-branch`
gh api --help
```

Any missing subcommand means an older `gh` — warn and skip the
affected action (most commonly `gh pr update-branch`, which
landed in `gh` 2.20+; earlier versions need the REST call from
[`actions.md#rebase`](actions.md)).

---

## What to do when a prerequisite fails mid-session

If step 1 or 2 passes at start but a later mutation fails with a
permission error — e.g. the viewer's token expired, or they got
removed from the repo mid-sweep — stop the current group, tell
the maintainer, and print the summary for what *was* done this
session. Do not keep trying; retries on a permissions error
burn GraphQL budget without progress.
