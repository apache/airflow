---
name: setup-steward
description: |
  Adopt and maintain the apache-steward framework in a project
  repo using the snapshot-based adoption mechanism. The single
  framework artefact that lives **committed** in an adopter's
  repo — every other framework skill is a symlink into a
  gitignored snapshot this skill manages. Sub-actions:
    `/setup-steward`         — first-time adoption (default)
    `/setup-steward upgrade` — refresh the gitignored snapshot
                                per the committed lock
    `/setup-steward verify`  — health check + drift detection
    `/setup-steward override <skill>` — open or scaffold an
                               agentic override for a framework
                               skill in `.apache-steward-overrides/`
when_to_use: |
  Invoke when the user says "adopt apache-steward", "adopt
  apache/airflow-steward", "set up steward in this repo",
  "follow .claude/skills/setup-steward", or the agent
  equivalent triggered by following the framework's README
  adoption instructions. Also for periodic maintenance:
  "upgrade steward", "verify steward setup", "check steward
  drift", "the snapshot is stale". This is the only framework
  skill that should be **copied** into an adopter's repo
  (every other framework skill is a symlink the adopt
  sub-action wires up).
license: Apache-2.0
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

<!-- Placeholder convention (see ../../AGENTS.md#placeholder-convention-used-in-skill-files):
     <project-config>           → adopter's `.apache-steward-overrides/` directory
     <snapshot-dir>             → `.apache-steward/` (gitignored snapshot of the framework)
     <committed-lock>           → `.apache-steward.lock` (committed — project's pin)
     <local-lock>               → `.apache-steward.local.lock` (gitignored — per-machine record)
     <upstream>                 → adopter's public source repo (the repo this skill is being run in)
     <framework-source>         → the apache-steward source we download a snapshot from
                                   — one of: signed zip from ASF dist, git tag, git branch.
                                   See [`docs/setup/install-recipes.md`](../../../docs/setup/install-recipes.md). -->

# setup-steward

This skill is **the only framework artefact an adopter
project commits**. Every other apache-steward skill (security,
pr-management) is a gitignored symlink into the gitignored
snapshot at `<snapshot-dir>` that this skill manages.

The adoption model is **snapshot + agentic overrides + drift-
aware updates** (not submodule, not marketplace, not vendored
copy):

- The framework is downloaded into `<snapshot-dir>` and
  **gitignored** in the adopter repo. The snapshot is a build
  artefact, not source.
- Three install methods are supported (see
  [`docs/setup/install-recipes.md`](../../../docs/setup/install-recipes.md)
  for verbatim copy-pasteable recipes):
  - **svn-zip** — released, signed zip from ASF distribution
    (recommended for production once releases ship).
  - **git-tag** — pinned to a specific git tag.
  - **git-branch** — tracks a branch tip (default: `main`,
    the WIP path).
- **Two lock files** record the framework version. The
  committed one declares what the project pins to; the local
  one records what each machine actually fetched. Drift
  between them is surfaced and remediated by
  `/setup-steward upgrade`.
- Symlinks from the adopter's skill directory into
  `<snapshot-dir>/.claude/skills/<framework-skill>/` make the
  framework's skills callable as if they lived in the adopter
  repo. The symlinks are also **gitignored** because their
  targets disappear on a fresh clone before `/setup-steward`
  runs.
- Adopter-specific modifications to framework workflows live as
  agent-readable instructions under
  `.apache-steward-overrides/<skill-name>.md` (committed). They
  invalidate or change steps the framework's skill would
  otherwise run. See
  [`overrides.md`](overrides.md) for the contract and
  [`docs/setup/agentic-overrides.md`](../../../docs/setup/agentic-overrides.md)
  for the design rationale.

## The two lock files

The framework's lock-file model splits **what the project pins
to** (committed) from **what this machine actually fetched**
(local). This split is the foundation of drift detection and
the multi-installer support.

### `<committed-lock>` — `.apache-steward.lock`

Committed at the adopter repo root. The **project's pin**.
Edited only by `/setup-steward`; do not modify by hand.

```text
# .apache-steward.lock — committed; the project's pin.

method: <git-branch | git-tag | svn-zip>
url:    <see per-method format below>

# For method=git-branch:
ref:    main

# For method=git-tag:
ref:    v1.0.0          # the tag name
commit: <SHA>           # the commit the tag pointed to when committed

# For method=svn-zip:
ref:    1.0.0           # the version number
sha512: <hash>          # the released zip's SHA-512 (for re-fetch verification)
```

The next adopter who runs `/setup-steward adopt` reads this
file and re-installs to the **same version** the project
declared. This is the core of the "adopt once, all subsequent
users get the same thing" promise.

### `<local-lock>` — `.apache-steward.local.lock`

Gitignored at the adopter repo root. The **local snapshot's
fingerprint**. Records what this machine fetched and when.

```text
# .apache-steward.local.lock — gitignored; per-machine.

source_method:    <git-branch | git-tag | svn-zip>
source_url:       <URL the snapshot was actually fetched from>
source_ref:       <branch / tag / version actually fetched>
fetched_commit:   <commit SHA on disk now>
fetched_at:       <ISO-8601 timestamp>
```

The drift check on every framework-skill invocation compares
this against `<committed-lock>` and surfaces any mismatch as a
proposed `/setup-steward upgrade`.

## Detail files in this directory

| File | Purpose |
|---|---|
| [`adopt.md`](adopt.md) | First-time adoption walk-through — recognise existing-snapshot vs needs-bootstrap, write the two lock files, ask the user which skill families to wire up, create the gitignored symlinks, scaffold `.apache-steward-overrides/`, install the post-checkout hook, update project docs. The default sub-action. |
| [`upgrade.md`](upgrade.md) | Refresh the gitignored snapshot per the committed lock, reconcile any agentic overrides + symlinks against the new framework structure, surface conflicts. Drives the on-drift remediation flow. |
| [`verify.md`](verify.md) | Read-only health check — snapshot present + intact, both lock files in sync, symlinks point at live targets, `.gitignore` correct, `.apache-steward-overrides/` exists, drift status (committed vs local), the `setup-steward` skill itself is current. |
| [`conventions.md`](conventions.md) | Adopter skills-dir convention auto-detection — flat `.claude/skills/<n>/`, the `.claude/skills/<n>` → `.github/skills/<n>/` double-symlink pattern (e.g. apache/airflow), or neither yet. |
| [`overrides.md`](overrides.md) | Agentic-override file management — open / scaffold an override for a framework skill, list existing overrides, help reconcile when the framework changes the underlying skill's structure on upgrade. |

## Golden rules

**Golden rule 1 — never modify the snapshot.** The
`<snapshot-dir>` is a build artefact, gitignored, and **read-
only** from an adopter's perspective. Every modification an
adopter wants must go into `.apache-steward-overrides/` (where
it is *committed* and survives the next `upgrade`). The skill,
and any other framework skill consulting overrides at run-time,
**never** writes to `<snapshot-dir>`.

**Golden rule 2 — `<committed-lock>` is the project's pin;
`<local-lock>` is per-machine truth.** They serve different
purposes and live in different places:

- `<committed-lock>` declares what version the *project* uses.
  Edited by the adopter who runs `/setup-steward adopt` first
  (or who later runs `/setup-steward upgrade` and accepts the
  new pin). Bumping it is a deliberate project-level action;
  the bump shows up in the `git diff` of the PR that proposed
  it.
- `<local-lock>` records what *this machine* installed. Updated
  silently by `/setup-steward adopt` and `/setup-steward
  upgrade`. Per-developer, per-checkout, per-worktree.

**Golden rule 3 — drift surfaces, drift gets remediated.**
Every framework skill (and `/setup-steward verify`) checks
`<committed-lock>` vs `<local-lock>` at the top of its run.
On mismatch the skill surfaces the gap and proposes
`/setup-steward upgrade`. The user accepts or defers; if they
accept, `upgrade`:

1. Deletes `<snapshot-dir>` outright.
2. Re-installs per the *committed* lock (the new version the
   project chose).
3. Refreshes the gitignored framework-skill symlinks — adds
   any new framework skills the user's family pick covers,
   removes any framework skills that were renamed away or
   removed.
4. Reconciles agentic overrides against the new framework
   structure (surfaces conflicts; never auto-rewrites).
5. Updates `<local-lock>` to the new fetch.

**Golden rule 4 — `.gitignore` keeps the adopter repo clean.**
Three things gitignored in the adopter repo:

- `<snapshot-dir>` (the entire framework snapshot — gigabytes
  potentially).
- `<local-lock>` (per-machine state).
- The symlinks `setup-steward adopt` creates in the adopter's
  skills directory (they target the gitignored snapshot, so
  they would dangle in a fresh clone).

**Committed**: this skill (`setup-steward`), the
`<committed-lock>`, the `.apache-steward-overrides/`
directory, the `.gitignore` entries themselves, any
project-doc updates the `adopt` sub-action makes.

**Golden rule 5 — follow the adopter's existing skills-dir
convention.** Different ASF projects already organise their
`.claude/skills/` differently (see
[`conventions.md`](conventions.md)). The `adopt` sub-action
detects which pattern is in place and matches it.

**Golden rule 6 — copy this skill, symlink the rest.** This
skill (`setup-steward`) is the **only** framework skill that
gets copied into an adopter repo. All other framework skills
are symlinked into the gitignored snapshot. Mixing the two —
copying a security skill, for instance — creates a
maintenance hazard: copies drift from the framework's source-
of-truth, and the drift-detection mechanism (which assumes
the framework version is the one in `<snapshot-dir>`)
silently mis-applies.

**Golden rule 7 — agentic overrides are read at run-time.**
Every framework skill that supports overrides starts its run
by checking `.apache-steward-overrides/<this-skill>.md` for
adopter-specific instructions and applying them before
executing the default behaviour. The override file is plain
markdown the agent interprets — no templating engine, no
patch tool. See
[`docs/setup/agentic-overrides.md`](../../../docs/setup/agentic-overrides.md)
for the contract.

## Sub-actions

The skill dispatches by the first positional argument:

| Invocation | Loads | Purpose |
|---|---|---|
| `/setup-steward` (no args) | [`adopt.md`](adopt.md) | First-time adoption (default). Idempotent — re-running on an already-adopted repo behaves like `verify`. |
| `/setup-steward adopt` | [`adopt.md`](adopt.md) | Same as no-arg — explicit form. |
| `/setup-steward upgrade` | [`upgrade.md`](upgrade.md) | Refresh snapshot per `<committed-lock>` + reconcile overrides + refresh symlinks. |
| `/setup-steward verify` | [`verify.md`](verify.md) | Read-only health check + drift status report. |
| `/setup-steward override <skill>` | [`overrides.md`](overrides.md) | Open / scaffold an override file. |

If the snapshot is missing (no `<snapshot-dir>/`) and
`<committed-lock>` exists, the skill treats any sub-action as
the recover-snapshot path: re-install per the committed lock
first, then continue.

## Inputs

| Flag | Effect |
|---|---|
| `from:<git-ref>` / `from:<version>` | Adopt or upgrade from a specific framework ref or version. Used during `adopt` (overrides the user prompt) and `upgrade` (overrides the committed lock for *this run only* — does NOT update the committed lock). |
| `method:<git-branch\|git-tag\|svn-zip>` | Pick the install method explicitly. Default during `adopt`: prompt the user. |
| `skill-families:<list>` | Comma-separated families to symlink (`security`, `pr-management`). Default on `adopt`: prompt. Default on `upgrade`: re-symlink the families currently linked. |
| `dry-run` | Show what the skill would do without writing anything. |

## What this skill is NOT for

- Not for installing the secure agent setup (sandbox, hooks,
  pinned tools). That is
  [`setup-isolated-setup-install`](../setup-isolated-setup-install/SKILL.md).
- Not for upgrading framework tools installed on the host
  (`bubblewrap`, `socat`, `claude-code` itself). That is
  [`setup-isolated-setup-update`](../setup-isolated-setup-update/SKILL.md).
- Not for syncing the user's `~/.claude-config` across
  machines. That is
  [`setup-shared-config-sync`](../setup-shared-config-sync/SKILL.md).
- Not for committing framework changes. Framework PRs go
  against `apache/airflow-steward` directly — the snapshot is
  read-only.

## Failure modes

| Symptom | Likely cause | Remediation |
|---|---|---|
| `/setup-steward verify` reports drift between committed and local locks | Project lead bumped `<committed-lock>` since this machine last fetched, or local snapshot is stale on a `main`-tracking adopter | `/setup-steward upgrade` |
| Snapshot present but symlinks dangle | Adopter ran `git clone` but not `/setup-steward` after — symlinks are gitignored but persist in their target's absence on disk | `/setup-steward verify --auto-fix-symlinks` (or `/setup-steward adopt`, idempotent) |
| Worktree off the adopter repo can't find framework skills | Worktrees off the adopter don't auto-inherit the gitignored snapshot | The `adopt` sub-action installs a `post-checkout` git hook that re-runs the snapshot install on worktree creation; verify the hook is present (`/setup-steward verify`) |
| `git clone` of an upstream PR sees no framework skills | Expected — the snapshot is gitignored, so a fresh clone has no `<snapshot-dir>`. The clone needs `/setup-steward` once before any framework skill is invocable | `/setup-steward` |
