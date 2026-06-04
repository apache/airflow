---
name: setup
description: |
  Adopt and maintain the apache-steward framework in a project
  repo via the snapshot-based adoption mechanism. The only
  framework skill committed in an adopter's repo — every other
  skill is a symlink the adopt sub-action wires up.
  Sub-actions:
    `/magpie-setup`         — first-time adoption (default;
                                main-checkout only)
    `/magpie-setup upgrade` — refresh the gitignored snapshot
                                per the committed lock
                                (main-checkout only)
    `/magpie-setup worktree-init` — symlink a worktree's
                                snapshot to the main's
    `/magpie-setup verify`  — health check + drift detection
    `/magpie-setup override <skill>` — open or scaffold an
                                agentic override in
                                `.apache-magpie-overrides/`
    `/magpie-setup unadopt` — reverse the adoption (snapshot,
                               locks, symlinks, hook, doc
                               sections); preserves
                               `.apache-magpie-overrides/` by
                               default (main-checkout only)
when_to_use: |
  Invoke when the user says "adopt apache-steward", "adopt
  apache/airflow-steward", "set up steward in this repo",
  "follow .claude/skills/magpie-setup", or follows the
  framework's README adoption instructions. Also for periodic
  maintenance: "upgrade steward", "verify steward setup",
  "check steward drift", "the snapshot is stale".
argument-hint: "[adopt|upgrade|worktree-init|verify|override skill-name|unadopt]"
capability: capability:setup
license: Apache-2.0
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

<!-- Placeholder convention (see ../../AGENTS.md#placeholder-convention-used-in-skill-files):
     <project-config>           → adopter's `.apache-magpie-overrides/` directory
     <snapshot-dir>             → `.apache-magpie/` (gitignored snapshot of the framework)
     <committed-lock>           → `.apache-magpie.lock` (committed — project's pin)
     <local-lock>               → `.apache-magpie.local.lock` (gitignored — per-machine record)
     <upstream>                 → adopter's public source repo (the repo this skill is being run in)
     <framework-source>         → the apache-steward source we download a snapshot from
                                   — one of: signed zip from ASF dist, git tag, git branch.
                                   See [`docs/setup/install-recipes.md`](../../docs/setup/install-recipes.md). -->

# setup

This skill is **the only framework artefact an adopter
project commits**. Every other apache-steward skill (security,
pr-management, issue) is a gitignored symlink into the gitignored
snapshot at `<snapshot-dir>` that this skill manages.

The adoption model is **snapshot + agentic overrides + drift-
aware updates** (not submodule, not marketplace, not vendored
copy):

- The framework is downloaded into `<snapshot-dir>` and
  **gitignored** in the adopter repo. The snapshot is a build
  artefact, not source.
- Three install methods are supported (see
  [`docs/setup/install-recipes.md`](../../docs/setup/install-recipes.md)
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
  `/magpie-setup upgrade`.
- Symlinks from the adopter's skill directory into
  `<snapshot-dir>/skills/<framework-skill>/` make the
  framework's skills callable as if they lived in the adopter
  repo. **Each symlink is named `magpie-<framework-skill>`** —
  every framework skill is installed under a `magpie-` prefix
  so it is namespaced and never collides with the adopter's own
  skills (e.g. the snapshot's `skills/pr-management-triage/`
  becomes `.claude/skills/magpie-pr-management-triage`, invoked
  as `/magpie-pr-management-triage`). The symlinks are also
  **gitignored** because their targets disappear on a fresh
  clone before `/magpie-setup` runs.
- Adopter-specific modifications to framework workflows live as
  agent-readable instructions under
  `.apache-magpie-overrides/<skill-name>.md` (committed). They
  invalidate or change steps the framework's skill would
  otherwise run. See
  [`overrides.md`](overrides.md) for the contract and
  [`docs/setup/agentic-overrides.md`](../../docs/setup/agentic-overrides.md)
  for the design rationale.

## The two lock files

The framework's lock-file model splits **what the project pins
to** (committed) from **what this machine actually fetched**
(local). This split is the foundation of drift detection and
the multi-installer support.

### `<committed-lock>` — `.apache-magpie.lock`

Committed at the adopter repo root. The **project's pin**.
Edited only by `/magpie-setup`; do not modify by hand.

```text
# .apache-magpie.lock — committed; the project's pin.

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

The next adopter who runs `/magpie-setup adopt` reads this
file and re-installs to the **same version** the project
declared. This is the core of the "adopt once, all subsequent
users get the same thing" promise.

### `<local-lock>` — `.apache-magpie.local.lock`

Gitignored at the adopter repo root. The **local snapshot's
fingerprint**. Records what this machine fetched and when.

```text
# .apache-magpie.local.lock — gitignored; per-machine.

source_method:    <git-branch | git-tag | svn-zip>
source_url:       <URL the snapshot was actually fetched from>
source_ref:       <branch / tag / version actually fetched>
fetched_commit:   <commit SHA on disk now>
fetched_at:       <ISO-8601 timestamp>
```

The drift check on every framework-skill invocation compares
this against `<committed-lock>` and surfaces any mismatch as a
proposed `/magpie-setup upgrade`.

## Detail files in this directory

| File | Purpose |
|---|---|
| [`adopt.md`](adopt.md) | First-time adoption walk-through — recognise existing-snapshot vs needs-bootstrap, write the two lock files, ask the user which skill families to wire up, create the gitignored symlinks, scaffold `.apache-magpie-overrides/`, install the post-checkout hook, update project docs. The default sub-action. |
| [`upgrade.md`](upgrade.md) | Refresh the gitignored snapshot per the committed lock, reconcile any agentic overrides + symlinks against the new framework structure, surface conflicts. Drives the on-drift remediation flow. |
| [`verify.md`](verify.md) | Read-only health check — snapshot present + intact, both lock files in sync, symlinks point at live targets, `.gitignore` correct, `.apache-magpie-overrides/` exists, drift status (committed vs local), the `setup` skill itself is current. |
| [`conventions.md`](conventions.md) | Adopter skills-dir convention auto-detection — four patterns: A (flat `.claude/skills/<n>/`), B (per-skill `.claude/skills/<n>` → `.github/skills/<n>/` double-symlink), C (none yet), D (single directory symlink where one of `.claude/skills` / `.github/skills` is itself a symlink to the other; two orientations). |
| [`overrides.md`](overrides.md) | Agentic-override file management — open / scaffold an override for a framework skill, list existing overrides, help reconcile when the framework changes the underlying skill's structure on upgrade. |
| [`unadopt.md`](unadopt.md) | Reverse the adoption — remove snapshot, locks, symlinks, post-checkout hook, `.gitignore` entries, the adoption sections in `README.md` / `AGENTS.md` / `CONTRIBUTING.md`, and the committed `setup` skill itself. Preserves `.apache-magpie-overrides/` by default; `--purge-overrides` removes it too. Surfaces the full removal plan before any write. |

## Golden rules

**Golden rule 1 — never modify the snapshot.** The
`<snapshot-dir>` is a build artefact, gitignored, and **read-
only** from an adopter's perspective. Every modification an
adopter wants must go into `.apache-magpie-overrides/` (where
it is *committed* and survives the next `upgrade`). The skill,
and any other framework skill consulting overrides at run-time,
**never** writes to `<snapshot-dir>`.

**Golden rule 2 — `<committed-lock>` is the project's pin;
`<local-lock>` is per-machine truth.** They serve different
purposes and live in different places:

- `<committed-lock>` declares what version the *project* uses.
  Edited by the adopter who runs `/magpie-setup adopt` first
  (or who later runs `/magpie-setup upgrade` and accepts the
  new pin). Bumping it is a deliberate project-level action;
  the bump shows up in the `git diff` of the PR that proposed
  it.
- `<local-lock>` records what *this machine* installed. Updated
  silently by `/magpie-setup adopt` and `/magpie-setup
  upgrade`. Per-developer, per-checkout, per-worktree.

**Golden rule 3 — drift surfaces, drift gets remediated.**
Every framework skill (and `/magpie-setup verify`) checks
`<committed-lock>` vs `<local-lock>` at the top of its run.
On mismatch the skill surfaces the gap and proposes
`/magpie-setup upgrade`. The user accepts or defers; if they
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
- The symlinks `setup adopt` creates in the adopter's
  skills directory (they target the gitignored snapshot, so
  they would dangle in a fresh clone).

**Committed**: this skill (`setup`), the
`<committed-lock>`, the `.apache-magpie-overrides/`
directory, the `.gitignore` entries themselves, any
project-doc updates the `adopt` sub-action makes.

**Golden rule 5 — follow the adopter's existing skills-dir
convention.** Different ASF projects already organise their
`.claude/skills/` differently (see
[`conventions.md`](conventions.md)). The `adopt` sub-action
detects which pattern is in place and matches it.

**Golden rule 6 — copy this skill, symlink the rest; all under
the `magpie-` prefix.** This skill (source `skills/setup/`) is
the **only** framework skill that gets **copied** into an
adopter repo — committed as `.claude/skills/magpie-setup/`.
All other framework skills are **symlinked** into the
gitignored snapshot, each named `magpie-<framework-skill>`
(e.g. `magpie-security-issue-import` → `<snapshot-dir>/skills/security-issue-import/`).
The `magpie-` prefix namespaces every framework skill so it
never collides with an adopter's own skills. Mixing copy and
symlink — copying a security skill, for instance — creates a
copying a security skill, for instance — creates a
maintenance hazard: copies drift from the framework's source-
of-truth, and the drift-detection mechanism (which assumes
the framework version is the one in `<snapshot-dir>`)
silently mis-applies.

**Golden rule 7 — agentic overrides are read at run-time.**
Every framework skill that supports overrides starts its run
by checking `.apache-magpie-overrides/<this-skill>.md` for
adopter-specific instructions and applying them before
executing the default behaviour. The override file is plain
markdown the agent interprets — no templating engine, no
patch tool. See
[`docs/setup/agentic-overrides.md`](../../docs/setup/agentic-overrides.md)
for the contract.

**Golden rule 8 — two families are *always* installed; the
rest are opt-in.** Two skill families are wired up
unconditionally on every adopt / upgrade / worktree-init run
and the user is **never asked** about them:

- **`setup-*`** — every framework skill whose source name
  starts with `setup-` *except* the bootstrap `setup` itself
  (which is copied as `magpie-setup` per Rule 6, not
  symlinked). Concretely:
  `setup-isolated-setup-install`,
  `setup-isolated-setup-update`,
  `setup-isolated-setup-verify`, `setup-override-upstream`,
  `setup-shared-config-sync`, plus any new `setup-*` skill
  the framework grows in the future — each symlinked as
  `magpie-setup-*`.
- **`list-*`** — the discovery family; every framework skill
  whose source name starts with `list-`. Today this is
  `list-skills` only (symlinked as `magpie-list-skills`); the
  prefix lets the framework grow a discovery family without
  re-prompting every adopter.

These two families are not exposed in the `skill-families:`
prompt and not stored as user-selectable in the lock files;
every sub-action that wires symlinks always covers them in
addition to the user's opt-in family picks (`security`,
`pr-management`, `issue`). Dropping them is *not* a supported
configuration — the secure-setup and discovery flows the
framework ships depend on those skills being callable.

**Golden rule 9 — reload `setup` in-flight after a
self-update.** When a sub-action changes or creates the
content of the committed `setup` skill (in practice:
`adopt` recovering an out-of-date bootstrap, or `upgrade`'s
overwrite-from-snapshot step), the agent **re-reads the
modified files of this skill before continuing** the rest of
the current run. Concretely: after the copy lands on disk,
re-load `SKILL.md` and the sub-action file you are
currently executing (and any helper file you have already
opened, such as `conventions.md` or `overrides.md`), then
resume from the step after the overwrite. The reload runs as
the **first thing** that happens after the overwrite, before
any further reconciliation, symlink work, or doc updates.
The reason: the snapshot's skill version may have renamed
steps, added new sub-actions, or changed the symlink
contract; finishing the run against the *old* in-memory
copy of the skill would silently mis-apply the new
framework version the project just pinned to.

## Sub-actions

The skill dispatches by the first positional argument:

| Invocation | Loads | Purpose |
|---|---|---|
| `/magpie-setup` (no args) | [`adopt.md`](adopt.md) | First-time adoption (default; **main-checkout only**). Idempotent — re-running on an already-adopted repo behaves like `verify`. |
| `/magpie-setup adopt` | [`adopt.md`](adopt.md) | Same as no-arg — explicit form. Main-checkout only. |
| `/magpie-setup upgrade` | [`upgrade.md`](upgrade.md) | Refresh snapshot per `<committed-lock>` + reconcile overrides + refresh symlinks. **Main-checkout only** — worktrees pick up upgrades automatically via the symlink installed by `worktree-init`. |
| `/magpie-setup worktree-init` | [`worktree-init.md`](worktree-init.md) | **Worktree-only.** Symlink the worktree's `<snapshot-dir>` to the main checkout's so this worktree shares one framework state. No fetch, no lock files written; idempotent. |
| `/magpie-setup verify` | [`verify.md`](verify.md) | Read-only health check + drift status report. Works in both main and worktrees. |
| `/magpie-setup override <skill>` | [`overrides.md`](overrides.md) | Open / scaffold an override file. |
| `/magpie-setup unadopt` | [`unadopt.md`](unadopt.md) | Reverse the adoption. Removes snapshot, locks, symlinks, hook, doc sections, and this skill itself. Preserves `.apache-magpie-overrides/` unless `--purge-overrides` is passed. **Main-checkout only.** |

**Main-checkout-only sub-actions** (`adopt`, `upgrade`, `unadopt`)
detect their context via `git rev-parse --git-dir` ≠
`git rev-parse --git-common-dir` and refuse to run in a worktree
with a pointer back to the main checkout. The worktree counterpart
of `adopt` is `worktree-init`; for `upgrade`, every worktree
automatically sees the refreshed snapshot once the main runs
upgrade, because each worktree's `<snapshot-dir>` is a symlink to
the main's.

**`adopt` and `upgrade` always chain into `worktree-init` on every
linked worktree as their final pass.** The chain is unconditional
— even on a fresh adoption with no linked worktrees yet (the pass
becomes a no-op), even on an upgrade where every worktree already
looks wired (`worktree-init` is idempotent, repairs broken
symlinks, and adds new always-on-family entries the upgrade
introduced). The user does not need to remember to `cd` into each
worktree and re-run anything; the main-checkout sub-action
propagates state outward to the worktrees by itself. See
[`adopt.md` Step 12.2](adopt.md#step-12--post-install-sync--worktree-propagation--sandbox-allowlist--sanity-check)
and
[`upgrade.md` Step 6c](upgrade.md#step-6c--propagate-to-every-worktree-run-worktree-init-unconditionally).

If the snapshot is missing (no `<snapshot-dir>/`) and
`<committed-lock>` exists, the skill treats any sub-action as
the recover-snapshot path: re-install per the committed lock
first, then continue.

## Inputs

| Flag | Effect |
|---|---|
| `from:<git-ref>` / `from:<version>` | Adopt or upgrade from a specific framework ref or version. Used during `adopt` (overrides the user prompt) and `upgrade` (overrides the committed lock for *this run only* — does NOT update the committed lock). |
| `method:<git-branch\|git-tag\|svn-zip>` | Pick the install method explicitly. Default during `adopt`: prompt the user. |
| `skill-families:<list>` | Comma-separated **opt-in** families to symlink (`security`, `pr-management`, `issue`). Default on `adopt`: prompt. Default on `upgrade`: read the families list from `<committed-lock>` / `<local-lock>`, **auto-include any opt-in family the framework has introduced since the lock was written** (recorded back into the lock), and **ensure every framework skill in the effective family set has a valid symlink** — create or repair missing / broken symlinks, not just add new ones. The flag never accepts the always-on families (`setup-*` minus `setup` itself, and `list-*`); per [Golden rule 8](#golden-rules) those are wired up unconditionally on every run and there is no way to ask for them or opt out. |
| `--purge-overrides` | *(unadopt only)* Also `git rm -r` `.apache-magpie-overrides/`. Default: preserve. |
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
| `/magpie-setup verify` reports drift between committed and local locks | Project lead bumped `<committed-lock>` since this machine last fetched, or local snapshot is stale on a `main`-tracking adopter | `/magpie-setup upgrade` |
| Snapshot present but symlinks dangle | Adopter ran `git clone` but not `/magpie-setup` after — symlinks are gitignored but persist in their target's absence on disk | `/magpie-setup verify --auto-fix-symlinks` (or `/magpie-setup adopt`, idempotent) |
| Worktree off the adopter repo can't find framework skills | Worktrees off the adopter don't auto-inherit the gitignored snapshot | The `adopt` sub-action installs a `post-checkout` git hook that re-runs the snapshot install on worktree creation; verify the hook is present (`/magpie-setup verify`) |
| `git clone` of an upstream PR sees no framework skills | Expected — the snapshot is gitignored, so a fresh clone has no `<snapshot-dir>`. The clone needs `/magpie-setup` once before any framework skill is invocable | `/magpie-setup` |
| Project decided to stop using apache-steward | The reverse of adoption — remove the snapshot, locks, symlinks, hook, doc sections, and the `setup` skill itself. `.apache-magpie-overrides/` is preserved by default | `/magpie-setup unadopt` (add `--purge-overrides` to also drop the overrides directory) |
