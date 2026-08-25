<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# worktree-init — share the main checkout's snapshot from a worktree

`adopt` and `upgrade` are **main-checkout-only**. A new git
worktree of an already-adopted tracker repo gets the framework
state by **symlinking** its `.apache-magpie/` directory to the
main checkout's snapshot, rather than maintaining its own copy.
One snapshot on disk, one upgrade source, every worktree always
current.

This sub-action is the worktree counterpart of `adopt`:

- **`adopt`** runs in the main checkout, fetches the snapshot,
  writes both lock files, and wires up symlinks.
- **`worktree-init`** runs in a worktree, validates the main is
  adopted, and points the worktree's `<snapshot-dir>` at the
  main's. Nothing is fetched; no lock files are written.

The skill is idempotent: re-running on a worktree that already
has the right symlink is a no-op.

## Step 0 — Pre-flight

1. **Confirm we are in a git worktree, not the main checkout.**
   Compare `git rev-parse --git-dir` against
   `git rev-parse --git-common-dir`. They are equal in the main
   checkout and different in a worktree. If equal, stop:

   > *"You appear to be in the main checkout (`<path>`).
   > `worktree-init` only runs in a worktree. Use
   > `/magpie-setup` (or `/magpie-setup upgrade`) here
   > instead."*

2. **Resolve the main checkout's path.** Take
   `$(cd "$(git rev-parse --git-common-dir)" && pwd)`; the
   parent of that is the main checkout's working tree. Pin
   the result as `<main>` for the rest of this flow.

3. **Confirm the main checkout is adopted.** Check that
   `<main>/.apache-magpie/` exists and that
   `<main>/.apache-magpie.lock` exists. If either is missing,
   stop:

   > *"The main checkout at `<main>` is not adopted yet. From
   > the main checkout: `cd <main> && /magpie-setup`. Re-run
   > `worktree-init` here once that is complete."*

4. **Inspect the worktree's `<snapshot-dir>` state.** Four
   possibilities, each handled below:

   | Current state | Action |
   |---|---|
   | Missing | Step 1 — create the symlink. |
   | Symlink to `<main>/.apache-magpie/` | No-op. Surface "already wired" and stop. |
   | Symlink to **something else** | Step 1 with a move-aside warning. The skill backs the existing link up, names what it pointed at, and asks the user to confirm before replacing. |
   | Regular directory (per-worktree snapshot from before this convention) | Step 1 with a move-aside warning. Back up the directory to `.apache-magpie.bak.<timestamp>` and create the symlink. **Do not** `rm -rf` without confirmation — the directory may hold uncommitted local edits the operator wants to preserve before the framework standardised on snapshot-from-main. |

## Step 1 — Create the snapshot symlink

```bash
ln -s <main>/.apache-magpie <worktree>/.apache-magpie
```

**Trusted external skill sources.** If the main checkout has a
`.apache-magpie-sources/` directory (the adopter trusts at least
one [external source](../../docs/skill-sources/README.md)), share
it the same way so this worktree's source-skill symlinks resolve
against one snapshot on disk:

```bash
# only when <main>/.apache-magpie-sources exists:
ln -s <main>/.apache-magpie-sources <worktree>/.apache-magpie-sources
```

Then verify the chain end-to-end:

- `ls -la <worktree>/.apache-magpie` returns a symlink pointing
  at `<main>/.apache-magpie`.
- `ls <worktree>/.apache-magpie/skills/` lists the
  same skills as `ls <main>/.apache-magpie/skills/`.
- when sources are in use, `ls -la <worktree>/.apache-magpie-sources`
  is likewise a symlink to `<main>/.apache-magpie-sources`.

## Step 1b — Wire up the worktree's per-target symlinks

The snapshot symlink in Step 1 only makes the framework's
*source* available to this worktree. The per-skill symlinks (the
gitignored entries the agent harness actually resolves) live in
**every active target dir** ([`agents.md`](agents.md) —
`.agents/skills/` (universal), `.claude/skills/`,
`.github/skills/`, plus any present holdout) and are
**per-worktree** — each working copy needs its own in every
target. A worktree branched from before adoption
landed, or branched from a state where the symlinks were
cleaned, has none on disk.

Compose the **effective family set** for this worktree:

- **Opt-in families** the project recorded — read from
  `<main>/.apache-magpie.lock` (the committed lock; the
  worktree shares it via git).
- **Always-on families** — every `setup-*` skill in the
  snapshot *except* `setup` itself, and every
  `list-*` skill, per
  [`SKILL.md` Golden rule 8](SKILL.md#golden-rules). These
  are added unconditionally, never read from the lock.

Wiring follows the **canonical-plus-relay** model
([`agents.md`](agents.md)), with no per-layout variation. For each
framework skill in the effective family set:

- **Canonical (`.agents/skills/`)** — ensure
  `<worktree>/.agents/skills/magpie-<skill>` →
  `../../.apache-magpie/skills/<skill>/` (the worktree's snapshot
  symlink from Step 1). Create if missing, repair if broken or
  pointing at the wrong path, leave alone if correct.
- **Relays (`.claude/skills/`, `.github/skills/`, any present
  holdout)** — ensure `<worktree>/<target>/skills/magpie-<skill>`
  → `../../.agents/skills/magpie-<skill>`. Create / repair / leave
  alone the same way.

All these entries are gitignored and per-worktree.

The worktree's target directories themselves — `.agents/skills/`,
`.claude/skills/`, `.github/skills/`, any holdout — are **not**
framework artefacts; they are checked into the repo as part of the
adopter's layout, so every worktree inherits them via the
ordinary `git worktree add` flow. `worktree-init` only wires the
`magpie-*` entries inside them; it does not touch the
directories.

Pick a framework skill symlink that should now exist in **each**
active target dir (e.g.
`<worktree>/.agents/skills/magpie-security-issue-sync/SKILL.md`
and `<worktree>/.claude/skills/magpie-security-issue-sync/SKILL.md`)
and confirm `readlink -f` resolves each into
`<main>/.apache-magpie/...` rather than dangling — same
sanity check as Step 1's bottom bullet, just now end-to-end
from agent-harness path through the worktree's symlink
through the snapshot symlink to the framework source, in every
target.

## Step 1c — Add the worktree to its own project-local sandbox allowlists

Defensive against
[issue #197](https://github.com/apache/magpie/issues/197) —
`sandbox.filesystem.allowRead: ["."]` does not in practice cover
the worktree's working dir, so reads under this worktree fail
under the sandbox until an explicit absolute path is added. See
[`setup-isolated-setup-install/SKILL.md` → Step P](../setup-isolated-setup-install/SKILL.md#step-p--project-root-coverage-in-the-sandbox-allowlists)
for the underlying rationale.

If `~/.claude/scripts/sandbox-add-project-root.sh` is installed,
invoke it from the worktree's working directory (no
`--all-worktrees` flag — only this one worktree needs adding;
the helper picks up the current worktree's
`git rev-parse --show-toplevel` and writes the abs path to
`<this-worktree>/.claude/settings.local.json`):

```bash
"$HOME/.claude/scripts/sandbox-add-project-root.sh"
```

**Invoke with `dangerouslyDisableSandbox: true`** — the target
`settings.local.json` is in Claude Code's built-in sandbox
`denyWithinAllow` set, so a sandboxed Bash write fails with
`operation not permitted`. Surface the bypass proposal to the
operator *before* invoking; name the helper and the target file
(`<worktree>/.claude/settings.local.json`); confirm. Reason:
*"writing project-local sandbox-allowlist entry (issue #197
fix)"*.

The helper writes to **project-local** scope, not user-scope —
each worktree carries its own `.claude/settings.local.json`
entry, and the per-worktree file is gitignored. The helper is
idempotent (no-op when already added) and exits 0 when
prereqs are missing (no `jq`, not in a git repo). Surface a
one-line recap row for the Step 2 summary:

- ✓ already covered, OR
- + added `<worktree-path>`, OR
- ⚠ helper not installed — `/magpie-setup-isolated-setup-install` to wire it up.

`worktree-init` does **not** fail when the helper is absent;
secure-agent isolation is independent of framework adoption.

## Step 1d — Seed the worktree's agent-guard PreToolUse hook

The committed `.claude/settings.json` wires the deterministic
guard ([`tools/agent-guard`](../../tools/agent-guard/README.md))
at `$CLAUDE_PROJECT_DIR/.claude/hooks/agent-guard.py` — a
**per-worktree** path. The script + its `guards.d/` are
adopter-installed local files synced into the **main** checkout by
[`adopt.md` Step 12 pass 1](adopt.md#step-12--post-install-sync--worktree-propagation--sandbox-allowlist--sanity-check)
/ [`upgrade.md` Step 6b](upgrade.md#step-6b--sync-locally-installed-hooks-and-configuration)
and **gitignored** ([`adopt.md` Step 7](adopt.md#step-7--gitignore-entries-fresh-only)).
Because they are gitignored, no worktree inherits them via git —
every worktree starts without the script and would run with the
guard **silently inactive** until seeded.

This is the agent-driven counterpart of the
[post-checkout hook's agent-guard seeding](adopt.md#step-10--worktree-aware-post-checkout-hook-fresh-only):
the git hook covers `git worktree add`, this step covers worktrees
that pre-date the hook or where its best-effort copy did not run.

Seed from the main checkout's already-synced copy — a plain file
copy, the same `<main>` resolved in Step 0:

```bash
# Only when the main has a guard and this worktree has none — never
# overwrite a copy the worktree already carries (worktree-local guards).
if [ -f "<main>/.claude/hooks/agent-guard.py" ] &&
   [ ! -f "<worktree>/.claude/hooks/agent-guard.py" ]; then
  mkdir -p "<worktree>/.claude/hooks/guards.d"
  cp "<main>/.claude/hooks/agent-guard.py" "<worktree>/.claude/hooks/agent-guard.py"
  cp "<main>/.claude/hooks/guards.d/"*.py "<worktree>/.claude/hooks/guards.d/" 2>/dev/null || true
fi
```

Idempotent: a no-op when the worktree already has the script, and
a no-op when the main has no agent-guard yet (an adopter who has
not run the Step 12 / Step 6b sync). Surface a one-line recap row
for Step 2:

- ✓ already present, OR
- + seeded from `<main>` (script + N guards), OR
- ⚠ main has no agent-guard yet — run `/magpie-setup` (or
  `/magpie-setup upgrade`) from the main checkout to sync it.

`worktree-init` does **not** fail when the main carries no
agent-guard; the guard is an opt-in adopter-side file, and the
worktree's framework-skill symlinks are usable without it.

## Step 2 — Recap

Print a short summary:

- The snapshot symlink that was just created or confirmed.
- The main checkout's resolved path.
- The framework version the main is pinned at (read from
  `<main>/.apache-magpie.lock`).
- The effective family set wired in Step 1b across every
  active target dir (`.agents/skills/`, the `.claude/`/`.github/`
  pair, any present holdout), split into *opt-in* and
  *always-on*, with per-skill ✓ / + / ↻ counts.
- The sandbox-allowlist recap row from Step 1c.
- The agent-guard recap row from Step 1d (✓ already present /
  + seeded / ⚠ main has no agent-guard yet).
- A reminder: `upgrade` from the main, not from the worktree.

## Inputs

| Flag | Effect |
|---|---|
| `--force` | Replace an existing `<snapshot-dir>` (symlink or regular dir) without the confirmation prompt. Skips the move-aside backup. Use only when you are sure the existing state holds nothing worth keeping. |
| `dry-run` | Show what the skill would do without writing anything. |

## Adopter overrides

This sub-action does **not** touch `.apache-magpie-overrides/`.
That directory is committed in the tracker repo and is
worktree-local by design — different branches may carry
different overrides. Symlinking it would conflate branches.

## What this sub-action is NOT for

- **Fetching the framework.** Use `adopt` in the main checkout
  first.
- **Upgrading the framework version.** Use `upgrade` in the
  main checkout; the symlink means every worktree sees the
  refreshed snapshot immediately.
- **Auto-running on `git worktree add`.** Adopters who want
  automatic worktree initialisation can wrap `git worktree add`
  with a script that calls `/magpie-setup worktree-init` —
  the framework does not install that wrapper.

## Failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| Step 0 step 3 stops with "main checkout not adopted" | The main has never run `adopt`. | `cd <main> && /magpie-setup`, then re-run `worktree-init` here. |
| `worktree-init` runs but skills still fail to resolve | The per-target `magpie-<skill>` symlinks (in `.agents/skills/`, the `.claude/`/`.github/` pair, or a holdout) are missing from this worktree's commit (the worktree was branched from before `adopt` ran on main). | Re-run `worktree-init` from main's `adopt` flow afterwards, or `git merge` / `git rebase` the branch carrying the symlink commits. |
| `<snapshot-dir>` is a regular directory and `--force` is not passed | A previous worktree snapshot is still on disk. | Re-run the skill, accept the move-aside prompt, then optionally inspect `.apache-magpie.bak.<timestamp>` for any non-snapshot content before deleting. |
