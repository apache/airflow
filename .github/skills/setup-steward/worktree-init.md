<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# worktree-init — share the main checkout's snapshot from a worktree

`adopt` and `upgrade` are **main-checkout-only**. A new git
worktree of an already-adopted tracker repo gets the framework
state by **symlinking** its `.apache-steward/` directory to the
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
   > `/setup-steward` (or `/setup-steward upgrade`) here
   > instead."*

2. **Resolve the main checkout's path.** Take
   `$(cd "$(git rev-parse --git-common-dir)" && pwd)`; the
   parent of that is the main checkout's working tree. Pin
   the result as `<main>` for the rest of this flow.

3. **Confirm the main checkout is adopted.** Check that
   `<main>/.apache-steward/` exists and that
   `<main>/.apache-steward.lock` exists. If either is missing,
   stop:

   > *"The main checkout at `<main>` is not adopted yet. From
   > the main checkout: `cd <main> && /setup-steward`. Re-run
   > `worktree-init` here once that is complete."*

4. **Inspect the worktree's `<snapshot-dir>` state.** Four
   possibilities, each handled below:

   | Current state | Action |
   |---|---|
   | Missing | Step 1 — create the symlink. |
   | Symlink to `<main>/.apache-steward/` | No-op. Surface "already wired" and stop. |
   | Symlink to **something else** | Step 1 with a move-aside warning. The skill backs the existing link up, names what it pointed at, and asks the user to confirm before replacing. |
   | Regular directory (per-worktree snapshot from before this convention) | Step 1 with a move-aside warning. Back up the directory to `.apache-steward.bak.<timestamp>` and create the symlink. **Do not** `rm -rf` without confirmation — the directory may hold uncommitted local edits the operator wants to preserve before the framework standardised on snapshot-from-main. |

## Step 1 — Create the snapshot symlink

```bash
ln -s <main>/.apache-steward <worktree>/.apache-steward
```

Then verify the chain end-to-end:

- `ls -la <worktree>/.apache-steward` returns a symlink pointing
  at `<main>/.apache-steward`.
- `ls <worktree>/.apache-steward/.claude/skills/` lists the
  same skills as `ls <main>/.apache-steward/.claude/skills/`.

## Step 1b — Wire up the worktree's `<adopter-skills-dir>` symlinks

The snapshot symlink in Step 1 only makes the framework's
*source* available to this worktree. The `<adopter-skills-dir>`
symlinks (the gitignored per-skill entries the agent harness
actually resolves) are **per-worktree** — each working copy
needs its own. A worktree branched from before adoption
landed, or branched from a state where the symlinks were
cleaned, has none on disk.

Compose the **effective family set** for this worktree:

- **Opt-in families** the project recorded — read from
  `<main>/.apache-steward.lock` (the committed lock; the
  worktree shares it via git).
- **Always-on families** — every `setup-*` skill in the
  snapshot *except* `setup-steward` itself, and every
  `list-steward-*` skill, per
  [`SKILL.md` Golden rule 8](SKILL.md#golden-rules). These
  are added unconditionally, never read from the lock.

For each framework skill in the effective family set:

- If `<worktree>/<adopter-skills-dir>/<skill>` is missing —
  create it (gitignored).
- If it exists and points at the correct snapshot path —
  leave it alone.
- If it exists but is broken or points at the wrong path —
  repair it.

Reuse the convention detection from
[`conventions.md`](conventions.md). The pattern drives how
many layers the worktree's `<adopter-skills-dir>` needs:

- **Pattern A (flat)** — one layer at
  `.claude/skills/<n>`.
- **Pattern B (double-symlinked)** — two layers (inner at
  `.github/skills/<n>`, outer at `.claude/skills/<n>` →
  inner). Both gitignored.
- **Pattern D (single directory symlink)** — one layer at
  the canonical side (D.1: `.github/skills/<n>`;
  D.2: `.claude/skills/<n>`). The symlinked side resolves
  automatically through the directory symlink, so there is
  no per-skill plumbing to add or repair on that side.

The worktree's `.claude/skills` / `.github/skills` directory
symlink itself (for Pattern D) is **not** a framework
artefact — it is checked into the repo as part of the
adopter's layout, so every worktree inherits it via the
ordinary `git worktree add` flow. `worktree-init` does not
touch it.

Pick any framework skill symlink that should now exist (e.g.
`<worktree>/.claude/skills/security-issue-sync/SKILL.md`) and
confirm `readlink -f` resolves it into
`<main>/.apache-steward/...` rather than dangling — same
sanity check as Step 1's bottom bullet, just now end-to-end
from agent-harness path through the worktree's symlink
through the snapshot symlink to the framework source.

## Step 1c — Add the worktree to its own project-local sandbox allowlists

Defensive against
[issue #197](https://github.com/apache/airflow-steward/issues/197) —
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
- ⚠ helper not installed — `/setup-isolated-setup-install` to wire it up.

`worktree-init` does **not** fail when the helper is absent;
secure-agent isolation is independent of framework adoption.

## Step 2 — Recap

Print a short summary:

- The snapshot symlink that was just created or confirmed.
- The main checkout's resolved path.
- The framework version the main is pinned at (read from
  `<main>/.apache-steward.lock`).
- The effective family set wired in Step 1b, split into
  *opt-in* and *always-on*, with per-skill ✓ / + / ↻
  counts.
- A reminder: `upgrade` from the main, not from the worktree.

## Inputs

| Flag | Effect |
|---|---|
| `--force` | Replace an existing `<snapshot-dir>` (symlink or regular dir) without the confirmation prompt. Skips the move-aside backup. Use only when you are sure the existing state holds nothing worth keeping. |
| `dry-run` | Show what the skill would do without writing anything. |

## Adopter overrides

This sub-action does **not** touch `.apache-steward-overrides/`.
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
  with a script that calls `/setup-steward worktree-init` —
  the framework does not install that wrapper.

## Failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| Step 0 step 3 stops with "main checkout not adopted" | The main has never run `adopt`. | `cd <main> && /setup-steward`, then re-run `worktree-init` here. |
| `worktree-init` runs but skills still fail to resolve | The `<adopter-skills-dir>/<skill>` symlinks are missing from this worktree's commit (the worktree was branched from before `adopt` ran on main). | Re-run `worktree-init` from main's `adopt` flow afterwards, or `git merge` / `git rebase` the branch carrying the symlink commits. |
| `<snapshot-dir>` is a regular directory and `--force` is not passed | A previous worktree snapshot is still on disk. | Re-run the skill, accept the move-aside prompt, then optionally inspect `.apache-steward.bak.<timestamp>` for any non-snapshot content before deleting. |
