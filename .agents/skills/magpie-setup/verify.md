<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# verify — health check of the magpie integration + drift detection

Confirms the framework is wired in correctly so the rest of
the framework's skills resolve from the right paths, and
surfaces any **drift** between the committed lock (project
pin) and the local lock (per-machine fetch). Read-only by
default — surfaces gaps and remediation commands.

## Inputs

- `--auto-fix-symlinks` — *exception to read-only*. If the
  snapshot is present but symlinks are missing or dangling
  in **any active target dir** ([`agents.md`](agents.md) —
  `.agents/skills/`, `.claude/skills/`, `.github/skills/`, plus
  any present holdout), recreate them across all of them. Used
  by the post-checkout hook
  ([`adopt.md` Step 10](adopt.md)) on a fresh worktree
  where the gitignored symlinks didn't follow the
  checkout.

## Pre-flight

1. `git rev-parse --show-toplevel` — must succeed.
2. **Framework checkout?** Detect structurally (as in
   [`adopt.md` Step 0](adopt.md#step-0--pre-flight)):
   `skills/setup/SKILL.md` exists at the repo root with
   `name: magpie-setup` and `skills/list-skills/` is present. If
   so **and** `.apache-magpie.lock` records `method: local`, the
   repo is **self-adopted** — run the
   [Local self-adoption checks](#local-self-adoption-checks)
   instead of the snapshot checks below, then stop. A framework
   checkout with no `method: local` lock is simply not adopted
   yet — point at `/magpie-setup`.
3. If `<repo-root>/.apache-magpie.lock` is missing, the
   repo is not adopted. Surface and stop with a pointer at
   `/magpie-setup adopt`.

## Local self-adoption checks

Run these (and only these) when pre-flight detected a framework
checkout self-adopted with `method: local`. There is no snapshot,
no remote lock, and no per-machine lock to drift against — the
committed symlinks into the in-repo `skills/` source *are* the
adoption state.

1. **Marker lock.** `.apache-magpie.lock` parses and records
   `method: local`. ✓ when present; ✗ with a pointer at
   `/magpie-setup` otherwise.
2. **Symlinks resolve (canonical → source, relays → canonical).**
   In the canonical dir `.agents/skills/`, every `magpie-<n>` is a
   symlink whose target (`../../skills/<n>/`) resolves to a
   directory containing `SKILL.md`. In every **relay** target dir
   ([`agents.md`](agents.md) — `.claude/skills/`, `.github/skills/`,
   plus any present holdout), every `magpie-<n>` is a symlink whose
   target (`../../.agents/skills/magpie-<n>`) resolves through the
   canonical entry to the same `SKILL.md`. ✗ list any dangling or
   non-symlink entry — or any relay that points straight at the
   snapshot/source instead of at `.agents/skills/` — naming the
   target dir; remediation: re-run `/magpie-setup` (idempotent).
3. **Coverage.** Every `skills/<n>/` with a `SKILL.md` has a
   canonical `magpie-<n>` symlink in `.agents/skills/` and a
   matching relay in **every other active target dir**
   (unless a `skill-families:` filter was deliberately applied).
   ⚠ list any source skill with no link, per target; remediation:
   `/magpie-setup`.
4. **`.gitignore`.** Each active target dir's `<dir>/*` is
   ignored, with `!/<dir>/magpie-*` un-ignoring the committed
   symlinks — `.agents/skills/`, `.claude/skills/`,
   `.github/skills/`, and any present holdout. ✗ if any un-ignore
   line is missing (those symlinks would not be tracked).
5. **No remote leftovers.** No `.apache-magpie/` snapshot dir and
   no `.apache-magpie.local.lock` — local self-adoption uses
   neither. ⚠ surface either if found (a stale remote adoption was
   not cleaned up).

## The checks

Run all checks even on early failure (a missing snapshot at
check 1 doesn't tell us anything about the override
directory or doc updates — surface every check).

### 1. Snapshot present + intact

`<snapshot-dir>/` exists (as a directory or a symlink that
resolves to one) and contains the expected top-level files
(`README.md`, `AGENTS.md`, `.claude/skills/`, `tools/`).

- ✗ if missing **and we are in the main checkout** (`git
  rev-parse --git-dir` equals `git rev-parse --git-common-dir`)
  → run `/magpie-setup upgrade` (it gracefully handles the
  recover-snapshot case when the committed lock exists but
  the snapshot does not).
- ✗ if missing **and we are in a worktree** (the two dirs
  differ) → run `/magpie-setup worktree-init` to symlink
  `<snapshot-dir>` to the main checkout's. Do **not**
  propose `upgrade` — that creates a per-worktree snapshot,
  which is the bug `worktree-init` is designed to prevent.
- ⚠ if present as a regular directory **in a worktree** →
  legacy per-worktree snapshot. Suggest
  `/magpie-setup worktree-init` (with the move-aside flow)
  to convert into a symlink to the main's snapshot. Verify
  continues — the per-worktree snapshot is still functional,
  just wasteful.
- ✗ if missing top-level files → snapshot is corrupted;
  same remediation as the missing-snapshot case above.
- ⚠ if `<snapshot-dir>` is a symlink that resolves outside
  the same repo's main checkout — the operator pointed it
  at a different framework checkout deliberately. Surface
  the resolved target and continue; do not auto-remediate.

### 2. Both lock files exist + parse

`<committed-lock>` (`.apache-magpie.lock`) and
`<local-lock>` (`.apache-magpie.local.lock`) both parse.

- ✗ if `<committed-lock>` is missing → not adopted;
  redirect (already caught in pre-flight).
- ⚠ if `<local-lock>` is missing or unparsable → first
  install on this machine has not run, or the file was
  truncated. Suggest `/magpie-setup upgrade` to re-create
  the snapshot + write the local lock.

### 3. Drift between committed and local locks

This is the **core drift check**. The same logic every
framework skill runs at the top of its invocation.

Compare:

- `<committed-lock>.method` vs `<local-lock>.source_method`
- `<committed-lock>.url` vs `<local-lock>.source_url`
- `<committed-lock>.ref` vs `<local-lock>.source_ref`
- For `git-branch`: also compare upstream tip (the actual
  current `HEAD` of the branch on the remote) against
  `<local-lock>.fetched_commit`.

| Result | Severity |
|---|---|
| All match (and for `git-branch`, local is at upstream tip) | ✓ |
| Method or URL differ | ✗ — full re-install needed; remediation: `/magpie-setup upgrade` |
| Ref differs (e.g. project bumped tag, or `git-branch` local is behind upstream) | ⚠ — sync needed; remediation: `/magpie-setup upgrade` |
| `svn-zip` SHA-512 differs from the verification anchor in `<committed-lock>` | ✗ — security-flagged; the released zip changed content; investigate before upgrading |

### 4. `.gitignore` correctly excludes the snapshot + local lock + symlinks + project-local settings

Check that the entries from
[`adopt.md` Step 7](adopt.md) are present in
`<repo-root>/.gitignore`. Required:

- `/.apache-magpie/` (snapshot path)
- `/.apache-magpie.local.lock` (per-machine state)
- `/.claude/settings.local.json` (per-machine project-scope
  settings — written to by
  [`sandbox-add-project-root.sh`](../../tools/agent-isolation/sandbox-add-project-root.sh)
  as the per-worktree sandbox-allowlist defense for
  [issue #197](https://github.com/apache/magpie/issues/197);
  must never be committed since the content is machine-specific
  absolute paths)
- `__pycache__/` and `*.pyc` (byte-compiled artefacts emitted when
  framework skill scripts run from the adopter checkout; non-anchored
  so they match at any depth)

Recommended (a **uniform** `magpie-*` glob block per **active
target dir** — [`agents.md`](agents.md) — with no per-layout
variation):

- **Canonical target (`.agents/skills/`)** — always present:
  `/.agents/skills/magpie-*` with `!/.agents/skills/magpie-setup`.
- **Relay targets (`.claude/skills/`, `.github/skills/`)** — the
  same two-line block keyed on each dir
  (`/.claude/skills/magpie-*` with `!/.claude/skills/magpie-setup`,
  and likewise for `.github/skills/`).
- **Any present holdout** (`.windsurf/skills/`,
  `.goose/skills/`, …) — the same two-line block keyed on its own
  dir.

- ✗ if `/.apache-magpie/` is not gitignored — the snapshot
  is at risk of being accidentally committed.
- ✗ if `/.apache-magpie.local.lock` is not gitignored —
  per-machine state would leak into the repo.
- ✗ if `/.claude/settings.local.json` is not gitignored —
  per-machine absolute paths would leak into the repo; the
  sandbox-allowlist helper refuses to write to a non-ignored
  target as defense in depth, but `verify` surfaces the
  underlying `.gitignore` gap so the operator fixes the root
  cause.
- ⚠ if `__pycache__/` or `*.pyc` is not gitignored — byte-compiled
  artefacts from skill scripts could be accidentally committed.
- ⚠ if symlink patterns are not gitignored.

### 5. Symlinks point at live framework skills

Run this check across **every active target dir**
([`agents.md`](agents.md) — `.agents/skills/`, `.claude/skills/`,
`.github/skills/`, plus any present holdout), not just the
`.claude/`/`.github/` pair.

For each `magpie-*` symlink under any active target dir —
canonical ones resolving (via `.agents/skills/`) into
`.apache-magpie/skills/<name>/`, relays resolving through
`../../.agents/skills/magpie-<name>` to the same:

- ✓ if it resolves to a live skill.
- ✗ if dangling (target deleted or snapshot missing), or a relay
  pointing straight at the snapshot instead of at the canonical
  `.agents/skills/` entry, naming the target dir. Remediation:
  `/magpie-setup adopt` (idempotent re-run) or this same skill
  with `--auto-fix-symlinks`.

For each framework skill in the snapshot **not** symlinked
in a given active target dir, classify it (a skill missing
from `.agents/skills/` is as much a gap as one missing from
`.claude/skills/`):

- **Always-on family** (every `setup-*` *except*
  `setup` itself, and every `list-*` — per
  [`SKILL.md` Golden rule 8](SKILL.md#golden-rules)) →
  surface as ✗. These families are not opt-in; missing
  symlinks here indicate a broken install or a skipped
  upgrade pass. Remediation:
  `/magpie-setup verify --auto-fix-symlinks` (cheap), or
  `/magpie-setup upgrade` (covers the family-wide pass).
- **Opt-in family the project picked** (per
  `<committed-lock>` / `<local-lock>`) → surface as ✗. The
  project declared the family but the install is missing a
  skill from it. Remediation as above.
- **Opt-in family the project did NOT pick** → surface as
  ⚠. The user may have intentionally not picked that
  family; the warning prompts a decision.

The `--auto-fix-symlinks` path repairs the first two
classes in place — in **every active target dir** — without
prompting; the ⚠ class needs an explicit `/magpie-setup adopt`
re-run with the family added to the pick.

### 6. `.apache-magpie-overrides/` exists + has the README

`<repo-root>/.apache-magpie-overrides/` is a directory
with the `README.md` scaffold from
[`adopt.md` Step 9](adopt.md).

- ✗ if missing → `/magpie-setup adopt` (idempotently
  re-creates).
- ⚠ if present but `README.md` is missing — the directory
  may have been hand-created. Suggest re-running
  `/magpie-setup adopt`.

### 7. The `setup` skill itself is up to date

Compare the canonical committed `setup` skill
(at `.agents/skills/magpie-setup/`) against the
snapshot's `.apache-magpie/skills/setup/`.

- ✓ if same content.
- ⚠ if different — the adopter's committed copy has
  drifted from the snapshot. The remediation depends on
  *which way* the drift goes:

  - **Snapshot is newer than the committed copy** (typical
    case after a framework upgrade where the adopter has
    not yet rerun `/magpie-setup upgrade`). Run
    `/magpie-setup upgrade` — its
    [Step 4b](upgrade.md#step-4b--overwrite-the-committed-setup-from-the-new-snapshot--reload-in-flight)
    auto-overwrites the committed copy with the snapshot's
    version, **reloads the skill in-flight** so the rest of
    the upgrade run executes against the new bootstrap
    content (per
    [`SKILL.md` Golden rule 9](SKILL.md#golden-rules)),
    surfaces local modifications first if any exist, and
    lands the change in `git status` for the user to commit.
  - **Committed copy is newer than the snapshot** (the
    adopter modified the bootstrap skill directly; an
    anti-pattern per the framework's hard rule). The
    framework-side fix is to upstream the modifications as
    a PR against `apache/magpie`; the local fix
    is to revert the modifications and use
    `.apache-magpie-overrides/` instead.

### 8. Post-checkout hook installed *and content matches the framework's expected*

Two sub-checks on `<repo-root>/.git/hooks/post-checkout`:

1. **Presence + executable.** File exists, is executable,
   and carries the current hook body — the sandbox-allowlist
   helper chain **and** the agent-guard seeding block (see
   [`adopt.md` Step 10](adopt.md#step-10--worktree-aware-post-checkout-hook-fresh-only)).
   It must **not** contain the long-removed
   `/magpie-setup verify --auto-fix-symlinks` line (a slash
   command is not shell-callable; it printed a spurious error on
   every checkout).
   - ⚠ if missing — strictly optional, but worktrees off this
     repo will then not get their sandbox allowlist or
     agent-guard seeded automatically on `git worktree add`
     (they fall back to `/magpie-setup worktree-init`). Print
     the install recipe.

2. **Content drift vs the framework's expected.** Diff the
   installed hook against the framework's expected hook
   content (the canonical source is shipped under the
   snapshot — locate it during the check). Same logic
   applies for any other adopter-installed local hook or
   config file the framework grows in future.
   - ✓ if content matches.
   - ⚠ if drifted and the diff looks like operator
     hand-edits — surface the diff; remediation is to run
     `/magpie-setup` (adopt or upgrade), whose
     hook+config-sync pass re-installs from the snapshot
     after asking about hand-edits.
   - ✗ if drifted and the installed content is clearly
     stale (older framework version's recipe) — same
     remediation, no operator prompt needed; the sync
     pass overwrites silently.

### 8a. agent-guard PreToolUse hook installed and wired

Three sub-checks for the deterministic guard
([`tools/agent-guard`](../../tools/agent-guard/README.md)):

1. **Script present + matches the snapshot.** `<repo-root>/.claude/hooks/agent-guard.py`
   exists and its content matches the snapshot's
   `tools/agent-guard/src/agent_guard/__init__.py`.
   - ⚠ / ✗ on missing / stale — remediation is `/magpie-setup`
     (adopt or upgrade), whose sync pass re-installs it.
2. **`guards.d` populated.** `<repo-root>/.claude/hooks/guards.d/`
   exists and contains every guard the snapshot ships — the
   engine's bundled `guards.d/*.py` **and** each skill-owned
   `skills/*/guards/*.py` (e.g. `mention`, `mark_ready`,
   `security_language`). Flag a *missing* expected guard or a stale
   copy; extra locally-added `*.py` are fine. A missing skill guard
   means that skill's deterministic protection is silently inactive
   — remediation is `/magpie-setup` (adopt/upgrade), which re-collects.
3. **Hook wired in settings.json.** `<repo-root>/.claude/settings.json`
   has a `hooks.PreToolUse` entry (matcher `Bash`) whose command
   runs `agent-guard.py`.
   - ⚠ if missing — the script is present but not active; print
     the one-time wiring snippet (see
     [`adopt.md` Step 12](adopt.md#step-12--post-install-sync--worktree-propagation--sandbox-allowlist--sanity-check))
     for the maintainer to apply (settings.json is agent-edit-denied).

The script + `guards.d` are **gitignored** framework code
([`adopt.md` Step 7](adopt.md#step-7--gitignore-entries-fresh-only)),
synced from the snapshot rather than committed — so a *missing*
script is the expected state of a fresh checkout, not a defect, and
the fix is always a re-sync (never `git add`). When this check runs
**inside a worktree**, the script + `guards.d` are per-worktree
files (the `settings.json` wiring resolves
`$CLAUDE_PROJECT_DIR/.claude/hooks/agent-guard.py` against the
worktree root). The remediation for a *missing* script in a worktree
is not the main-checkout sync but
[`worktree-init.md` Step 1d](worktree-init.md#step-1d--seed-the-worktrees-agent-guard-pretooluse-hook)
(or the post-checkout hook on the next `git worktree add`), which
seeds it from the main checkout's already-synced copy.

### 8b. Sandbox-allowlist coverage of the current worktree

Defensive cross-check for
[issue #197](https://github.com/apache/magpie/issues/197):
`sandbox.filesystem.allowRead: ["."]` does not in practice cover
CWD under the harness, so `/magpie-setup` (adopt, upgrade,
worktree-init) chains into
`~/.claude/scripts/sandbox-add-project-root.sh` to add explicit
absolute paths to each worktree's own project-local settings.
This check verifies that chain landed for the *current* worktree.

For the current worktree (resolved via
`git rev-parse --show-toplevel`):

- ✓ if the absolute path appears in **both**
  `<worktree>/.claude/settings.local.json`'s
  `sandbox.filesystem.allowRead` and `sandbox.filesystem.allowWrite`.
- ✗ if missing from either array, **and** the helper script
  `~/.claude/scripts/sandbox-add-project-root.sh` is installed
  — remediation:
  `~/.claude/scripts/sandbox-add-project-root.sh`
  (no `--all-worktrees` needed — just this worktree), or
  re-run `/magpie-setup` (adopt/upgrade) which chains into
  the helper as part of its Step 12 / Step 6c sandbox-allowlist
  pass.
- ⚠ if missing from either array **and** the helper script is
  absent — the operator has not run
  `/magpie-setup-isolated-setup-install` yet. Suggest that skill.
  Not ✗ because secure-agent isolation is independent of
  framework adoption, and an adopter who runs without the
  sandbox enabled has nothing to lose by the missing entry.
- ⚠ if `<worktree>/.claude/settings.local.json` is absent
  entirely — same remediation (re-run the helper or
  `/magpie-setup-isolated-setup-install`). The file is auto-created
  by the helper on first run.
- ✗ if `<worktree>/.claude/settings.local.json` exists AND
  is **not** gitignored (cross-check via `git check-ignore`).
  Per the security rationale in
  [`docs/setup/secure-agent-setup.md` → *Security rationale — why project-local is safe to write to*](../../docs/setup/secure-agent-setup.md#security-rationale--why-project-local-is-safe-to-write-to),
  the per-machine settings.local.json must never be committed.
  Remediation: add `/.claude/settings.local.json` to the
  adopter's `.gitignore` (also surfaced by check 4 above).

The check scopes to the current worktree only, not the full
`git worktree list`, because each worktree carries its own
project-local settings file — `/magpie-setup verify` running
in worktree A has no business asserting on worktree B's file
(which it cannot even reliably read without crossing into
another working tree's path).

This check is read-only on the framework state. The defence
is layered: `/magpie-setup` writes during adopt/upgrade,
`setup-isolated-setup-verify` adds a live read+write probe
(check 8 there), and this check is the cheap static cross-check
to surface drift between the two skill families.

### 8c. Stale agent-worktrees under `.claude/worktrees/`

Detect worktrees the agent (or a prior session) created under
`<repo-root>/.claude/worktrees/` that have been left lying around
beyond their useful life. **Main-checkout only** — worktrees can
only be inspected from the checkout that owns them, and the
`git worktree list` output is the same across the family anyway.

Stale agent-worktrees are a real friction source: they hold
branches (typically `main`, since `EnterWorktree` defaults to
branching from `main`), so a subsequent `git checkout main` from
the main checkout fails with *"main is already used by worktree
at …"* — silently, in the middle of a longer command pipeline,
producing confusing downstream failures. A session that ended
without explicit `ExitWorktree(action: "remove")` leaves the
worktree on disk; the next session has no way to know it is
abandoned.

The check:

1. Run `git worktree list --porcelain` and filter to entries
   whose `worktree` path is under `<repo-root>/.claude/worktrees/`.
2. For each, compute the **age** — the maximum of:
   - the worktree directory's `mtime` (file-system signal — how
     long since anything inside changed); and
   - `git -C <worktree> log -1 --format=%cI HEAD`'s commit time
     (git-state signal — how recent the latest commit on the
     worktree's branch is).

   The max-of-two avoids two failure modes: a worktree whose
   commits are old but whose files were touched recently (still
   active) and a worktree whose files are old but whose branch
   was recently rebased (still in use). Both look fresh to one
   of the signals alone.

3. Bucket the result against a threshold (default: **7 days**;
   adopter override via `worktree_stale_days` in
   `<project-config>/magpie-setup.md` — if absent, default
   stands):
   - ✓ if age ≤ threshold
   - ⚠ if age > threshold AND the worktree has zero
     uncommitted changes (`git -C <worktree> status --porcelain`
     is empty) — surface the path, age, branch name, and
     propose `git worktree remove <path>` as the cleanup.
   - ✗ if age > threshold AND the worktree has uncommitted
     changes — surface the same info plus an explicit
     *"uncommitted changes present"* warning, and propose
     two-step cleanup: first commit-or-stash, then
     `git worktree remove --force <path>` (or
     `EnterWorktree(path)` to enter it interactively and
     decide).

4. The check is **read-only**: it never auto-removes a
   worktree, never force-anything. The proposal lands in the
   verify-report and the operator chooses to act.

**Threshold rationale.** Agent-worktrees are designed for
per-task isolation: open, work, close. A worktree older than
7 days is overwhelmingly a session that ended without explicit
cleanup. Lower thresholds (3 days, 1 day) hit false-positive
on multi-day tasks that legitimately stretch across sessions;
higher thresholds (14, 30 days) let the bug class persist
long enough to actually break a `git checkout main` weeks
later.

**Why this check exists separately from worktree-init.**
`worktree-init` wires up a newly-created worktree. There is
no symmetric step for end-of-life: `EnterWorktree(action:
"remove")` from inside a session removes it cleanly, but
sessions that crash, get interrupted, or end via context-
window-exhaustion leak. This check is the periodic cleanup
sweep that catches the leakage.

### 8d. Permission allow-list hygiene

Audit the adopter's per-machine permission allow-list for
patterns that grant arbitrary code execution, and surface
the recommended read-only patterns the framework's skills
use heavily. **Local-state only** — the framework never
mutates `.claude/settings*.json`; this check produces
*proposals* the operator confirms before any write.

Two files to read:

- `<repo-root>/.claude/settings.json` (committed,
  project-wide).
- `<repo-root>/.claude/settings.local.json` (gitignored,
  per-machine — same security model as
  `.apache-magpie.local.lock`).

For each, parse the JSON, walk `permissions.allow[]`, and
bucket each entry against two canonical lists.

**Forbidden — propose removal (✗ per entry hit):** broad
wildcards over interpreters, shells, and package runners.
Treat any of the following allow-list strings as an
arbitrary-code-execution hole, regardless of how the
adopter justified adding them:

- `Bash(python *)`, `Bash(python3 *)`,
  `Bash(node *)`, `Bash(bun *)`, `Bash(deno *)`,
  `Bash(ruby *)`, `Bash(perl *)`, `Bash(php *)`,
  `Bash(lua *)`
- `Bash(bash *)`, `Bash(sh *)`, `Bash(zsh *)`,
  `Bash(fish *)`, `Bash(eval *)`, `Bash(exec *)`,
  `Bash(ssh *)`
- `Bash(npx *)`, `Bash(bunx *)`, `Bash(uvx *)`,
  `Bash(uv run *)`
- `Bash(npm run *)`, `Bash(yarn run *)`,
  `Bash(pnpm run *)`, `Bash(bun run *)`,
  `Bash(make *)`, `Bash(just *)`, `Bash(cargo run *)`,
  `Bash(go run *)`
- `Bash(gh api *)`, `Bash(docker run *)`,
  `Bash(docker exec *)`, `Bash(kubectl exec *)`,
  `Bash(sudo *)`

The list mirrors the *"Never allowlist a pattern that
grants arbitrary code execution"* rule from Claude Code's
user-level `/fewer-permission-prompts` slash command — the
framework's copy lives here so adoption itself is not
silently contingent on a sibling skill being present.
**It is not exhaustive**: an allow-list entry that fits
the *same category* (anything that can spawn an arbitrary
process or shell out via a flag) is a ✗ even if its exact
token does not appear above.

**Recommended — propose addition (⚠ per entry missing):**
narrow read-only patterns the framework's skills invoke
often. An adopter who picks up the `security` family will
hit these constantly; pre-allowing them removes the
repetitive confirmation prompts without weakening the
boundary. Tailor the recommendation to the families the
adopter opted into via
[`<committed-lock>` → `skill-families`](adopt.md#step-5--pick-the-skill-families):

- **`security` family** —
  - `mcp__claude_ai_Gmail__get_thread`
  - `mcp__claude_ai_Gmail__search_threads`
  - `mcp__claude_ai_Gmail__list_drafts`
  - `mcp__claude_ai_Gmail__list_labels`
  - `mcp__gmail-plaintext__create_draft`
  - `mcp__ponymail__search_list`
  - `mcp__ponymail__auth_status`
  - `mcp__ponymail__get_thread`
  - `mcp__ponymail__get_email`
  - `mcp__ponymail__list_restrictions`
  - `mcp__apache-projects__project_stats`
  - `mcp__apache-projects__get_committee`
  - `mcp__apache-projects__get_group_members`
  - `mcp__apache-projects__get_person`
  - `mcp__apache-projects__search_people`
  - `Bash(vulnogram-api-record-fetch *)`

  (The `mcp__apache-projects__*` read tools back the roster /
  affiliation lookups — also used by `contributor-nomination`,
  the maintainer-side <governance-body>/committer assessment skill. Both MCP
  servers are installed from the latest `main` of `apache/comdev`;
  see [`tools/apache-projects/tool.md`](../../tools/apache-projects/tool.md)
  and [`tools/ponymail/tool.md`](../../tools/ponymail/tool.md).)

- **Any family that ships docs / markdown** (effectively
  every adopter, since the framework itself ships docs) —
  - `Bash(lychee *)` — read-only link-checker invoked by
    the *"run lychee before pushing a PR"* hygiene gate
    documented in [`AGENTS.md`](../../AGENTS.md).

The recommended list is **deliberately narrow** — every
entry is read-only, scoped to a specific tool, and
verified against Claude Code's auto-allowed harness
exclusions (`READONLY_COMMANDS`, `GIT_READ_ONLY_COMMANDS`,
`GH_READ_ONLY_COMMANDS`, etc.) so the framework does not
redundantly propose entries that never prompt anyway.

**Implementation.** The classification logic and the atomic
edit path are factored out into the
[`tools/permission-audit`](../../tools/permission-audit/README.md)
CLI; the canonical forbidden + recommended-by-family lists
live in
[`tools/permission-audit/src/permission_audit/audit.py`](../../tools/permission-audit/src/permission_audit/audit.py).
The skill invokes the CLI once per settings file:

```bash
uv run --project <framework>/tools/permission-audit \
  permission-audit audit <repo>/.claude/settings.local.json \
  --families <comma-joined families from the lock>
```

The CLI emits structured JSON the skill folds into the verify
report. Exit code `1` from the CLI maps to ✗ on this check.

**Reporting shape:** group findings by file, then by bucket.
For each forbidden entry, print the exact JSON-pointer-style
path (`.permissions.allow[<index>]`) the CLI returned so the
operator can locate it instantly; for each recommended entry
missing, print the suggested string verbatim ready for paste.
**Do not auto-write the files** — the per-machine
`settings.local.json` is the operator's; surface the proposal
and let `/magpie-setup verify --apply-permission-audit`
(interactive) or a hand-edit close the gap. The apply path
calls

```bash
uv run --project <framework>/tools/permission-audit \
  permission-audit apply <repo>/.claude/settings.local.json \
  --add '<entry>' --remove '<entry>' ...
```

which holds a POSIX `fcntl.flock` advisory exclusive lock on
the target file, re-parses under the lock, mutates
`.permissions.allow[]` in place, writes to a sibling temp
file, and `os.replace`s into place — so concurrent
`/magpie-setup-isolated-setup-install` (which also writes to the same
file's `sandbox.filesystem.*` arrays) does not silently
clobber the diff. When the target file lives at a path the
agent's sandbox marks as `denyWithinAllow` (the per-machine
settings files typically are), the apply path requires the
operator to authorise the sandbox bypass for that single write
— it does not silently skip the file. ⚠ if either file is
absent (most adopters will have at least
`settings.local.json` after the first
`/magpie-setup-isolated-setup-install` pass; absence is a soft signal
not a hard fault).

**Why we propose, never auto-apply.** The allow-list is
the operator's *capability surface* for the agent in this
checkout. Even an objectively-safer edit (drop a
known-dangerous wildcard) is a capability change the
operator must own, both to know it happened and to keep
the audit trail human-readable. The framework's job is to
*surface* the gap — the operator's job is to close it.

### 8e. comdev MCP prerequisites (ASF projects)

**Run this check only for ASF projects** — detect ASF the same way
as [`adopt.md` Step 9c](adopt.md#step-9c--comdev-mcp-prerequisites-asf-projects):
`<project-config>/project.md` declares `project_metadata.mandatory:
true` or `Mail sources` `ponymail` `mandatory: yes`. Skip otherwise
(the two MCP servers are optional for non-ASF adopters).

For ASF projects, both the
[PonyMail](../../tools/ponymail/tool.md) and
[Apache Projects](../../tools/apache-projects/tool.md) MCP
servers are mandatory pre-flight prerequisites, installed from the
latest `main` of `apache/comdev` (tracked, not pinned). Confirm:

1. **Registered.** `mcp__ponymail__*` and `mcp__apache-projects__*`
   appear in the session tool list. ✗ on either missing — the
   mandatory pre-flight gates in `security-issue-import` /
   `security-issue-sync` (PonyMail) and `contributor-nomination`
   (Apache Projects) will hard-stop. Remediation:
   [`adopt.md` Step 9c](adopt.md#step-9c--comdev-mcp-prerequisites-asf-projects).
2. **PonyMail authenticated.** For ASF projects an authenticated
   LDAP session is required, not just a registered server — a
   trivial `mcp__ponymail__auth_status()` should report an
   authenticated session. ⚠ if registered but unauthenticated
   (remediation: `mcp__ponymail__login()`).
3. **Checkout on `main`, current.** Resolve each server's checkout
   root from its `mcpServers` `args` path and confirm `origin` is
   `apache/comdev`, the branch is `main`, and it is not behind the
   last-fetched `origin/main`. This is the read-only, offline form
   of the freshness assertion; the authoritative live fetch belongs
   to [`/magpie-setup upgrade` Step 6e](upgrade.md#step-6e--refresh-comdev-mcp-checkouts-asf-projects)
   and [`setup-isolated-setup-update`](../setup-isolated-setup-update/SKILL.md).
   ✗ off-`main` or non-`apache/comdev` remote; ⚠ behind
   `origin/main`.

### 9. Project documentation mentions the framework

Two files to check (per
[`adopt.md` Step 11](adopt.md#step-11--project-doc-updates-fresh-only)):

- **`<repo-root>/README.md`** — should have a contributor-facing
  section (typically `## Agent-assisted contribution
  (apache-magpie)`) that mentions the snapshot mechanism, the
  `/magpie-setup` invocation for fresh clones, the
  `.apache-magpie.lock` pin, and `.apache-magpie-overrides/`.
  Grep for `apache-magpie` and `/magpie-setup` together as a
  proxy. ⚠ if either token is absent.
- **`<repo-root>/AGENTS.md`** — if the file exists, it should
  have an `## apache-magpie framework` section that
  cross-references the README section. Grep for
  `apache-magpie` and a link to the README anchor. ⚠ if the
  file exists but lacks the section; not applicable if the
  file does not exist (do not create one just to satisfy
  the check).

Cheap to skip if both are absent on a minimal repo — surface
as ⚠ overall only, never ✗. `CONTRIBUTING.md` counts as a
fallback for `README.md` if the adopter declared it so during
adoption.

### 10. Trusted external source snapshots + symlinks

Only when `<project-config>/skill-sources.md` (the trust list)
lists at least one source — otherwise skip this check silently
(the adopter runs in-tree skills only). For each trusted source
([`skill-sources.md`](skill-sources.md)):

- **Committed pin present.** The source has a block in
  `.apache-magpie.sources.lock` (`method`/`url`/`ref` + anchor).
  Missing ⇒ ✗: the trust list vouches for a source that was never
  pinned — run `/magpie-setup skill-sources`.
- **Snapshot present.** `.apache-magpie-sources/<id>/` exists on
  disk with the source's `skills_root`. Missing ⇒ ✗ with the
  remediation `/magpie-setup skill-sources` (the fetch is
  gitignored, so a fresh clone has none — expected, same as the
  framework snapshot).
- **Source drift.** The source's committed block vs its
  `.apache-magpie.sources.local.lock` block — a mismatch ⇒ ⚠ and
  proposes `/magpie-setup upgrade`, exactly like framework drift
  (check 3).
- **Symlinks live.** Every `magpie-<name>` the source `provides`
  resolves through the canonical
  `.agents/skills/magpie-<name>` → `../../.apache-magpie-sources/<id>/skills/<name>/`
  and its relays (same rule as check 5). Dangling / misdirected ⇒
  ✗ with `/magpie-setup verify --auto-fix-symlinks`.
- **No name collision.** No `magpie-<name>` provided by a source
  shadows a framework skill or another source's skill. Collision
  ⇒ ✗ (surface, do not auto-resolve).

## After the report

If every check is ✓ (or ⚠ on items the adopter has
intentionally opted out of), say so explicitly and stop.

If anything is ✗, end the report with a concrete next-step
list, ordered most → least urgent:

- ✗ on check 1 → `/magpie-setup upgrade` (re-fetches per
  the committed lock).
- ✗ on check 3 (drift) → `/magpie-setup upgrade`.
- ✗ on check 5 (dangling symlinks) →
  `/magpie-setup verify --auto-fix-symlinks` (cheap;
  no-op when symlinks already correct).
- ✗ on check 6 → `/magpie-setup adopt` (idempotent
  re-create).
- ✗ on check 4 / SHA-512 mismatch → **investigate first**;
  do not run upgrade until you understand why the
  released zip changed under the same version.
- ⚠ on check 8c (stale agent-worktree, no uncommitted
  changes) → `git worktree remove <path>` per the
  per-worktree proposal in the report. Idempotent; safe to
  batch across all flagged worktrees in one pass.
- ✗ on check 8c (stale agent-worktree, **uncommitted
  changes present**) → operator decision required. The
  proposal lists each affected worktree with its branch
  + diff summary; recover via `EnterWorktree(path)` (or
  `cd <path>` outside the harness) to inspect, then either
  commit / push or stash, then `git worktree remove --force
  <path>`. Never propose `--force` without first
  surfacing the diff.
- ✗ on check 8d (forbidden allow-list entry — arbitrary
  code execution) → propose removing the named entry from
  the file's `permissions.allow[]` array. Print the JSON-
  pointer path so the operator can locate it. Per-machine
  `settings.local.json` writes go via
  `/magpie-setup verify --apply-permission-audit`
  (interactive, atomic JSON edit, sandbox-bypass requires
  per-write authorisation). Committed `settings.json`
  writes are a regular file edit + commit; flag them
  loudly because they bind every developer on the project.
- ⚠ on check 8d (recommended allow-list entry missing) →
  optional. Print the suggested string ready for paste;
  apply via the same `--apply-permission-audit` flag, or
  paste manually. The recommendation is family-scoped, so
  an adopter who skipped the `security` family will not
  see the Gmail / PonyMail entries surfaced as gaps.
- ✗ on check 8e (ASF project, comdev MCP not registered or
  off-`main`) → `/magpie-setup adopt` Step 9c to (re-)install
  from latest `apache/comdev` `main`. ⚠ on check 8e (PonyMail
  unauthenticated, or checkout behind `origin/main`) →
  `mcp__ponymail__login()` and/or `/magpie-setup upgrade`
  Step 6e (live fetch + `git pull --ff-only`).
- All other ✗ / ⚠ → name the gap, give the one-line
  remediation.
