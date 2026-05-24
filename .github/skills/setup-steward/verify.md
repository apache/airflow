 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# verify — health check of the steward integration + drift detection

Confirms the framework is wired in correctly so the rest of
the framework's skills resolve from the right paths, and
surfaces any **drift** between the committed lock (project
pin) and the local lock (per-machine fetch). Read-only by
default — surfaces gaps and remediation commands.

## Inputs

- `--auto-fix-symlinks` — *exception to read-only*. If the
  snapshot is present but symlinks are missing or dangling,
  recreate them. Used by the post-checkout hook
  ([`adopt.md` Step 10](adopt.md)) on a fresh worktree
  where the gitignored symlinks didn't follow the
  checkout.

## Pre-flight

1. `git rev-parse --show-toplevel` — must succeed.
2. `git remote get-url origin` — refuse if it resolves to
   `apache/airflow-steward` itself (this skill is for
   adopters, not the framework).
3. If `<repo-root>/.apache-steward.lock` is missing, the
   repo is not adopted. Surface and stop with a pointer at
   `/setup-steward adopt`.

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
  → run `/setup-steward upgrade` (it gracefully handles the
  recover-snapshot case when the committed lock exists but
  the snapshot does not).
- ✗ if missing **and we are in a worktree** (the two dirs
  differ) → run `/setup-steward worktree-init` to symlink
  `<snapshot-dir>` to the main checkout's. Do **not**
  propose `upgrade` — that creates a per-worktree snapshot,
  which is the bug `worktree-init` is designed to prevent.
- ⚠ if present as a regular directory **in a worktree** →
  legacy per-worktree snapshot. Suggest
  `/setup-steward worktree-init` (with the move-aside flow)
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

`<committed-lock>` (`.apache-steward.lock`) and
`<local-lock>` (`.apache-steward.local.lock`) both parse.

- ✗ if `<committed-lock>` is missing → not adopted;
  redirect (already caught in pre-flight).
- ⚠ if `<local-lock>` is missing or unparsable → first
  install on this machine has not run, or the file was
  truncated. Suggest `/setup-steward upgrade` to re-create
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
| Method or URL differ | ✗ — full re-install needed; remediation: `/setup-steward upgrade` |
| Ref differs (e.g. project bumped tag, or `git-branch` local is behind upstream) | ⚠ — sync needed; remediation: `/setup-steward upgrade` |
| `svn-zip` SHA-512 differs from the verification anchor in `<committed-lock>` | ✗ — security-flagged; the released zip changed content; investigate before upgrading |

### 4. `.gitignore` correctly excludes the snapshot + local lock + symlinks + project-local settings

Check that the entries from
[`adopt.md` Step 7](adopt.md) are present in
`<repo-root>/.gitignore`. Required:

- `/.apache-steward/` (snapshot path)
- `/.apache-steward.local.lock` (per-machine state)
- `/.claude/settings.local.json` (per-machine project-scope
  settings — written to by
  [`sandbox-add-project-root.sh`](../../../tools/agent-isolation/sandbox-add-project-root.sh)
  as the per-worktree sandbox-allowlist defense for
  [issue #197](https://github.com/apache/airflow-steward/issues/197);
  must never be committed since the content is machine-specific
  absolute paths)

Recommended (the family patterns the adopter's
[skills-dir convention](conventions.md) requires):

- **Pattern A** — framework-skill symlink patterns
  (`security-*`, `pr-management-*`, `issue-*`,
  `setup-isolated-setup-*`, `setup-shared-config-sync`,
  `list-steward-*`) under `.claude/skills/` only.
- **Pattern B** — same patterns under **both**
  `.claude/skills/` and `.github/skills/` (one ignore line
  per physical symlink).
- **Pattern D** — same patterns under the **canonical side
  only** (`.github/skills/` for D.1; `.claude/skills/` for
  D.2). The symlinked side does not need its own ignore
  lines because git does not descend into a directory
  symlink.

- ✗ if `/.apache-steward/` is not gitignored — the snapshot
  is at risk of being accidentally committed.
- ✗ if `/.apache-steward.local.lock` is not gitignored —
  per-machine state would leak into the repo.
- ✗ if `/.claude/settings.local.json` is not gitignored —
  per-machine absolute paths would leak into the repo; the
  sandbox-allowlist helper refuses to write to a non-ignored
  target as defense in depth, but `verify` surfaces the
  underlying `.gitignore` gap so the operator fixes the root
  cause.
- ⚠ if symlink patterns are not gitignored.

### 5. Symlinks point at live framework skills

For each symlink under `<adopter-skills-dir>` that resolves
into `.apache-steward/.claude/skills/<name>/`:

- ✓ if the target exists.
- ✗ if dangling (target deleted or snapshot missing).
  Remediation: `/setup-steward adopt` (idempotent re-run)
  or this same skill with `--auto-fix-symlinks`.

For each framework skill in the snapshot **not** symlinked
in the adopter, classify it:

- **Always-on family** (every `setup-*` *except*
  `setup-steward` itself, and every `list-steward-*` — per
  [`SKILL.md` Golden rule 8](SKILL.md#golden-rules)) →
  surface as ✗. These families are not opt-in; missing
  symlinks here indicate a broken install or a skipped
  upgrade pass. Remediation:
  `/setup-steward verify --auto-fix-symlinks` (cheap), or
  `/setup-steward upgrade` (covers the family-wide pass).
- **Opt-in family the project picked** (per
  `<committed-lock>` / `<local-lock>`) → surface as ✗. The
  project declared the family but the install is missing a
  skill from it. Remediation as above.
- **Opt-in family the project did NOT pick** → surface as
  ⚠. The user may have intentionally not picked that
  family; the warning prompts a decision.

The `--auto-fix-symlinks` path repairs the first two
classes in place without prompting; the ⚠ class needs an
explicit `/setup-steward adopt` re-run with the family
added to the pick.

### 6. `.apache-steward-overrides/` exists + has the README

`<repo-root>/.apache-steward-overrides/` is a directory
with the `README.md` scaffold from
[`adopt.md` Step 9](adopt.md).

- ✗ if missing → `/setup-steward adopt` (idempotently
  re-creates).
- ⚠ if present but `README.md` is missing — the directory
  may have been hand-created. Suggest re-running
  `/setup-steward adopt`.

### 7. The `setup-steward` skill itself is up to date

Compare the adopter-side committed `setup-steward` skill
(at `<adopter-skills-dir>/setup-steward/`) against the
snapshot's `.apache-steward/.claude/skills/setup-steward/`.

- ✓ if same content.
- ⚠ if different — the adopter's committed copy has
  drifted from the snapshot. The remediation depends on
  *which way* the drift goes:

  - **Snapshot is newer than the committed copy** (typical
    case after a framework upgrade where the adopter has
    not yet rerun `/setup-steward upgrade`). Run
    `/setup-steward upgrade` — its
    [Step 4b](upgrade.md#step-4b--overwrite-the-committed-setup-steward-from-the-new-snapshot--reload-in-flight)
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
    a PR against `apache/airflow-steward`; the local fix
    is to revert the modifications and use
    `.apache-steward-overrides/` instead.

### 8. Post-checkout hook installed *and content matches the framework's expected*

Two sub-checks on `<repo-root>/.git/hooks/post-checkout`:

1. **Presence + executable.** File exists, is executable,
   and contains the
   `/setup-steward verify --auto-fix-symlinks` recipe.
   - ⚠ if missing — strictly optional, but worktrees off
     this repo will need a manual
     `/setup-steward verify --auto-fix-symlinks` after
     checkout. Print the install recipe.

2. **Content drift vs the framework's expected.** Diff the
   installed hook against the framework's expected hook
   content (the canonical source is shipped under the
   snapshot — locate it during the check). Same logic
   applies for any other adopter-installed local hook or
   config file the framework grows in future.
   - ✓ if content matches.
   - ⚠ if drifted and the diff looks like operator
     hand-edits — surface the diff; remediation is to run
     `/setup-steward` (adopt or upgrade), whose
     hook+config-sync pass re-installs from the snapshot
     after asking about hand-edits.
   - ✗ if drifted and the installed content is clearly
     stale (older framework version's recipe) — same
     remediation, no operator prompt needed; the sync
     pass overwrites silently.

### 8b. Sandbox-allowlist coverage of the current worktree

Defensive cross-check for
[issue #197](https://github.com/apache/airflow-steward/issues/197):
`sandbox.filesystem.allowRead: ["."]` does not in practice cover
CWD under the harness, so `/setup-steward` (adopt, upgrade,
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
  re-run `/setup-steward` (adopt/upgrade) which chains into
  the helper as part of its Step 12 / Step 6c sandbox-allowlist
  pass.
- ⚠ if missing from either array **and** the helper script is
  absent — the operator has not run
  `/setup-isolated-setup-install` yet. Suggest that skill.
  Not ✗ because secure-agent isolation is independent of
  framework adoption, and an adopter who runs without the
  sandbox enabled has nothing to lose by the missing entry.
- ⚠ if `<worktree>/.claude/settings.local.json` is absent
  entirely — same remediation (re-run the helper or
  `/setup-isolated-setup-install`). The file is auto-created
  by the helper on first run.
- ✗ if `<worktree>/.claude/settings.local.json` exists AND
  is **not** gitignored (cross-check via `git check-ignore`).
  Per the security rationale in
  [`docs/setup/secure-agent-setup.md` → *Security rationale — why project-local is safe to write to*](../../../docs/setup/secure-agent-setup.md#security-rationale--why-project-local-is-safe-to-write-to),
  the per-machine settings.local.json must never be committed.
  Remediation: add `/.claude/settings.local.json` to the
  adopter's `.gitignore` (also surfaced by check 4 above).

The check scopes to the current worktree only, not the full
`git worktree list`, because each worktree carries its own
project-local settings file — `/setup-steward verify` running
in worktree A has no business asserting on worktree B's file
(which it cannot even reliably read without crossing into
another working tree's path).

This check is read-only on the framework state. The defence
is layered: `/setup-steward` writes during adopt/upgrade,
`setup-isolated-setup-verify` adds a live read+write probe
(check 8 there), and this check is the cheap static cross-check
to surface drift between the two skill families.

### 9. Project documentation mentions the framework

Two files to check (per
[`adopt.md` Step 11](adopt.md#step-11--project-doc-updates-fresh-only)):

- **`<repo-root>/README.md`** — should have a contributor-facing
  section (typically `## Agent-assisted contribution
  (apache-steward)`) that mentions the snapshot mechanism, the
  `/setup-steward` invocation for fresh clones, the
  `.apache-steward.lock` pin, and `.apache-steward-overrides/`.
  Grep for `apache-steward` and `/setup-steward` together as a
  proxy. ⚠ if either token is absent.
- **`<repo-root>/AGENTS.md`** — if the file exists, it should
  have an `## apache-steward framework` section that
  cross-references the README section. Grep for
  `apache-steward` and a link to the README anchor. ⚠ if the
  file exists but lacks the section; not applicable if the
  file does not exist (do not create one just to satisfy
  the check).

Cheap to skip if both are absent on a minimal repo — surface
as ⚠ overall only, never ✗. `CONTRIBUTING.md` counts as a
fallback for `README.md` if the adopter declared it so during
adoption.

## After the report

If every check is ✓ (or ⚠ on items the adopter has
intentionally opted out of), say so explicitly and stop.

If anything is ✗, end the report with a concrete next-step
list, ordered most → least urgent:

- ✗ on check 1 → `/setup-steward upgrade` (re-fetches per
  the committed lock).
- ✗ on check 3 (drift) → `/setup-steward upgrade`.
- ✗ on check 5 (dangling symlinks) →
  `/setup-steward verify --auto-fix-symlinks` (cheap;
  no-op when symlinks already correct).
- ✗ on check 6 → `/setup-steward adopt` (idempotent
  re-create).
- ✗ on check 4 / SHA-512 mismatch → **investigate first**;
  do not run upgrade until you understand why the
  released zip changed under the same version.
- All other ✗ / ⚠ → name the gap, give the one-line
  remediation.
