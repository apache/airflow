<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

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

`<snapshot-dir>/` exists, is a directory, and contains the
expected top-level files (`README.md`, `AGENTS.md`,
`.claude/skills/`, `tools/`).

- ✗ if missing → run `/setup-steward upgrade` (it
  gracefully handles the recover-snapshot case when the
  committed lock exists but the snapshot does not).
- ✗ if missing top-level files → snapshot is corrupted;
  same remediation.

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

### 4. `.gitignore` correctly excludes the snapshot + local lock + symlinks

Check that the entries from
[`adopt.md` Step 7](adopt.md) are present in
`<repo-root>/.gitignore`. Required:

- `/.apache-steward/` (snapshot path)
- `/.apache-steward.local.lock` (per-machine state)

Recommended:

- The framework-skill symlink patterns (`security-*`,
  `pr-management-*`, `setup-isolated-setup-*`,
  `setup-shared-config-sync`) under both `.claude/skills/`
  and `.github/skills/` per convention.

- ✗ if `/.apache-steward/` is not gitignored — the snapshot
  is at risk of being accidentally committed.
- ✗ if `/.apache-steward.local.lock` is not gitignored —
  per-machine state would leak into the repo.
- ⚠ if symlink patterns are not gitignored.

### 5. Symlinks point at live framework skills

For each symlink under `<adopter-skills-dir>` that resolves
into `.apache-steward/.claude/skills/<name>/`:

- ✓ if the target exists.
- ✗ if dangling (target deleted or snapshot missing).
  Remediation: `/setup-steward adopt` (idempotent re-run)
  or this same skill with `--auto-fix-symlinks`.

For each framework skill in the snapshot **not** symlinked
in the adopter — surface as ⚠ with the family
classification. The user may have intentionally not picked
that family; the warning prompts a decision.

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
    [Step 6b](upgrade.md#step-6b--overwrite-the-committed-setup-steward-skill-from-the-new-snapshot)
    auto-overwrites the committed copy with the snapshot's
    version, surfaces local modifications first if any
    exist, and lands the change in `git status` for the
    user to commit.
  - **Committed copy is newer than the snapshot** (the
    adopter modified the bootstrap skill directly; an
    anti-pattern per the framework's hard rule). The
    framework-side fix is to upstream the modifications as
    a PR against `apache/airflow-steward`; the local fix
    is to revert the modifications and use
    `.apache-steward-overrides/` instead.

### 8. Post-checkout hook installed

`<repo-root>/.git/hooks/post-checkout` exists, is
executable, and contains the
`/setup-steward verify --auto-fix-symlinks` recipe.

- ⚠ if missing — strictly optional, but worktrees off this
  repo will need a manual
  `/setup-steward verify --auto-fix-symlinks` after
  checkout. Print the install recipe.

### 9. Project documentation mentions the framework

`<repo-root>/README.md` (or another committed doc the
adopter picked) mentions the steward adoption with a link
into the framework. Cheap to skip if absent — surface as
⚠ only.

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
