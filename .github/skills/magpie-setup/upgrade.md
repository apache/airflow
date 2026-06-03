 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# upgrade — refresh the gitignored snapshot per the committed lock

The upgrade flow is **drift-driven**. It detects mismatch
between `<committed-lock>` (project pin) and `<local-lock>`
(per-machine fetch), then re-installs per the committed lock,
refreshes symlinks, and reconciles overrides.

Two trigger paths land here:

1. **User-initiated** — explicit `/magpie-setup upgrade`,
   e.g. after a colleague bumped `<committed-lock>` to a
   new framework version and the user wants to align.
2. **Drift-triggered from a framework skill** — any
   framework skill (or `/magpie-setup verify`) detected
   drift on its pre-flight check and the user accepted the
   proposal to upgrade.

Both paths run the same flow.

## Inputs

- `from:<git-ref>` / `from:<version>` — bring the snapshot
  to a specific framework ref **for this run only**. Does
  NOT update `<committed-lock>` (use a project-level commit
  for that). Useful for testing a candidate version before
  pinning it.
- `bump-committed` — also update `<committed-lock>` to the
  new ref. Use when this run is the project-level decision
  to move to a newer version (the diff lands in the user's
  next commit).
- `dry-run` — show what would change without modifying
  anything.

## Step 0 — Pre-flight

1. **Confirm we are in the main checkout, not a git worktree.**
   Compare `git rev-parse --git-dir` against
   `git rev-parse --git-common-dir`. If different, stop with:

   > *"`upgrade` runs in the main checkout, not a worktree.
   > From the main: `cd <main-path> && /magpie-setup upgrade`.
   > Every worktree automatically picks up the refreshed
   > snapshot once the main upgrade lands, because each
   > worktree's `<snapshot-dir>` is a symlink to the main's
   > (per [`worktree-init.md`](worktree-init.md))."*

   `<main-path>` resolves to
   `$(dirname "$(cd "$(git rev-parse --git-common-dir)" && pwd)")` —
   surface it explicitly so the operator can `cd` there.
2. Read `<committed-lock>`. If missing, the repo isn't
   adopted — suggest `/magpie-setup adopt` and stop.
3. Read `<local-lock>`. If missing (gitignored, fresh
   clone), the local install hasn't been initialised yet —
   route as a recover-snapshot install per the committed
   lock, not as an upgrade. Continue at Step 3.

## Step 0a — Pre-Magpie leftovers safety check

A repo that adopted the framework before it was renamed from
**apache-steward** to **Apache Magpie** migrates via the
one-shot transition shim at `.claude/skills/setup-steward/`
(a frozen `/setup-steward upgrade` lands there automatically;
see that skill's [`upgrade.md`](../../.claude/skills/setup-steward/upgrade.md)).
A fully-migrated repo never reaches *this* file with legacy
artefacts present.

If you nonetheless detect **any** legacy artefact here —
`.apache-steward.lock`, `.apache-steward/`,
`.apache-steward-overrides/`, a committed
`<adopter-skills-dir>/setup-steward/`, or a framework symlink
**without** the `magpie-` prefix — a prior migration did not
finish. Do **not** continue the normal upgrade against the
half-migrated state. Run the transition migration to
completion first (follow
[`.claude/skills/setup-steward/upgrade.md`](../../.claude/skills/setup-steward/upgrade.md),
which is idempotent and safe to re-run), then resume this
upgrade. `~/.config/apache-steward/` alone (per-user, no
in-repo artefacts) just needs the dir + sandbox-allowlist
move from that file's Step 7.

## Step 1 — Compute drift

Compare `<committed-lock>` to `<local-lock>` and to upstream
where applicable:

| Method | Drift signal |
|---|---|
| `git-branch` | `<local-lock>.fetched_commit` vs upstream's current tip of `<committed-lock>.ref` (the branch). Drift if upstream has commits the local snapshot does not. |
| `git-tag` | `<committed-lock>.ref` vs `<local-lock>.source_ref`. Drift if they differ. |
| `svn-zip` | `<committed-lock>.ref` (version) vs `<local-lock>.source_ref`. Drift if they differ. Also: if `sha512` differs, surface as a security-flagged drift (the released zip changed under the same version — investigate). |

Also check method change: if
`<committed-lock>.method != <local-lock>.source_method`, the
project switched install methods — surface as a drift that
needs a full re-install.

If no drift detected, surface and stop — the local snapshot
matches the committed pin, no upgrade needed.

## Step 2 — Surface what changed

For each kind of drift, present:

- **Commits between `<local>` and `<committed>`** — for
  `git-branch` and `git-tag` methods, list the commit log
  (`git log --oneline <local-commit>..<committed-commit>`)
  via the GitHub API or by re-cloning to a temp dir.
- **Files touched in the framework's `.claude/skills/`** —
  grouped by skill family. Call out any change to a skill
  the adopter has an override for (the override will need
  reconciliation in Step 5).
- **`setup` skill changed in the framework** —
  surface as a separate note. The adopter's *committed*
  copy of `setup` will be auto-overwritten from the
  new snapshot in [Step 4b](#step-4b--overwrite-the-committed-setup-from-the-new-snapshot--reload-in-flight)
  and then the skill **reloads in-flight** before the rest
  of the upgrade runs, so the bootstrap stays in sync with
  the framework version the project just pinned and the
  remaining steps execute against the new content.

Ask for explicit confirmation before deleting and re-
installing.

## Step 3 — Delete the old snapshot

```bash
rm -rf .apache-magpie
```

The snapshot is gitignored — destroying it loses no
committed work. Do this **before** the new install to avoid
"new layered on top of old" partial state.

## Step 4 — Install per the committed lock

Per `<committed-lock>.method`:

- **`git-branch`** — `git clone --depth=1 --branch
  <committed.ref> <committed.url> .apache-magpie`. If
  `from:<git-ref>` was passed, use that branch instead of
  the committed one (this run only).
- **`git-tag`** — `git clone --depth=1 --branch
  <committed.ref> <committed.url> .apache-magpie`. If
  `from:<git-ref>` overrides, use it.
- **`svn-zip`** — `curl` the zip + verify `sha512` +
  `unzip` to `.apache-magpie/`. The verification step is
  **mandatory**; mismatched SHA-512 stops the upgrade and
  surfaces the discrepancy.

After install, capture the actual on-disk state for the
new `<local-lock>`:

- `source_method`, `source_url`, `source_ref` — from
  whatever method was used in this run (committed lock or
  `from:` override).
- `fetched_commit` — `git -C .apache-magpie rev-parse
  HEAD` for git methods; the version for svn-zip.
- `fetched_at` — current ISO-8601 timestamp.

## Step 4b — Overwrite the committed `setup` from the new snapshot + reload in-flight

This step **must run before Steps 5+** so the remainder of
this upgrade executes against the framework version the
project just pinned to, not against the pre-upgrade
bootstrap logic. It implements
[`SKILL.md` Golden rule 9](SKILL.md#golden-rules).

1. Compute the diff between the adopter-side
   `<adopter-skills-dir>/magpie-setup/` (committed copy)
   and the snapshot's
   `.apache-magpie/skills/setup/`.
2. **If the adopter has local modifications** to their
   committed copy beyond what the snapshot ships — surface
   the diff and stop. Do **not** silently overwrite local
   work. The user either (a) confirms the modifications can
   be discarded, (b) decides to upstream them as a PR
   against `apache/airflow-steward` first, or (c) defers
   the bootstrap-skill update to a later upgrade run.
3. **If there are no local modifications** (or the user
   confirmed in 2), copy the snapshot's `setup`
   over the committed copy:

   ```bash
   # For the flat layout (Pattern A):
   rm -rf .claude/skills/magpie-setup
   cp -r .apache-magpie/skills/setup \
         .claude/skills/magpie-setup

   # For the double-symlinked layout (Pattern B):
   rm -rf .github/skills/magpie-setup
   cp -r .apache-magpie/skills/setup \
         .github/skills/magpie-setup
   # The .claude/skills/magpie-setup per-skill symlink does
   # not need touching — it points at .github/skills/magpie-setup
   # which is now the new content.

   # For the single directory-symlink layout (Pattern D),
   # write to the *canonical* side only. With D.1
   # (canonical = .github/skills/):
   rm -rf .github/skills/magpie-setup
   cp -r .apache-magpie/skills/setup \
         .github/skills/magpie-setup
   # With D.2 (canonical = .claude/skills/), write to
   # .claude/skills/magpie-setup instead. Either way: the
   # symlinked side resolves to the refreshed content
   # automatically — nothing to touch there.
   ```

4. **Reload in-flight.** Immediately after the copy lands —
   before doing anything else in this run — re-read the
   updated `<adopter-skills-dir>/magpie-setup/SKILL.md`,
   the just-overwritten `upgrade.md` (this file), and any
   helper file you have already opened in this run
   (`conventions.md`, `overrides.md`, `verify.md`). Resume
   the upgrade from the step *after* this one, executing
   the reloaded content — not the version of this file
   that was in memory when the upgrade started.

5. The new bootstrap-skill content lands as **modified files
   in `git status`** at the adopter's committed-skill path.
   The user reviews the diff and commits it as part of the
   upgrade PR; on merge, every other contributor's next
   `/magpie-setup` run loads the matching version.

The adopter shouldn't modify the bootstrap copy locally —
the framework's hard rule is *"local mods go in
`.apache-magpie-overrides/`, framework changes go via PR
to `apache/airflow-steward`"*. But if they did, step (2)
catches it before the overwrite would erase their work.

## Step 5 — Reconcile overrides

For each file in `<repo-root>/.apache-magpie-overrides/`:

1. **Target skill check** — does the named framework skill
   exist in the new snapshot? If not (skill renamed or
   removed):
   - Surface as conflict.
   - The user updates the override's target skill name OR
     deletes the override.
2. **Anchor check** — if the override references framework
   structure (step numbers, golden rules, decision-table
   rows) that has changed in the new framework version:
   - Surface as conflict, with the specific anchors that
     have moved.
   - The user re-anchors the override against the new
     structure.

The skill **does not** auto-rewrite overrides. Agentic
interpretation means the right call is human judgement, not
pattern-matching.

## Step 6 — Refresh framework-skill symlinks

Read the opt-in skill families from `<committed-lock>`
(falling back to `<local-lock>` if the committed lock is
silent on families). Compose the **effective family set**
for this upgrade as:

- **Opt-in families** the project recorded (`security`,
  `pr-management`, `issue`, or any combination).
- **Newly-introduced opt-in families** — families the
  framework now ships that did not exist when the lock was
  written. Detect by enumerating the prefixes of opt-in
  families in the snapshot (`security-*`, `pr-management-*`,
  `issue-*`) and comparing against the lock's recorded set.
  Any family present in the snapshot but absent from the
  lock is auto-added to the effective set on this run, and
  the addition is **written back to `<committed-lock>`**
  (same fields as
  [`adopt.md` Step 4](adopt.md#step-4--write-committed-lock-fresh-only)).
  Surface the added family in the upgrade summary so the
  operator sees it; do not prompt — per the framework's
  policy each opt-in family is maintainer-grade and an
  adopter that has already adopted the framework is in scope
  for any opt-in family the framework grows.
- **Always-on families** (always added — never read from
  the lock, never user-configurable, per
  [`SKILL.md` Golden rule 8](SKILL.md#golden-rules)):
  - every `setup-*` skill in the new snapshot *except*
    `setup` itself, and
  - every `list-*` skill in the new snapshot.

Compute the always-on set fresh from the snapshot contents
on disk — it expands automatically when the framework adds
a new `setup-*` or `list-*` skill in a release, and
contracts on a rename / removal without code changes here.

Before creating symlinks for a newly-introduced opt-in
family, reconcile the adopter's `.gitignore` so the new
family's snapshot symlinks are gitignored. Append the
`.gitignore` lines from
[`adopt.md` Step 7](adopt.md#step-7--gitignore-entries-fresh-only)
for the new family's prefix, matching the adopter's
[skills-dir convention](conventions.md):

- Pattern A — `/.claude/skills/<prefix>-*` only.
- Pattern B — both `/.claude/skills/<prefix>-*` and
  `/.github/skills/<prefix>-*` (two physical symlinks per
  skill).
- Pattern D — only the *canonical-side* `<canonical>/<prefix>-*`
  ignore line. D.1 → `/.github/skills/<prefix>-*`; D.2 →
  `/.claude/skills/<prefix>-*`. The symlinked side's
  directory symlink does not need its own ignore line — git
  does not descend into it.

The append is idempotent — skip lines that already exist.
The same idempotence covers adopters whose `.gitignore`
already had the entries (e.g. from a manually-edited block
or a previous adopt run).

The post-upgrade state must be: *every framework skill in
the new snapshot that belongs to the effective family set
has a valid symlink in `<adopter-skills-dir>`*, and *no
symlink points at a framework skill that no longer exists
in the snapshot*.

Run two passes:

1. **Ensure every family-member skill is linked.** For each
   framework skill in the new snapshot that belongs to the
   effective family set, check
   `<adopter-skills-dir>/magpie-<skill>`:
   - If the symlink exists and points at the matching
     snapshot path, leave it alone.
   - If it's missing, create it.
   - If it exists but is broken (target gone, points at the
     wrong path), repair it.

   Do this unconditionally — do not skip skills whose
   symlinks "should" already be there. A contributor who
   ran `git clean -fdx`, blew away `<adopter-skills-dir>` by
   accident, or merged a branch that removed the symlinks
   gets the full set restored without per-symlink re-
   prompting. The aggregated list of created / repaired
   links is reported in the upgrade summary (Step 8 output
   block, under the `+` and `↻` rows).

2. **Reconcile stale symlinks.** Walk
   `<adopter-skills-dir>` looking for symlinks that point
   at framework skills no longer in the new snapshot
   (rename, removal). For each:
   - If renamed (the framework documented a rename in its
     release notes), offer to re-symlink to the new name.
   - If removed, offer to remove the stale symlink.

Per-pattern symlink layers to refresh:

- **Pattern A (flat)** — refresh the single layer at
  `.claude/skills/magpie-<n>`.
- **Pattern B (double-symlinked)** — refresh both layers
  (inner at `.github/skills/magpie-<n>`, outer at
  `.claude/skills/magpie-<n>` → inner).
- **Pattern D (single directory symlink)** — refresh only
  the *canonical-side* layer at
  `<canonical-side>/magpie-<n>` (D.1 → `.github/skills/magpie-<n>`;
  D.2 → `.claude/skills/magpie-<n>`). The symlinked-side path
  resolves through the directory symlink and needs no
  per-skill plumbing.

## Step 6b — Sync locally-installed hooks and configuration

The framework ships hooks and config files an adopter
*carries locally* (in the working tree or under `.git/`)
rather than pulls in via symlink. Examples:

- `<repo-root>/.git/hooks/post-checkout` (the worktree-aware
  hook installed during adoption).
- Any future hook or local config the framework adds.

These can drift independently of the snapshot — an
adopter who never re-runs `/magpie-setup` after a
framework upgrade keeps the old hook content even after the
snapshot updates. This step closes that gap.

For each hook / local config file the framework declares as
"adopter-installed":

1. Compute the snapshot's *expected* content for that file
   (the framework ships the expected content under
   `.apache-magpie/skills/setup/` or a
   sibling location — locate the canonical source for each
   file).
2. Compare against the local copy.
3. If unchanged — ✓, move on.
4. If drifted and the diff is consistent with a stock
   framework refresh (no operator hand-edits) — overwrite
   silently.
5. If the local copy looks hand-edited — surface the diff,
   ask the user whether to overwrite, keep, or move-aside.

Run this sync unconditionally on every upgrade and every
adopt run, regardless of whether the snapshot changed. It
catches the "local config drifted while the snapshot didn't"
case (e.g. a contributor accidentally edited
`.git/hooks/post-checkout`).

## Step 6c — Propagate to every worktree (run `worktree-init` unconditionally)

The main checkout drives the upgrade, but each worktree
carries its own gitignored `<adopter-skills-dir>` symlinks.
Those symlinks need refreshing too — otherwise a developer
sitting in a worktree sees the new snapshot via the shared
`<snapshot-dir>` symlink (per
[`worktree-init.md`](worktree-init.md)) but their
`<adopter-skills-dir>` may still point at *missing* skills
(a family the upgrade added) or *renamed* ones (a framework
rename).

`worktree-init` is **always run on every worktree** at the
end of an upgrade, even when the user did not ask for it,
even when the worktree looks "already wired", because
`worktree-init` is idempotent (a no-op when state is
correct) and the cost of running it unnecessarily is
trivially small. Conversely, *not* running it leaves worktree
state inconsistent with the new framework version. The
post-checkout hook covers the "next checkout" case, but
upgrade re-aligns the existing worktrees **now**.

Procedure:

1. Enumerate worktrees with `git worktree list --porcelain`.
   Filter to the linked worktrees only — skip the main
   checkout (already handled above) and any bare worktrees.
2. For each linked worktree, invoke
   `/magpie-setup worktree-init` with that worktree's
   working directory as the `cwd`. The sub-action picks up
   the family set from `<main>/.apache-magpie.lock` (the
   committed lock the worktree shares via git) plus the
   always-on families per
   [`SKILL.md` Golden rule 8](SKILL.md#golden-rules), and
   reconciles both the snapshot symlink and the
   `<adopter-skills-dir>` symlinks (see
   [`worktree-init.md` Step 1 + Step 1b](worktree-init.md)).
3. Collect each invocation's recap into a per-worktree row
   for the upgrade summary's `Worktrees:` section
   (Step 8 output block).

**Failure handling per worktree:**

- If a worktree is on a branch that does not carry the
  adopter's committed `setup` skill (e.g. a feature
  branch from before adoption landed), the worktree-init
  invocation refuses with "main checkout not adopted from
  this branch's perspective". Surface as a ⚠ row in the
  summary and continue with the next worktree — the user
  resolves it later by merging the adoption commit forward.
- If a worktree has a hand-maintained `<snapshot-dir>` that
  is **not** a symlink to the main's, the move-aside flow
  in [`worktree-init.md` Step 0 row 4](worktree-init.md)
  asks for confirmation. Surface to the user; either
  confirm and continue, or defer that worktree and move on.

Do **not** abort the whole upgrade because one worktree
failed — the main is already upgraded and the other
worktrees still benefit from the propagation. The summary
makes the skipped worktrees easy to come back to.

**After the per-worktree loop**, run the
sandbox-allowlist helper once with `--all-worktrees` to
ensure each worktree's project root is in that worktree's
own `.claude/settings.local.json` (defensive against
[issue #197](https://github.com/apache/airflow-steward/issues/197);
see
[`setup-isolated-setup-install/SKILL.md` → Step P](../setup-isolated-setup-install/SKILL.md#step-p--project-root-coverage-in-the-sandbox-allowlists)):

```bash
~/.claude/scripts/sandbox-add-project-root.sh --all-worktrees
```

**Invoke with `dangerouslyDisableSandbox: true`** — the
target settings files are in Claude Code's built-in sandbox
`denyWithinAllow` set, so a sandboxed Bash write fails with
`operation not permitted`. Surface the bypass proposal to
the operator *before* invoking — name the helper, name the
target files, and confirm. The reason for the bypass is
*"writing project-local sandbox-allowlist entries (issue
#197 fix)"*. The bypass fires `sandbox-bypass-warn.sh`'s
bold-red banner as a backstop, but the agent must propose
the bypass first; do not silently approve.

The helper enumerates `git worktree list --porcelain` and
writes each worktree's path into that worktree's own
project-local `settings.local.json` (gitignored, never the
committed project-scope file). Idempotent — already-present
paths are skipped. If
`~/.claude/scripts/sandbox-add-project-root.sh` is absent,
surface as ⚠ in the upgrade summary with a pointer at
`/setup-isolated-setup-install` and continue (do not block
upgrade — secure-agent setup is independent of framework
upgrade). The recap row in Step 8's output goes under a new
`Sandbox allowlist:` section.

## Step 6d — Audit framework template genericity

A defensive hygiene pass over the framework's
`<snapshot-dir>/projects/_template/` directory. Templates
are meant to be **project-agnostic scaffolds** that adopters
copy and customise — they should contain placeholders
(`<Project Name>`, `<github-org>/<team-slug>`, etc.),
generic examples, and no hardcoded project identity.

In practice the framework's `_template/` files have at
times been seeded from one specific adopter's data
(originally the project the framework grew out of) and
not always generalised back. This step surfaces the
residue so it can be filed as an issue against
`apache/airflow-steward` and fixed upstream.

For each file under `<snapshot-dir>/projects/_template/`,
scan for adopter-specific signals:

- **Hardcoded project names in titles or prose** — H1
  headings, doc body text, calibration sentences. A
  genuine template starts with `# TODO: <Project Name> —
  ...` or uses a `<PROJECT>` placeholder; if a concrete
  name appears there instead, that is the residue.
- **Hardcoded URLs** pointing at a specific adopter —
  `github.com/<org>/<repo>/...` paths in body text,
  mailing-list addresses tied to a specific TLD,
  project-specific chat URLs. Template URLs should be
  `<placeholder>` or annotated "Example: …".
- **Hardcoded org / team identifiers** — committer-team
  slugs (e.g. `<org>/<team>-committers`), real maintainer
  handles, project-specific issue-tracker keys. Same
  rule: placeholder, or marked as an example.
- **Project-specific calibration prose** — references
  to contributor counts, specific issue numbers,
  incident postmortems, or other particulars an
  arbitrary adopter wouldn't share.

Surface the findings as a `Framework templates:` block in
the upgrade summary (see [Step 8 output](#output-to-the-user))
— one ⚠ row per file with a short summary of what looks
adopter-specific. **Do not modify the snapshot** (it is
read-only per
[`SKILL.md` Golden rule 1](SKILL.md#golden-rules)). The
recap is purely advisory; the operator decides whether to
open a tracking issue / PR against
`apache/airflow-steward`.

The check is intentionally heuristic — false positives are
acceptable because the cost is one line in the summary, not
a blocked upgrade. False negatives are also acceptable; the
operator's read of the upgrade summary is the real signal.
**Never attempt to auto-fix.**

If every template scans clean, surface the section as
`✓ all framework templates look generic`.

## Step 6e — Refresh comdev MCP checkouts (ASF projects)

**Run this step only for ASF projects** — detect ASF the same way
as [`adopt.md` Step 9c](adopt.md#step-9c--comdev-mcp-prerequisites-asf-projects):
`<project-config>/project.md` declares `project_metadata.mandatory:
true` or `ponymail` `mandatory: yes`. Skip otherwise.

The [PonyMail](../../tools/ponymail/tool.md) and
[Apache Projects](../../tools/apache-projects/tool.md) MCP
servers are installed from a local `apache/comdev` checkout and are
**tracked at `main`, not pinned** (no tagged releases — contrast
the cooldown-pinned system tools in the secure-setup update flow).
An ASF adopter running `/magpie-setup upgrade` should refresh that
checkout in the same pass, so it does not silently rot between
framework upgrades.

For each of `ponymail` / `apache-projects` registered in the
user/project `mcpServers` config, resolve the checkout root from
its `args` path (`<comdev>/mcp/<server>/index.js`), then — **surface
only, never auto-pull** (same contract as Step 6b):

1. Confirm `origin` is an `apache/comdev` URL and the checkout is on
   `main` (`git -C <root> rev-parse --abbrev-ref HEAD`). Flag a
   detached HEAD / feature branch; remediation
   `git -C <root> checkout main`.
2. `git -C <root> fetch origin main` and report the behind-count
   (`git -C <root> rev-list --count HEAD..origin/main`). When
   behind, print — do not run:

   ```bash
   git -C <root> pull --ff-only
   ( cd <root>/mcp/<server> && npm install )
   ```

   with the `github.com/apache/comdev/compare/<sha>...main` link.

This is the adoption-flow mirror of
[`setup-isolated-setup-update`](../setup-isolated-setup-update/SKILL.md)'s
comdev-MCP check — it exists here so the prereq rides along with the
upgrade an ASF adopter actually runs. If a registered MCP is
missing entirely, point the operator at
[`adopt.md` Step 9c](adopt.md#step-9c--comdev-mcp-prerequisites-asf-projects)
to (re-)install it.

## Step 7 — Update `<local-lock>`

Write the new local lock with the values captured in Step
4.

## Step 8 — (optional) Update `<committed-lock>`

If `bump-committed` was passed, update
`<committed-lock>.ref` (and per-method extras: `commit` for
tag, `sha512` for svn-zip) to the new fetch. Surface the
diff to the user before writing.

Without `bump-committed`, the committed lock is unchanged —
this upgrade is a *local* sync to the project pin.

## Step 9 — Sanity check

Run [`verify.md`](verify.md)'s checklist as a final step.

## Output to the user

```text
Drift remediated:
  Method:    <method>
  Project:   <committed.ref>      (committed)
  Local:     <local.fetched-commit-or-version>  → <new>
  Snapshot:  refreshed at .apache-magpie/

setup (bootstrap):
  ✓ in sync   OR   ↻ overwritten from snapshot (reloaded in-flight)

Symlinks (main checkout):
  Opt-in families:     <security>, <pr-management>, <issue>   (from lock)
  Newly added opt-in:  <issue>   (introduced since lock was written; lock updated)
  Always-on families:  setup-*, list-*       (per Golden rule 8)
  ✓ <list of unchanged symlinks>
  + <list of newly-created symlinks (skill present in the
     effective family set but missing from <adopter-skills-dir>)>
  ↻ <list of repaired symlinks (existed but broken / pointing
     at the wrong path)>
  - <list of removed stale symlinks>

.gitignore reconcile:
  ✓ all opt-in family prefixes already gitignored   OR
  + <list of /.claude/skills/<prefix>-* and /.github/skills/<prefix>-*
     lines appended for newly-introduced opt-in families>

Hooks + local config:
  ✓ <list of files in sync>
  ↻ <list of files re-synced from the snapshot>
  ⚠ <list of files with hand-edits that need operator review>

Worktrees (worktree-init was run on each, idempotently):
  ✓ <worktree-path>   (snapshot symlink + family symlinks aligned)
  ↻ <worktree-path>   (refreshed by worktree-init)
  ⚠ <worktree-path>   (skipped — branch missing adopter's setup)
  - <none>            (when this repo has no linked worktrees)

Sandbox allowlist (sandbox-add-project-root.sh --all-worktrees):
  ✓ already covers this project + N worktrees   OR
  + <list of <worktree>/.claude/settings.local.json files updated>   OR
  ⚠ helper not installed — run /setup-isolated-setup-install

Overrides:
  ✓ <list of overrides whose target is unchanged>
  ⚠ <list of overrides flagged for re-anchoring> (open the
     file and update against the new framework structure)

Framework templates (projects/_template/):
  ✓ all templates look generic   OR
  ⚠ <_template/foo.md>           (e.g. H1 title hardcoded to a specific project name)
  ⚠ <_template/bar.md>           (e.g. `committers_team` set to a concrete org/team without placeholder)
  → file an issue against apache/airflow-steward to upstream a fix

Recommended follow-ups:
  - Run /setup-isolated-setup-update if the secure-setup blast
    radius (settings.json, agent-isolation/, pinned-versions.toml)
    appears in the diff.
  - Open .apache-magpie-overrides/<name>.md for any ⚠ entry above.
```

## Failure modes

- **`<committed-lock>` missing** → repo not adopted; suggest
  `/magpie-setup adopt`.
- **Network failure** → stop, surface error, user retries.
  The skill never leaves a half-deleted snapshot — Step 3's
  `rm -rf` runs only after Step 2's user confirmation.
- **SHA-512 mismatch on svn-zip** → stop. The released zip
  has changed content under the same version, which is a
  serious signal. The user investigates (re-check the
  project's announcement, re-fetch the official KEYS,
  consider whether the project re-released).
- **Conflict during reconcile** → not a failure per se. The
  skill surfaces the conflict and finishes the upgrade up
  to Step 7. The user's next step is editing the override
  file.
