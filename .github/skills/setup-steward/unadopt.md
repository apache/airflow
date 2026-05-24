 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# unadopt — remove the apache-steward framework from an adopter repo

The reverse of [`adopt.md`](adopt.md). Removes the framework
artefacts the adopt flow installed — gitignored snapshot,
committed lock, gitignored local lock, framework-skill
symlinks, `.gitignore` entries, post-checkout hook, the
adoption sections in `README.md` / `AGENTS.md` /
`CONTRIBUTING.md`, and the committed `setup-steward` skill
itself.

By default the adopter-authored `.apache-steward-overrides/`
directory is **preserved** — it contains hand-written
customisation that is not the framework's to delete. Pass
`--purge-overrides` to remove it too.

This sub-action is destructive on disk and on the index. It
always surfaces the full removal plan first and requires one
explicit confirmation before any write.

## When to use

- The project decided to stop using apache-steward.
- The repo was adopted experimentally and the experiment is
  over.
- The adoption was scoped to a fork / branch and that branch
  is being cleaned up before merge.

If the goal is to **change install method or version**, use
[`/setup-steward upgrade`](upgrade.md) instead — that path
preserves overrides and re-uses the existing wiring.

If the goal is to **temporarily detach for debugging** (e.g.
test what a skill looks like without overrides), edit the
relevant override file rather than unadopting.

## Inputs

- `--purge-overrides` — also `git rm -r` the
  `.apache-steward-overrides/` directory. Default: preserve.
  Surfaces uncommitted edits on any override file before
  removal — the user confirms once more if any are present.
- `dry-run` — print the removal plan and stop. No writes, no
  confirmation prompt.

## Step 0 — Pre-flight

1. Confirm we are in a git repo
   (`git rev-parse --show-toplevel`).
2. **Confirm we are in the main checkout, not a git worktree.**
   Compare `git rev-parse --git-dir` against
   `git rev-parse --git-common-dir`. If different, stop with:

   > *"`unadopt` runs in the main checkout, not a worktree.
   > Unadoption removes the shared snapshot every worktree
   > points at; running from a worktree would leave the main
   > and other worktrees in a half-removed state. From the
   > main: `cd <main-path> && /setup-steward unadopt`. To
   > undo just this worktree's symlink without touching the
   > main, `rm <worktree>/.apache-steward` manually."*

3. Confirm we are **not** in `apache/airflow-steward` itself
   (`git remote get-url origin`); refuse if it resolves to the
   framework — the framework is not "adopted into" itself.
4. Confirm `<committed-lock>` (`.apache-steward.lock`) is
   present. If missing, the repo is not adopted — surface and
   stop. (If only the snapshot is present without a committed
   lock, the adopter ran the install recipe but never
   completed `/setup-steward adopt`; treat that as not-yet-
   adopted and stop with the same message.)
5. Detect the adopter's skills-dir convention per
   [`conventions.md`](conventions.md). Pin the result as
   `<adopter-skills-dir>` for the rest of this flow.

## Step 1 — Inventory what was installed

Build a concrete list of every artefact the unadopt flow
would touch. The inventory drives the plan in Step 2 and
distinguishes *present* from *absent* (skip absent items
silently — adopt is configurable, so not every adopter has
every artefact).

| Artefact | Path | Inspect |
|---|---|---|
| Snapshot | `<snapshot-dir>/` | exists + non-empty |
| Local lock | `<local-lock>` | exists |
| Committed lock | `<committed-lock>` | exists |
| `.gitignore` entries | `<repo-root>/.gitignore` | which of the entries from [`adopt.md` Step 7](adopt.md) are present |
| Framework-skill symlinks | `<adopter-skills-dir>/` — both layers under Pattern B; canonical side only under Pattern D (D.1: `.github/skills/`; D.2: `.claude/skills/`); single layer under Pattern A | each symlink whose target resolves into `<snapshot-dir>/.claude/skills/` |
| Post-checkout hook | `<repo-root>/.git/hooks/post-checkout` | exists + invokes `~/.claude/scripts/sandbox-add-project-root.sh` |
| Doc section: `README.md` | `<repo-root>/README.md` | contains the `## Agent-assisted contribution (apache-steward)` heading |
| Doc section: `AGENTS.md` | `<repo-root>/AGENTS.md` | contains the `## apache-steward framework` heading |
| Doc section: `CONTRIBUTING.md` | `<repo-root>/CONTRIBUTING.md` | contains the adoption section (fallback layout) |
| Overrides directory | `<repo-root>/.apache-steward-overrides/` | exists; count framework-scaffold files vs adopter-authored |
| `setup-steward` skill itself | `<adopter-skills-dir>/setup-steward/` | exists (this is the only committed framework skill) |

For the overrides directory: distinguish the
**framework-scaffold** files (`README.md`, `user.md` from
[`adopt.md` Step 9 / 9b](adopt.md)) from
**adopter-authored** files (e.g. `pr-management-triage.md`,
any `user.md` filled in beyond the scaffold). Also check
`git status -- .apache-steward-overrides/` for **uncommitted
edits** — those are surfaced separately in Step 2.

## Step 2 — Surface the removal plan

Render the inventory as a single table so the user can read
the blast radius at a glance:

```text
The following will be REMOVED:

  Gitignored (no commit needed):
    .apache-steward/                      (snapshot, ~N MB)
    .apache-steward.local.lock
    <adopter-skills-dir>/<symlink-1>     → .apache-steward/.claude/skills/<skill-1>/
    <adopter-skills-dir>/<symlink-2>     → ...
    .github/skills/<symlink-1>           (Pattern B only — second physical layer)
    .git/hooks/post-checkout              (if it contains the steward recipe)
    # Pattern A:  <adopter-skills-dir> = .claude/skills/
    # Pattern B:  <adopter-skills-dir> spans .claude/skills/ AND .github/skills/
    # Pattern D:  <adopter-skills-dir> = canonical side only
    #             (D.1: .github/skills/;  D.2: .claude/skills/)

  Committed (will show in `git status`):
    .apache-steward.lock                  (the project's pin)
    .gitignore                            (the entries listed in adopt.md Step 7)
    README.md                             (the `## Agent-assisted contribution (apache-steward)` section)
    AGENTS.md                             (the `## apache-steward framework` section, if present)
    <adopter-skills-dir>/setup-steward/   (this skill itself — self-destructive)

The following will be PRESERVED:

    .apache-steward-overrides/           (M file(s); pass `--purge-overrides` to remove)
    ~/.config/apache-steward/user.md     (per-user; shared with other adopters on this machine — remove manually if this was your last adoption)
```

Surface the `~/.config/apache-steward/user.md` line only if that
file is actually present on disk. If it is absent (or the
operator drove `user.md` resolution via
`$APACHE_STEWARD_USER_CONFIG` / the legacy per-project location),
omit the line. The framework never touches the per-user file
regardless of `--purge-overrides` — it is shared across every
adopter project on the operator's machine and unadopting from
*this* project does not imply they have stopped using
apache-steward elsewhere.

If `--purge-overrides` was passed, move
`.apache-steward-overrides/` into the *removed* section and
list its files explicitly so the adopter sees what custom
content is about to go.

If `git status -- .apache-steward-overrides/` showed
uncommitted edits, prepend a **warning** above the table:

```text
⚠ Uncommitted edits in .apache-steward-overrides/:
    <list of files>

  --purge-overrides would delete these along with the directory.
  Commit, stash, or copy them out before continuing.
```

If any framework-skill symlink under `<adopter-skills-dir>/`
resolves to a path **outside** `<snapshot-dir>/` — i.e. an
adopter committed a real skill at the same name post-
adoption, or a symlink points elsewhere — list it under a
separate **Preserved (not framework-owned)** subsection. The
unadopt flow never deletes content it does not own.

## Step 3 — Confirm

Ask the user to confirm the plan with a single structured
prompt (when the harness offers a structured-question tool
such as Claude Code's `AskUserQuestion`). Default selection
is **abort**, not proceed — destructive defaults bite.

Free-form chat is the fallback: paraphrase the plan, ask
"proceed with removal? (yes / no)", treat anything other
than `yes` as abort.

If `dry-run` was passed, skip the confirmation entirely and
stop after surfacing the plan.

## Step 4 — Execute removal in safe order

Run the deletions in the order below. The order matters:
artefacts that *depend* on others come out first, so a
half-completed unadopt never leaves a dangling symlink
pointing at a deleted snapshot.

1. **Framework-skill symlinks.** For each entry in the
   inventory, `rm` the symlink. Per-pattern:

   - **Pattern A** — one layer; just remove
     `.claude/skills/<n>`.
   - **Pattern B** — two layers; remove both
     `.claude/skills/<n>` and `.github/skills/<n>`.
   - **Pattern D** — one layer at the canonical side
     (D.1: `.github/skills/<n>`; D.2: `.claude/skills/<n>`).
     The directory symlink itself (`.claude/skills` or
     `.github/skills`) is **adopter-owned** and **not
     removed by unadopt** — it predates framework adoption
     and serves the adopter's own native skills too.

   Never touch a non-symlink at the same path.
2. **Post-checkout hook.** Remove only if its content matches
   the steward recipe verbatim (i.e. the hook the adopt flow
   wrote — a single
   `~/.claude/scripts/sandbox-add-project-root.sh` invocation
   guarded by the `-x` test; see
   [`adopt.md` Step 10](adopt.md#step-10--worktree-aware-post-checkout-hook-fresh-only)
   for the exact text). If the hook contains additional adopter
   logic, surface that, leave the hook in place, and tell the
   user which line to delete by hand. Hooks that still contain
   the obsolete `/setup-steward verify --auto-fix-symlinks` line
   (a Claude Code slash command that does not work from a shell
   hook — removed in a later framework release) should be
   replaced with the current Step 10 template.
3. **Snapshot directory.** `rm -rf <snapshot-dir>/`.
4. **Local lock.** `rm <local-lock>`.
5. **`.gitignore` entries.** Read `<repo-root>/.gitignore`,
   remove exactly the lines from
   [`adopt.md` Step 7](adopt.md) that are present, and leave
   any adopter-added entries (e.g. unrelated rules near the
   adoption block) untouched. Do not collapse blank lines —
   the diff stays minimal.
6. **Doc sections.** For each of `README.md`, `AGENTS.md`,
   `CONTRIBUTING.md` that contains an adoption section,
   remove the section. The boundaries are the section
   heading (e.g. `## Agent-assisted contribution (apache-
   steward)`) and the next `##`-level heading (or EOF).
   Surface the proposed diff (`git diff` form) to the user
   before writing; one batched confirmation for the whole
   doc set, not per file.
7. **Committed lock.** `git rm <committed-lock>`.
8. **Overrides directory** *(only if `--purge-overrides`)*.
   `git rm -r .apache-steward-overrides/`.
9. **`setup-steward` skill itself.**
   `git rm -r <adopter-skills-dir>/setup-steward/`. After
   this step the running skill has deleted its own committed
   source. Future invocations of `/setup-steward` will
   resolve to nothing — the adopter has to re-run the
   install recipe in
   [`docs/setup/install-recipes.md`](../../../docs/setup/install-recipes.md)
   to re-adopt.

Each step is independently surfaced as it runs (one
`✓ Removed <path>` line per artefact), so a mid-flow abort
leaves a clean record of what made it out.

## Step 5 — Sanity check

After the deletions, verify the post-state:

- `<snapshot-dir>/` does not exist.
- `<committed-lock>` and `<local-lock>` do not exist.
- No symlinks under `<adopter-skills-dir>/` resolve into
  `<snapshot-dir>/` (the path is gone, so dangling symlinks
  would surface here).
- `.gitignore` no longer contains the steward entries.
- The doc sections are gone from the affected files.
- `<adopter-skills-dir>/setup-steward/` does not exist.
- If `--purge-overrides`: `.apache-steward-overrides/` does
  not exist.
- If *not* `--purge-overrides`:
  `.apache-steward-overrides/` does exist (unchanged).

Any ✗ here is a bug — surface it and stop.

## Output to the user

A summary of what was removed + what remains:

```text
✓ Snapshot removed:        .apache-steward/
✓ Locks removed:           .apache-steward.lock, .apache-steward.local.lock
✓ Symlinks removed:        <count> (per-pattern — A: under .claude/skills/; B: under both .claude/skills/ AND .github/skills/; D: under the canonical side only)
✓ Post-checkout hook:      removed (or: preserved — contained extra adopter logic)
✓ Doc sections removed:    README.md[, AGENTS.md][, CONTRIBUTING.md]
✓ .gitignore cleaned:      <N> entries removed
✓ setup-steward skill:     removed (this skill self-destructed)

Preserved:
  .apache-steward-overrides/   (M files; pass `--purge-overrides` to remove)
  ~/.config/apache-steward/user.md   (per-user; shared with other adopters on this machine — remove manually if this was your last adoption)
  .claude/skills (or .github/skills)   (Pattern D directory symlink — adopter-owned, predates framework adoption)
  <list of any non-steward-owned content the plan flagged>

Staged for commit (you'll see in `git status`):
  D  .apache-steward.lock
  M  .gitignore
  M  README.md
  M  AGENTS.md             (if section was present)
  D  <adopter-skills-dir>/setup-steward/...

To re-adopt later: follow docs/setup/install-recipes.md in the
framework repo at https://github.com/apache/airflow-steward.
```

Suggest the user open the diff (`git diff --cached`) before
committing — the unadopt flow's `.gitignore` edit and the
`README.md` / `AGENTS.md` patches are the most likely to
need a human re-read.

## Hard rules

- **Never delete what the adopt flow did not install.**
  Symlinks pointing outside `<snapshot-dir>/`, hooks with
  custom adopter logic, `.gitignore` entries not in the
  adopt template, and content at any of the doc-section
  paths that isn't bounded by the adoption heading — all
  preserved + flagged.
- **Never silently delete `.apache-steward-overrides/`.**
  The default is preserve. `--purge-overrides` is opt-in and
  requires an additional confirmation if uncommitted edits
  are present.
- **Always surface the plan before writing.** No
  `--yes`-style auto-apply flag. Destructive defaults bite;
  the one-confirmation gate is the framework's user-trust
  invariant (see
  [Hard rules in `SKILL.md`](SKILL.md#golden-rules)).
- **Removal order is fixed.** Symlinks before snapshot,
  doc-section patches before `.gitignore` edit, committed
  lock before the `setup-steward` skill itself. The order
  guarantees no intermediate state has a dangling reference.

## Failure modes

- **`<committed-lock>` missing** → repo not adopted. Stop
  with a pointer at `/setup-steward adopt`.
- **`<snapshot-dir>/` contains committed content**
  (anti-pattern: adopter put real files inside the
  gitignored snapshot path before adoption) → surface, do
  not `rm -rf`, ask the user to relocate the content first.
- **Symlink target resolves outside `<snapshot-dir>/`** →
  preserved + flagged in Step 2. The adopt flow never
  installs such symlinks; the adopter created it post-
  adoption.
- **Post-checkout hook has extra logic** → preserved; the
  unadopt flow names the line to remove by hand.
- **`.gitignore` entry overlaps adopter rules** (e.g. the
  adopter also has `/.apache-steward/foo` for unrelated
  reasons) → only the exact adopt-template lines are
  removed; adopter rules stay.
- **Adopter ran `unadopt` then realised they wanted to keep
  override content** → the override directory was preserved
  by default; if they passed `--purge-overrides` and
  confirmed past the uncommitted-edits warning, the only
  recourse is `git restore` from a pre-unadopt commit. The
  flow makes this expensive on purpose.
- **`setup-steward` skill resolution fails after self-
  removal** → expected. Re-adoption goes via the install
  recipe in
  [`docs/setup/install-recipes.md`](../../../docs/setup/install-recipes.md),
  not via the now-deleted skill.
