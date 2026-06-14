<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# unadopt — remove the apache-steward framework from an adopter repo

The reverse of [`adopt.md`](adopt.md). Removes the framework
artefacts the adopt flow installed — gitignored snapshot,
committed lock, gitignored local lock, framework-skill
symlinks **in every active target dir** ([`agents.md`](agents.md)
— `.agents/skills/`, `.claude/skills/`, `.github/skills/`, plus
any present holdout), the matching `.gitignore` blocks,
post-checkout hook, the adoption sections in `README.md` /
`AGENTS.md` / `CONTRIBUTING.md`, and the committed `setup`
skill itself.

> **Critical — tear down *all* target dirs.** Removing only the
> `.claude/skills/` + `.github/skills/` pair would **orphan** the
> `.agents/skills/magpie-*` links (and any holdout's) — dangling
> symlinks into a snapshot that no longer exists, plus stale
> `.gitignore` blocks. The removal must cover every active target
> dir per [`agents.md`](agents.md).

By default the adopter-authored `.apache-magpie-overrides/`
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
[`/magpie-setup upgrade`](upgrade.md) instead — that path
preserves overrides and re-uses the existing wiring.

If the goal is to **temporarily detach for debugging** (e.g.
test what a skill looks like without overrides), edit the
relevant override file rather than unadopting.

## Inputs

- `--purge-overrides` — also `git rm -r` the
  `.apache-magpie-overrides/` directory. Default: preserve.
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
   > main: `cd <main-path> && /magpie-setup unadopt`. To
   > undo just this worktree's symlink without touching the
   > main, `rm <worktree>/.apache-magpie` manually."*

3. Confirm we are **not** in `apache/airflow-steward` itself
   (`git remote get-url origin`); refuse if it resolves to the
   framework — the framework is not "adopted into" itself.
4. Confirm `<committed-lock>` (`.apache-magpie.lock`) is
   present. If missing, the repo is not adopted — surface and
   stop. (If only the snapshot is present without a committed
   lock, the adopter ran the install recipe but never
   completed `/magpie-setup adopt`; treat that as not-yet-
   adopted and stop with the same message.)
5. Compute the **active target set** per
   [`agents.md`](agents.md): the canonical `.agents/skills/`, the
   `.claude/skills/` + `.github/skills/` relay pair, and any
   holdout dir present. There is no skills-dir convention to
   detect — every target carries `magpie-*` symlinks in the same
   canonical-plus-relay shape.

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
| Framework-skill symlinks | **Every active target dir** ([`agents.md`](agents.md)): the canonical `.agents/skills/` (always present), the `.claude/skills/` + `.github/skills/` relay pair, and any present holdout (`.windsurf/skills/`, `.goose/skills/`) | each `magpie-*` symlink — canonical entries resolving into `<snapshot-dir>/skills/`, relays resolving into `.agents/skills/magpie-*` — in **each** target dir |
| Post-checkout hook | `<repo-root>/.git/hooks/post-checkout` | exists + invokes `~/.claude/scripts/sandbox-add-project-root.sh` |
| Doc section: `README.md` | `<repo-root>/README.md` | contains the `## Agent-assisted contribution (apache-steward)` heading |
| Doc section: `AGENTS.md` | `<repo-root>/AGENTS.md` | contains the `## apache-steward framework` heading |
| Doc section: `CONTRIBUTING.md` | `<repo-root>/CONTRIBUTING.md` | contains the adoption section (fallback layout) |
| Overrides directory | `<repo-root>/.apache-magpie-overrides/` | exists; count framework-scaffold files vs adopter-authored |
| `setup` skill itself | canonical `.agents/skills/magpie-setup/` + the `.claude`/`.github` relay symlinks to it | exists (this is the only committed framework skill) |

For the overrides directory: distinguish the
**framework-scaffold** files (`README.md`, `user.md` from
[`adopt.md` Step 9 / 9b](adopt.md)) from
**adopter-authored** files (e.g. `pr-management-triage.md`,
any `user.md` filled in beyond the scaffold). Also check
`git status -- .apache-magpie-overrides/` for **uncommitted
edits** — those are surfaced separately in Step 2.

## Step 2 — Surface the removal plan

Render the inventory as a single table so the user can read
the blast radius at a glance:

```text
The following will be REMOVED:

  Gitignored (no commit needed):
    .apache-magpie/                      (snapshot, ~N MB)
    .apache-magpie.local.lock
    .agents/skills/magpie-<skill-1>      → .apache-magpie/skills/<skill-1>/   (canonical)
    .agents/skills/magpie-<skill-2>      → ...
    .claude/skills/magpie-<skill-1>      → ../../.agents/skills/magpie-<skill-1>   (relay)
    .github/skills/magpie-<skill-1>      → ../../.agents/skills/magpie-<skill-1>   (relay)
    <holdout>/skills/magpie-<skill-1>    → ../../.agents/skills/magpie-<skill-1>   (relay; e.g. .windsurf/skills/, .goose/skills/ — only if present)
    .git/hooks/post-checkout              (if it contains the steward recipe)
    # Target dirs (per agents.md): canonical .agents/skills/, the
    #   .claude/skills/ + .github/skills/ relay pair, plus any present
    #   holdout — each carries one magpie-<n> entry per linked skill.

  Committed (will show in `git status`):
    .apache-magpie.lock                  (the project's pin)
    .gitignore                            (the entries listed in adopt.md Step 7)
    README.md                             (the `## Agent-assisted contribution (apache-steward)` section)
    AGENTS.md                             (the `## apache-steward framework` section, if present)
    .agents/skills/magpie-setup/         (this skill itself — self-destructive; canonical copy)
    .claude/skills/magpie-setup          (relay symlink)
    .github/skills/magpie-setup          (relay symlink)

The following will be PRESERVED:

    .apache-magpie-overrides/           (M file(s); pass `--purge-overrides` to remove)
    ~/.config/apache-magpie/user.md     (per-user; shared with other adopters on this machine — remove manually if this was your last adoption)
```

Surface the `~/.config/apache-magpie/user.md` line only if that
file is actually present on disk. If it is absent (or the
operator drove `user.md` resolution via
`$APACHE_STEWARD_USER_CONFIG` / the legacy per-project location),
omit the line. The framework never touches the per-user file
regardless of `--purge-overrides` — it is shared across every
adopter project on the operator's machine and unadopting from
*this* project does not imply they have stopped using
apache-steward elsewhere.

If `--purge-overrides` was passed, move
`.apache-magpie-overrides/` into the *removed* section and
list its files explicitly so the adopter sees what custom
content is about to go.

If `git status -- .apache-magpie-overrides/` showed
uncommitted edits, prepend a **warning** above the table:

```text
⚠ Uncommitted edits in .apache-magpie-overrides/:
    <list of files>

  --purge-overrides would delete these along with the directory.
  Commit, stash, or copy them out before continuing.
```

If any `magpie-*` symlink in **any active target dir**
(canonical `.agents/skills/`, a holdout, or the `.claude/`/`.github/`
relay pair) resolves to a path **outside** the adoption — i.e. a
canonical entry that does not resolve into `<snapshot-dir>/`, a
relay that does not resolve through `.agents/skills/`, or an
adopter who committed a real skill at the same name post-adoption
— list it under a separate **Preserved (not framework-owned)**
subsection. The unadopt flow never deletes content it does not
own.

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

1. **Framework-skill symlinks — in every active target dir.**
   For each entry in the inventory, `rm` the `magpie-*` symlink.
   Cover **every active target dir** ([`agents.md`](agents.md)),
   not just the `.claude/`/`.github/` pair — skipping
   `.agents/skills/` or a holdout would orphan its `magpie-*`
   links once the snapshot is removed in step 3.

   - **Canonical target (`.agents/skills/`)** — remove each
     `.agents/skills/magpie-<n>` (the link into the snapshot).
   - **Relay targets (`.claude/skills/`, `.github/skills/`, any
     present holdout)** — remove each `<target>/skills/magpie-<n>`
     (the relay into `.agents/skills/`).

   The target dirs themselves (`.agents/skills/`, `.claude/skills/`,
   `.github/skills/`, any holdout) are **adopter-owned** and **not
   removed by unadopt** — they may predate framework adoption and
   serve the adopter's own native skills too. Only the `magpie-*`
   entries come out, never the directory.

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
   the obsolete `/magpie-setup verify --auto-fix-symlinks` line
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
   the diff stays minimal. **Exception:** leave `__pycache__/`
   and `*.pyc` in place — they are stock Python entries that
   most repos carry independently of the framework, so removing
   them would break the adopter's own Python ignores. Only drop
   them if they sit unambiguously inside the steward-managed
   block (under the same comment header the adopt flow wrote)
   and the repo has no other Python sources.
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
   `git rm -r .apache-magpie-overrides/`.
9. **`setup` skill itself.** `git rm -r` the canonical copy
   `.agents/skills/magpie-setup/` and its relay symlinks
   `.claude/skills/magpie-setup` and `.github/skills/magpie-setup`.
   After this step the running skill has deleted its own committed
   source. Future invocations of `/magpie-setup` will
   resolve to nothing — the adopter has to re-run the
   install recipe in
   [`docs/setup/install-recipes.md`](../../docs/setup/install-recipes.md)
   to re-adopt.

Each step is independently surfaced as it runs (one
`✓ Removed <path>` line per artefact), so a mid-flow abort
leaves a clean record of what made it out.

## Step 5 — Sanity check

After the deletions, verify the post-state:

- `<snapshot-dir>/` does not exist.
- `<committed-lock>` and `<local-lock>` do not exist.
- No `magpie-*` symlinks remain in **any active target dir**
  (canonical `.agents/skills/`, the `.claude/`/`.github/` relay
  pair, or any holdout) — neither dangling canonical links into
  the removed `<snapshot-dir>/` nor relays into the now-empty
  `.agents/skills/`.
- `.gitignore` no longer contains the steward entries.
- The doc sections are gone from the affected files.
- `.agents/skills/magpie-setup/` and its `.claude`/`.github`
  relays do not exist.
- If `--purge-overrides`: `.apache-magpie-overrides/` does
  not exist.
- If *not* `--purge-overrides`:
  `.apache-magpie-overrides/` does exist (unchanged).

Any ✗ here is a bug — surface it and stop.

## Output to the user

A summary of what was removed + what remains:

```text
✓ Snapshot removed:        .apache-magpie/
✓ Locks removed:           .apache-magpie.lock, .apache-magpie.local.lock
✓ Symlinks removed:        <count> across every active target dir — canonical .agents/skills/ + the .claude/skills/ + .github/skills/ relay pair + any present holdout
✓ Post-checkout hook:      removed (or: preserved — contained extra adopter logic)
✓ Doc sections removed:    README.md[, AGENTS.md][, CONTRIBUTING.md]
✓ .gitignore cleaned:      <N> entries removed
✓ setup skill:     removed (this skill self-destructed)

Preserved:
  .apache-magpie-overrides/   (M files; pass `--purge-overrides` to remove)
  ~/.config/apache-magpie/user.md   (per-user; shared with other adopters on this machine — remove manually if this was your last adoption)
  .agents/skills/, .claude/skills/, .github/skills/   (target dirs — adopter-owned; only the magpie-* entries were removed)
  <list of any non-steward-owned content the plan flagged>

Staged for commit (you'll see in `git status`):
  D  .apache-magpie.lock
  M  .gitignore
  M  README.md
  M  AGENTS.md             (if section was present)
  D  .agents/skills/magpie-setup/...   (+ .claude/.github relay symlinks)

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
- **Never silently delete `.apache-magpie-overrides/`.**
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
  lock before the `setup` skill itself. The order
  guarantees no intermediate state has a dangling reference.

## Failure modes

- **`<committed-lock>` missing** → repo not adopted. Stop
  with a pointer at `/magpie-setup adopt`.
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
  adopter also has `/.apache-magpie/foo` for unrelated
  reasons) → only the exact adopt-template lines are
  removed; adopter rules stay.
- **Stock Python entries (`__pycache__/`, `*.pyc`)** → left
  in place by default per Step 5; they predate adoption in
  most repos, so removing them would break the adopter's own
  Python ignores.
- **Adopter ran `unadopt` then realised they wanted to keep
  override content** → the override directory was preserved
  by default; if they passed `--purge-overrides` and
  confirmed past the uncommitted-edits warning, the only
  recourse is `git restore` from a pre-unadopt commit. The
  flow makes this expensive on purpose.
- **`setup` skill resolution fails after self-
  removal** → expected. Re-adoption goes via the install
  recipe in
  [`docs/setup/install-recipes.md`](../../docs/setup/install-recipes.md),
  not via the now-deleted skill.
