<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# upgrade — refresh the gitignored snapshot per the committed lock

The upgrade flow is **drift-driven**. It detects mismatch
between `<committed-lock>` (project pin) and `<local-lock>`
(per-machine fetch), then re-installs per the committed lock,
refreshes symlinks, and reconciles overrides.

Two trigger paths land here:

1. **User-initiated** — explicit `/setup-steward upgrade`,
   e.g. after a colleague bumped `<committed-lock>` to a
   new framework version and the user wants to align.
2. **Drift-triggered from a framework skill** — any
   framework skill (or `/setup-steward verify`) detected
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

1. Read `<committed-lock>`. If missing, the repo isn't
   adopted — suggest `/setup-steward adopt` and stop.
2. Read `<local-lock>`. If missing (gitignored, fresh
   clone), the local install hasn't been initialised yet —
   route as a recover-snapshot install per the committed
   lock, not as an upgrade. Continue at Step 3.

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
- **`setup-steward` skill changed in the framework** —
  surface as a separate note. The adopter's *committed*
  copy of `setup-steward` will be auto-overwritten from the
  new snapshot in [Step 6b](#step-6b--overwrite-the-committed-setup-steward-skill-from-the-new-snapshot)
  so the bootstrap stays in sync with the framework version
  the project just pinned.

Ask for explicit confirmation before deleting and re-
installing.

## Step 3 — Delete the old snapshot

```bash
rm -rf .apache-steward
```

The snapshot is gitignored — destroying it loses no
committed work. Do this **before** the new install to avoid
"new layered on top of old" partial state.

## Step 4 — Install per the committed lock

Per `<committed-lock>.method`:

- **`git-branch`** — `git clone --depth=1 --branch
  <committed.ref> <committed.url> .apache-steward`. If
  `from:<git-ref>` was passed, use that branch instead of
  the committed one (this run only).
- **`git-tag`** — `git clone --depth=1 --branch
  <committed.ref> <committed.url> .apache-steward`. If
  `from:<git-ref>` overrides, use it.
- **`svn-zip`** — `curl` the zip + verify `sha512` +
  `unzip` to `.apache-steward/`. The verification step is
  **mandatory**; mismatched SHA-512 stops the upgrade and
  surfaces the discrepancy.

After install, capture the actual on-disk state for the
new `<local-lock>`:

- `source_method`, `source_url`, `source_ref` — from
  whatever method was used in this run (committed lock or
  `from:` override).
- `fetched_commit` — `git -C .apache-steward rev-parse
  HEAD` for git methods; the version for svn-zip.
- `fetched_at` — current ISO-8601 timestamp.

## Step 5 — Reconcile overrides

For each file in `<repo-root>/.apache-steward-overrides/`:

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

Walk `<adopter-skills-dir>` looking for stale symlinks that
point at framework skills no longer in the new snapshot
(rename, removal). For each:

- If renamed (the framework documented a rename in its
  release notes), offer to re-symlink to the new name.
- If removed, offer to remove the stale symlink.

Then walk the new snapshot for any new framework skills in
the families the adopter previously picked, and offer to
symlink them in.

For the double-symlinked convention, refresh both layers.

## Step 6b — Overwrite the committed `setup-steward` skill from the new snapshot

The adopter-side committed `setup-steward` skill is the
**only** framework skill that lives as a committed copy
rather than a gitignored symlink (per
[`SKILL.md` Golden rule 6](SKILL.md#golden-rules)). When the
framework's `setup-steward` evolves — new sub-action, lock-
format change, drift-detection refinement — the adopter's
copy must follow, or the bootstrap on a fresh clone will run
old logic against a new snapshot.

This step keeps the two in sync **automatically on every
upgrade**:

1. Compute the diff between the adopter-side
   `<adopter-skills-dir>/setup-steward/` (committed copy)
   and the snapshot's
   `.apache-steward/.claude/skills/setup-steward/`.
2. **If the adopter has local modifications** to their
   committed copy beyond what's in the snapshot — surface
   the diff and stop. Do **not** silently overwrite local
   work. The user either (a) confirms the modifications can
   be discarded, (b) decides to upstream them as a PR
   against `apache/airflow-steward` first, or (c) defers the
   bootstrap-skill update to a later upgrade run.
3. **If there are no local modifications** (or the user
   confirmed in 2), copy the snapshot's `setup-steward`
   over the committed copy:

   ```bash
   # For the flat layout:
   rm -rf .claude/skills/setup-steward
   cp -r .apache-steward/.claude/skills/setup-steward \
         .claude/skills/setup-steward

   # For the double-symlinked layout (e.g. apache/airflow):
   rm -rf .github/skills/setup-steward
   cp -r .apache-steward/.claude/skills/setup-steward \
         .github/skills/setup-steward
   # The .claude/skills/setup-steward symlink does not need
   # touching — it points at .github/skills/setup-steward
   # which is now the new content.
   ```

4. The new bootstrap-skill content lands as **modified files
   in `git status`** at the adopter's committed-skill path.
   The user reviews the diff and commits it as part of the
   upgrade PR; on merge, every other contributor's next
   `/setup-steward` run loads the matching version.

The adopter shouldn't modify the bootstrap copy locally —
the framework's hard rule is *"local mods go in
`.apache-steward-overrides/`, framework changes go via PR
to `apache/airflow-steward`"*. But if they did, this step
catches it before the overwrite would erase their work.

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
  Snapshot:  refreshed at .apache-steward/

Symlinks:
  ✓ <list of unchanged symlinks>
  + <list of newly-symlinked framework skills>
  - <list of removed stale symlinks>

Overrides:
  ✓ <list of overrides whose target is unchanged>
  ⚠ <list of overrides flagged for re-anchoring> (open the
     file and update against the new framework structure)

Recommended follow-ups:
  - Run /setup-isolated-setup-update if the secure-setup blast
    radius (settings.json, agent-isolation/, pinned-versions.toml)
    appears in the diff.
  - Open .apache-steward-overrides/<name>.md for any ⚠ entry above.
```

## Failure modes

- **`<committed-lock>` missing** → repo not adopted; suggest
  `/setup-steward adopt`.
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
