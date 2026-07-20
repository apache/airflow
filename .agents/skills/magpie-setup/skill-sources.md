<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

<!-- Placeholder convention (see ../../AGENTS.md#placeholder-convention-used-in-skill-files):
     <project-config>            → adopter's `.apache-magpie-overrides/` directory
     <source-snapshot>           → `.apache-magpie-sources/<source-id>/` (gitignored fetch of one source)
     <sources-lock>              → `.apache-magpie.sources.lock` (committed — the project's per-source pins)
     <sources-local-lock>        → `.apache-magpie.sources.local.lock` (gitignored — per-machine fetch record)
     <trust-list>                → `<project-config>/skill-sources.md` (the adopter opt-in / install gate)
     <upstream>                  → adopter's public source repo (the repo this skill is being run in) -->

# setup — `skill-sources` (pull skills from trusted external sources)

Wire a skill or a whole skill-family from a **trusted external
source** — a repo other than `apache/magpie` that ships
Magpie-shaped skills — into the adopter repo so it behaves
**exactly like an in-tree framework skill**: same
`magpie-`-prefixed canonical-plus-relay symlink, same override
layer, same eval binding.

This is the runnable half of the [trusted external skill
sources](../../docs/skill-sources/README.md) feature. The
formats (source descriptor, `skills/<name>/source.md` pointer),
the trust model, and the §13 carve-out are defined there and in
[`RFC-AI-0006`](../../docs/rfcs/RFC-AI-0006.md); this sub-action
does the fetch, the pin, and the symlink.

**The rule that governs everything here:** per
[`PRINCIPLES.md` §13](../../PRINCIPLES.md#13-snapshot-plus-override-never-vendored-copies),
a source is fetched **only if the adopter has listed it in
`<trust-list>` and committed its pin.** An org curating a source,
or the [registry](../../docs/skill-sources/registry.md) listing
one, never triggers an install. This sub-action reads the trust
list and does nothing for a source that is not on it.

Invocations (registered in [`SKILL.md`](SKILL.md#sub-actions)):

- `/magpie-setup skill-sources` — reconcile every trusted source:
  fetch/verify, pin, symlink the provided skills.
- `/magpie-setup skill-sources add <source-id>` — the same flow
  scoped to one source id (the id must already be present in
  `<trust-list>` — this sub-action never edits the trust list;
  vouching for a source is the adopter's committed act).

**Main-checkout only**, like `adopt`/`upgrade` — a worktree picks
up the source snapshots through the same `<snapshot-dir>` symlink
that `worktree-init` seeds (the source snapshots live beside the
framework snapshot and are shared the same way). Refuse to run in
a worktree; direct the user to run it in the main checkout.

## The two source locks

Sources reuse the framework's [two-lock drift
model](SKILL.md#the-two-lock-files) verbatim, in a **separate pair
of files** so a source re-pin is never entangled with a framework
upgrade:

### `<sources-lock>` — `.apache-magpie.sources.lock` (committed)

The project's **per-source pins** — one block per trusted source,
keyed by `id`, carrying the same pin keys as the framework lock
(`method` / `url` / `ref` + the per-method verification anchor).
Edited only by this sub-action and `upgrade`; do not modify by
hand.

```text
# .apache-magpie.sources.lock — committed; the project's per-source pins.

- id:     acme-security-skills
  method: git-tag
  url:    https://github.com/acme/magpie-skills
  ref:    v2.1.0
  commit: <SHA the tag resolved to when pinned>

- id:     example-svn-source
  method: svn-zip
  url:    https://downloads.example.org/skills/skills-1.4.0.zip
  ref:    1.4.0
  sha512: <released archive SHA-512>
```

`git-branch` sources carry `method`/`url`/`ref` and **no**
cryptographic anchor (tip-tracking, WIP-only) — exactly as for a
`git-branch` framework snapshot.

### `<sources-local-lock>` — `.apache-magpie.sources.local.lock` (gitignored)

The **per-machine fetch fingerprint** — what this checkout
actually pulled for each source, and when. One block per source,
keyed by `id`.

```text
# .apache-magpie.sources.local.lock — gitignored; per-machine.

- id:             acme-security-skills
  source_method:  git-tag
  source_url:     https://github.com/acme/magpie-skills
  source_ref:     v2.1.0
  fetched_commit: <commit SHA on disk now>
  fetched_at:     <ISO-8601 timestamp>
```

**Drift** for a source is a committed-vs-local mismatch on its
block, read identically to the framework drift check: every
framework skill (and `verify`) compares the two and, on a gap,
proposes `/magpie-setup upgrade`. The committed lock is the pin
that travels with the repo; the local lock is per-machine truth.

## Step 0 — Pre-flight

1. **Main-checkout guard.** As `adopt`/`upgrade`
   ([`SKILL.md` Sub-actions](SKILL.md#sub-actions)): if
   `git rev-parse --git-dir` ≠ `git rev-parse --git-common-dir`,
   this is a linked worktree — refuse and tell the user to run
   the reconcile in the main checkout (the worktree already sees
   the source snapshots via its shared `<snapshot-dir>`).
2. **Framework adopted?** Require `<committed-lock>`
   (`.apache-magpie.lock`) and a live `<snapshot-dir>`. If the
   framework itself is not adopted yet, stop and point the user
   at `/magpie-setup` first — trusted sources ride on the same
   symlink relay the framework install establishes.
3. **Trust list present?** Read `<trust-list>`
   (`<project-config>/skill-sources.md`). If it is absent or
   lists no sources, there is nothing to do — say so and exit
   (this is the common, expected case: an adopter running only
   in-tree framework skills).

## Step 1 — Resolve the trusted sources

For every entry in `<trust-list>`
([format](../../docs/skill-sources/README.md#source-descriptor)):

1. **Resolve the descriptor.** An entry that is a bare
   `id` (+ `ref` + anchor) **references** an org-curated
   descriptor — look it up in
   `organizations/<org>/skill-sources.md` for the org resolved by
   the standard `project → organization → framework` precedence
   (see [`AGENTS.md`](../../AGENTS.md#configuration-resolution-order)).
   An entry that carries a full descriptor is used as-is. Merge:
   the org descriptor supplies `method`/`url`/`layout`/`provides`;
   the adopter entry supplies (and may override) `ref` + anchor —
   the adopter's committed pin always wins.
2. **Validate before any fetch.** `organization:` must name a
   directory under `organizations/`; `method` must be one of
   `git-tag`/`git-branch`/`svn-zip`; a non-`git-branch` source
   must carry its anchor (`commit` for git-tag, `sha512` for
   svn-zip). A source that fails validation is reported and
   **skipped** — never fetched on a partial pin. (The
   [skill-and-tool-validator](../../tools/skill-and-tool-validator/)
   enforces the same shape statically on the descriptor files.)
3. Present the resolved, validated source list (id, org, method,
   ref, what it `provides`) and the exact fetch each will run.
   This is the point of consent before any network egress.

## Step 2 — Fetch + verify each source

Into **`.apache-magpie-sources/<source-id>/`** (gitignored,
sibling to `<snapshot-dir>` — kept separate on purpose so a
framework `upgrade`, which deletes `<snapshot-dir>` outright, does
**not** wipe the source snapshots). Reuse the framework [install
recipes](../../docs/setup/install-recipes.md) **verbatim**,
parameterized by the source `url`/`ref`:

- **`git-tag` / `git-branch`** — `git clone --depth=1 --branch <ref>
  <url> .apache-magpie-sources/<id>`. For `git-tag`, resolve
  `HEAD` after clone and confirm it equals the committed `commit`
  anchor; a mismatch aborts that source (the tag moved — a
  supply-chain signal).
- **`svn-zip`** — download the archive, `sha512sum -c` against the
  committed `sha512`, optional `gpg --verify` against the source's
  published `KEYS`, then unzip into
  `.apache-magpie-sources/<id>/`. A checksum failure aborts that
  source.

A source that fails verification is left un-installed and
reported; the others proceed. Never fall back to an unverified
fetch.

## Step 3 — Write both source locks

1. **`<sources-lock>`** (committed) — for a first pin of a source,
   write its block: `method`/`url`/`ref` + the resolved anchor
   (`commit` from the clone for git-tag; `sha512` for svn-zip).
   Bumping an existing pin is a deliberate project action and
   shows up in the PR diff, exactly like a framework-lock bump.
2. **`<sources-local-lock>`** (gitignored) — write/refresh this
   machine's fetch fingerprint for each source
   (`source_*` + `fetched_commit` + `fetched_at`).

The two source locks + the `.apache-magpie-sources/` snapshot dir
must be gitignored — see
[`adopt.md` Step 7](adopt.md#step-7--gitignore-entries-fresh-only),
which writes the block. If any line is missing (e.g. this is the
first source on an older adoption), add it here idempotently.

## Step 4 — Select the provided skills

Expand each source's `provides` against its **fetched**
`skills_root`:

- `skill: <name>` → the single directory `skills/<name>/`.
- `family: <prefix>-*` → every `skills/<prefix>-*` directory in
  the fetched snapshot — the **same prefix-glob** as framework
  family selection ([`adopt.md` Step 8](adopt.md#step-8--wire-up-the-framework-skill-symlinks)),
  computed fresh from disk, never a hard-coded list.

Show the resolved skill set per source and confirm. A
**pointer file** (`skills/<name>/source.md`) may already sit in
the adopter's committed tree marking where a specific skill is
expected; a `provides` entry and a pointer are two views of the
same intent — reconcile them (a pointer with no matching
`provides` is surfaced as an unfulfilled redirect).

## Step 5 — Wire the symlinks

Identical to the framework's canonical-plus-relay model
([`adopt.md` Step 8](adopt.md#step-8--wire-up-the-framework-skill-symlinks),
[`agents.md`](agents.md)) — the target is the source snapshot
instead of the framework snapshot:

- **Canonical (`.agents/skills/`)** — one gitignored symlink per
  provided skill: `.agents/skills/magpie-<name>` →
  `../../.apache-magpie-sources/<source-id>/skills/<name>/`.
- **Relays (`.claude/skills/`, `.github/skills/`, any present
  holdout)** — one gitignored symlink per skill:
  `<target>/skills/magpie-<name>` →
  `../../.agents/skills/magpie-<name>` (back through the canonical
  entry, never at the source snapshot).

Every source skill is `magpie-`-prefixed, so the same
`magpie-*` gitignore glob and the same `symlink-lint`
relay-through-canonical rule that cover framework skills cover
source skills unchanged. **Never overwrite an existing committed
skill or a framework symlink of the same name** — a source that
`provides` a `magpie-<name>` already claimed by the framework or
another source is a collision: surface it and stop, do not
silently shadow.

The pulled skill's eval suite rides along in the source snapshot
at `.apache-magpie-sources/<id>/tools/skill-evals/evals/<name>/`;
because the source keeps the framework's two-tree layout (declared
in its descriptor `layout:` and required by the
[layout contract](../../docs/skill-sources/README.md#layout-contract--skills-evals-tests)),
the eval binding resolves after fetch with no extra wiring.

## Step 6 — Chain worktree propagation

As with `adopt`/`upgrade`, finish by running
[`worktree-init`](worktree-init.md) on every linked worktree so
each one's shared `<snapshot-dir>` and agent-dir symlinks reflect
the new source skills. Unconditional and idempotent — a no-op when
there are no worktrees.

## Relationship to `adopt`, `upgrade`, `verify`

- **`adopt`** chains into this sub-action as its final content
  pass when `<trust-list>` lists any source, so a fresh adoption
  that already trusts a source wires it in the same run (see
  [`adopt.md` Step 8b](adopt.md#step-8b--wire-up-trusted-external-source-skills)).
- **`upgrade`** re-fetches every source per its committed
  `<sources-lock>` pin, refreshes the symlinks, and extends drift
  detection to the two source locks (see
  [`upgrade.md` Step 6f](upgrade.md#step-6f--re-fetch-trusted-external-sources)).
- **`verify`** reports source-snapshot presence, source-symlink
  health, and source drift (committed vs local) alongside the
  framework checks (see
  [`verify.md` check 10](verify.md#10-trusted-external-source-snapshots--symlinks)).

## Failure modes

| Symptom | Likely cause | Remediation |
|---|---|---|
| "nothing to do — no trusted sources" | `<trust-list>` absent or empty | Expected when running only in-tree skills. Add a source to `<trust-list>` (and commit its pin) to install one. |
| A source is skipped with a validation error | Unknown `organization:`, bad `method`, or a missing anchor on a non-`git-branch` source | Fix the descriptor / pin in `<trust-list>` (or the org's `skill-sources.md`) and re-run. |
| `git-tag` fetch aborts on a `commit` mismatch | The upstream tag was moved after it was pinned — a supply-chain signal | Re-review the source; only re-pin (bump `commit` in `<sources-lock>`) once the new tag is trusted. |
| `svn-zip` fetch aborts on a checksum failure | The archive changed, or the pinned `sha512` is wrong | Re-verify the archive out-of-band before re-pinning. |
| A provided `magpie-<name>` collides with a framework or another source skill | Two sources (or a source and the framework) claim the same prefixed name | Rename is the source's call; drop one `provides`/pointer to resolve locally. |
| Worktree can't see a source skill | Source snapshots are shared via the worktree's `<snapshot-dir>` symlink, seeded by `worktree-init` | `/magpie-setup verify` in the worktree; `worktree-init` if the symlink is missing. |
