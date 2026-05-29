<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/legal/release-policy.html -->

# conventions — auto-detect the adopter's skills-dir layout

Different ASF projects already organise their `.claude/skills/`
differently. Before `setup-steward adopt` creates symlinks
into the snapshot, it detects which pattern is in place and
matches it. The framework's symlinks land at the same depth
as the adopter's existing skills, not one level off.

## Patterns

### A. Flat — skills live directly in `.claude/skills/`

```text
<repo-root>/
└── .claude/
    └── skills/
        └── <skill-name>/
            └── SKILL.md
```

The simple, default Claude Code layout. Most repos that just
started using Claude Code use this. **Detection signal**:
`.claude/skills/<n>/SKILL.md` is a regular file.

For framework symlinks: create them at
`<repo-root>/.claude/skills/<n>` → relative path into
`.apache-steward/.claude/skills/<n>/`.

**Caveat — `.claude/` already gitignored.** Some adopters (notably
those that previously used Claude Code with per-user `.claude/`
settings) have `.claude/` listed in their repo's `.gitignore`.
This prevents `.claude/skills/setup-steward/` from being committed
per [`SKILL.md` Golden rule 6](SKILL.md#golden-rules) (which expects
`setup-steward` itself to be the only committed framework skill).

Three resolution paths:

- **Override the gitignore** — add `!/.claude/skills/setup-steward/`
  after the broader `.claude/` line. Keeps the rest of `.claude/`
  gitignored; commits only the framework's bootstrap skill.
- **Switch to Pattern B** — move skills to `.github/skills/` and use
  the double-symlinked layout. Sidesteps the `.claude/` gitignore
  entirely. See *B. Double-symlinked* below.
- **Defer commit** — treat the whole adoption as a no-commit local
  experiment until the gitignore is revised. Useful for spike-style
  evaluation.

The adopt flow surfaces the conflict when it detects `.claude/` is
gitignored AND Pattern A was selected; the user picks one of the
three above.

### B. Double-symlinked — `.claude/skills/` mirrors `.github/skills/`

```text
<repo-root>/
├── .claude/
│   └── skills/
│       └── <skill-name>  →  ../../.github/skills/<skill-name>/
└── .github/
    └── skills/
        └── <skill-name>/
            └── SKILL.md
```

The pattern apache/airflow uses today (e.g. apache/airflow has this): actual skill content
lives under `.github/skills/`; `.claude/skills/<n>` is a
relative symlink pointing into `.github/skills/<n>/`. The
rationale (per the airflow team): `.github/` is the canonical
infra-glue directory in apache/airflow (e.g. that project), and `.claude/` is a
view of those skills filtered for Claude Code.

**Detection signal**: at least one entry in `.claude/skills/`
is a symlink resolving into `.github/skills/`.

For framework symlinks: create *both* layers — the inner
`.github/skills/<n>` → relative path into
`.apache-steward/.claude/skills/<n>/`, and the outer
`.claude/skills/<n>` → `../../.github/skills/<n>/` (matching
the existing pattern). Both layers gitignored.

### C. None yet — neither directory exists

A new adopter that has never used Claude Code skills before.
The skill creates the directory layout the adopter prefers
(default: pattern A, flat — simpler). If the user has a
preference, they say so during the adopt flow.

### D. Single directory symlink — one of `.claude/skills` / `.github/skills` is a symlink to the other

```text
# D.1 — content under .github/skills/, .claude/skills is the symlink:
<repo-root>/
├── .claude/
│   └── skills  →  ../.github/skills/
└── .github/
    └── skills/
        ├── <native-skill>/
        │   └── SKILL.md
        ├── <framework-symlink>  →  ../../.apache-steward/.claude/skills/<framework-skill>/
        └── ...
```

```text
# D.2 — content under .claude/skills/, .github/skills is the symlink:
<repo-root>/
├── .claude/
│   └── skills/
│       ├── <native-skill>/
│       │   └── SKILL.md
│       ├── <framework-symlink>  →  ../../.apache-steward/.claude/skills/<framework-skill>/
│       └── ...
└── .github/
    └── skills  →  ../.claude/skills/
```

A simplification of Pattern B: instead of one per-skill
symlink mirroring every entry from one directory to the
other, **one of the two directories is itself a symlink to
the other**. Both `.claude/skills/<n>` and
`.github/skills/<n>` always resolve to the same content for
every skill — the project's native skills and the framework's
gitignored symlinks alike — without any per-skill plumbing.
Adding a new skill (project-native or framework) just means
adding it once in the canonical directory; the mirror is
automatic.

**Two orientations** — same shape, opposite direction:

- **D.1** — content lives under `.github/skills/`,
  `.claude/skills` is the symlink. The natural choice for
  projects whose canonical skills directory is `.github/`
  (e.g. apache/airflow, which uses `.github/` as its
  infra-glue root and `.claude/` as a Claude-Code-facing
  view).
- **D.2** — content lives under `.claude/skills/`,
  `.github/skills` is the symlink. The natural choice for
  projects whose canonical skills directory is `.claude/`
  (e.g. a Pattern A project that wants `.github/skills/`
  available too without duplicating content).

**Detection signal**: exactly one of `.claude/skills` /
`.github/skills` is a symlink (test with `[ -L <path> ]` /
`readlink <path>`) and resolves to the other path in the same
repo. Either orientation counts as Pattern D.

For framework symlinks: create them at **only one layer** —
the *real* directory side, never the symlinked side. With
D.1 that means `.github/skills/<n>` → relative path into
`.apache-steward/.claude/skills/<n>/`; with D.2 it means
`.claude/skills/<n>` → the same. The opposite path is
automatically the same content via the directory symlink.

Gitignore consequences: only entries on the real-directory
side are needed (e.g. `/.github/skills/security-*` for D.1,
or `/.claude/skills/security-*` for D.2). Git treats the
symlinked side as a single tracked symlink and does not
descend into it, so ignore entries on that side would match
no actual tracked path and are unnecessary.

The directory symlink itself is **adopter-owned** — created
deliberately by the adopter as part of the project's layout
choice, and not touched by `/setup-steward unadopt`. The
framework treats it the same way it treats the real-directory
side: as part of the surrounding repo layout.

**Pre-Pattern-D consolidation** — if both `.claude/skills/`
and `.github/skills/` exist as **regular directories** (not
yet symlinked to each other) and contain skill content that
is not already aliased through symlinks, the adopt flow
**does not silently apply Pattern D**. Each directory's
contents are an independent set; turning one into a symlink
to the other would clobber the symlinked side's content. The
flow surfaces the conflict and offers a consolidation prompt:

1. List the skills present in each directory (real
   directories, regular files, and any non-Pattern-B
   symlinks).
2. Flag name collisions where the same skill name exists in
   both directories with different content.
3. Ask the user to pick D.1 or D.2 and confirm the
   consolidation steps:
   - Move every skill from the side that will become the
     symlink into the side that will become the real
     directory, resolving any flagged name collisions first.
   - Replace the now-empty side with a relative symlink to
     the other side.
4. Only after the consolidation is complete does the adopt
   flow proceed to wire framework symlinks at the chosen
   real-directory side.

If the consolidation cannot proceed (unresolved name
collisions the user has not addressed), the adopt flow stops
and lets the user resolve in their own commit before
re-invoking — the framework never auto-renames adopter-owned
content.

## Detection algorithm

```text
# Pattern D first — either orientation:
if .claude/skills is a symlink:
    if it resolves to .github/skills/ in the same repo:
        pattern = D.1 (single directory symlink; canonical = .github/skills/)
    else:
        # operator pointed `.claude/skills` somewhere else
        # deliberately; surface, do not guess.
        pattern = ambiguous → prompt the user
elif .github/skills is a symlink:
    if it resolves to .claude/skills/ in the same repo:
        pattern = D.2 (single directory symlink; canonical = .claude/skills/)
    else:
        # same — surface the unexpected target, do not guess.
        pattern = ambiguous → prompt the user

# Otherwise fall through to A / B / C:
elif .claude/skills/ exists (regular directory):
    if any entry in .claude/skills/ is a symlink resolving
    into .github/skills/:
        pattern = B (double-symlinked)
    else:
        if .github/skills/ also exists as a regular directory
        with independent content:
            pattern = ambiguous → propose Pattern D
                                   consolidation (see *Pre-Pattern-D
                                   consolidation* under section D
                                   above), with A as the fallback
                                   if the user declines
        else:
            pattern = A (flat)
elif .github/skills/ exists:
    pattern = B (the user has a `.github/skills/` half but
                  hasn't wired up `.claude/` yet — the adopt
                  flow will create the .claude/ side as part
                  of installing framework skills)
else:
    pattern = C (none yet — default to A unless user picks
                  otherwise)
```

## What the adopt flow does per pattern

| Pattern | `<adopter-skills-dir>` (where framework symlinks land) | Side effect |
|---|---|---|
| A — flat | `.claude/skills/` | None |
| B — double-symlinked | `.github/skills/` (the inner layer); `.claude/skills/` symlinks to it | If `.github/skills/<n>` for a framework skill already exists as a real directory (an old in-repo copy), refuse and let the user resolve |
| C — none yet | `.claude/skills/` | Create the directory |
| D.1 — single directory symlink, canonical `.github/skills/` | `.github/skills/` (the only layer; `.claude/skills` resolves into it via the directory symlink) | None — no outer-layer plumbing to create |
| D.2 — single directory symlink, canonical `.claude/skills/` | `.claude/skills/` (the only layer; `.github/skills` resolves into it via the directory symlink) | None — no outer-layer plumbing to create |

## Ambiguous cases

- **The repo has both `.claude/skills/` and `.github/skills/`
  but neither contains symlinks linking the two**. This is a
  half-migrated state. The adopt flow surfaces it and asks
  the user which pattern they want; it does not guess.
- **Pattern B but with absolute symlinks** (rather than
  relative). The adopt flow will use *relative* symlinks for
  consistency. If the user wants absolute, they say so;
  otherwise relative is the default — it survives a repo
  move.
- **`.claude/skills` (or `.github/skills`) is a symlink but
  resolves outside the repo or to a path other than the
  expected counterpart directory**. The operator pointed it
  somewhere deliberately (e.g. a sibling worktree). The
  adopt flow surfaces the resolved target and asks the user;
  it does not match Pattern D automatically.
- **Both `.claude/skills/` and `.github/skills/` exist as
  regular directories with independent (non-aliased)
  content**. Surfaced as a Pattern D consolidation
  opportunity per the **Pre-Pattern-D consolidation** flow
  under section D above. The user picks D.1 or D.2 (or
  declines, in which case the flow falls back to Pattern A
  treating `.claude/skills/` as canonical).
