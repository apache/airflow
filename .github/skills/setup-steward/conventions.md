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

## Detection algorithm

```text
if .claude/skills/ exists:
    if any entry in .claude/skills/ is a symlink resolving
    into .github/skills/:
        pattern = B (double-symlinked)
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
