<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Breeze Contribution Agent Skills

This directory contains the PoC for **Breeze-aware "agent skills"** that AI
tools can execute reliably while respecting host vs Breeze container
boundaries.

The system has two artifacts:

- `SKILL.md`: human-editable structured source
- `skills.json`: machine-readable output generated from `SKILL.md`

Pre-commit/prek enforces that these two stay in sync.

## How skills work

Each skill is represented as a JSON object inside a fenced ` ```json ... ``` `
block in `SKILL.md`.

At minimum each skill must define:

- `id`: stable identifier
- `commands`: command templates

Command templates may reference parameter placeholders in the form:
`{param_name}`.

## How extraction works

The extraction pipeline is implemented in:

- `scripts/ci/prek/extract_breeze_contribution_skills.py`

Flow:

1. The extractor reads `SKILL.md`
2. It finds all fenced JSON blocks
3. It parses and validates the skills
4. It writes the normalized payload into `skills.json`

## How drift sync is enforced (prek hook)

The drift check is wired in `.pre-commit-config.yaml` as:

- `check-breeze-contribution-skills-drift`

That hook runs:

- `./scripts/ci/prek/validate_skills.py --check`

Behavior:

- If `SKILL.md` and `skills.json` diverge, the hook fails
- The error output includes what diverged and how to fix it

To regenerate `skills.json`:

```bash
python scripts/ci/prek/validate_skills.py --fix
```

## Troubleshooting

### Drift detected

Run:

```bash
python scripts/ci/prek/validate_skills.py --fix
```

Then commit the updated `skills.json`.

### Malformed JSON in SKILL.md

The extractor will fail with an error message indicating the failing block
and the JSON parse error location.
Fix the JSON inside the ` ```json ... ``` ` fence.

### Missing required fields

Each skill must contain `id` and `commands`.
If you also include `parameters`, ensure every parameter includes both:

- `description`
- `required` (boolean)

## Future evolution

The long-term direction is to make contributor documentation executable by
embedding skill blocks into `contributing-docs/*.rst` (docs-first source of
truth).

This PoC keeps the structured source in `.github/skills/...` as a stepping
stone so the extraction and drift-check pipeline is validated early.

## Example usage by agents

An agent can:

1. Detect context (host vs Breeze)
2. Ask for a skill by ID (for example `run-unit-tests`)
3. Substitute required parameters (example: `{test_path}`)
4. Execute the selected command template

The concrete mapping between context and command execution is implemented by
the agent runtime helper in:

- `scripts/ci/prek/breeze_context.py`
