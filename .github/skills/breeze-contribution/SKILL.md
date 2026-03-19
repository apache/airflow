<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Breeze Contribution Agent Skills

## What this is

This file is the **structured source of truth** for the agent skills consumed
by AI tooling.

The machine-readable artifact is generated into:

- `.github/skills/breeze-contribution/skills.json`

`prek run check-breeze-contribution-skills-drift` (via
`scripts/ci/prek/validate_skills.py --check`) enforces that `SKILL.md` and
`skills.json` stay in sync.

## Skill format

The extractor looks for one or more fenced blocks of the form:

```text
{
  "id": "unique-skill-id",
  "name": "Human title",
  "description": "What the agent should do",
  "commands": {
    "host": "Command template for host execution",
    "breeze": "Command template for Breeze execution (optional but recommended)"
  },
  "preferred_context": "host|breeze|either",
  "parameters": {
    "PARAM_NAME": {
      "type": "string",
      "required": true,
      "description": "Parameter meaning"
    }
  },
  "prerequisites": [],
  "success_criteria": "exit_code == 0"
}
```

Required fields (for extraction/validation):

- `id` (string, unique)
- `commands` (object)

## Parameter template rule

If your command template includes `{param_name}` placeholders, add a matching
entry under `parameters`.

Parameter validation rules (when `parameters` is present):

- each parameter entry must include:
  - `description`
  - `required` (boolean)

## How to add a new skill

1. Add a new fenced JSON block here (use a fence that starts with ``` json and closes with ``` )
2. Use a new unique `id`
3. Add `commands.host` and/or `commands.breeze`
4. If you reference parameters in templates, define them under `parameters`
5. Regenerate `skills.json` via:
   - `python scripts/ci/prek/validate_skills.py --fix`

## Versioning and backward compatibility

`skills.json` is generated with a top-level `version` field (currently `1.0`).
Skill IDs should remain stable since agents refer to them by ID. New optional
fields can be added without breaking older agents as long as required keys
(`id`, `commands`) remain present.

## Future evolution toward docs-first source of truth

Long-term we want to embed structured blocks directly into contributor
documentation (for example `contributing-docs/*.rst`) and extract from there.
This PoC keeps `SKILL.md` under `.github/skills/` as the initial stepping stone.

## Skill: run-static-checks

```json
{
  "id": "run-static-checks",
  "name": "Run Static Checks",
  "description": "Run prek static checks (linting, formatting, type checks)",
  "commands": {
    "host": "prek run {--target module}",
    "breeze": "prek run {--target module}"
  },
  "preferred_context": "host",
  "parameters": {
    "module": {
      "type": "string",
      "required": false,
      "description": "Target module (e.g., airflow/api)"
    }
  },
  "prerequisites": [],
  "success_criteria": "exit_code == 0"
}
```

## Skill: run-unit-tests

```json
{
  "id": "run-unit-tests",
  "name": "Run Unit Tests",
  "description": "Run targeted unit tests with UV-first, Breeze-fallback strategy",
  "commands": {
    "host": "uv run --project airflow pytest {test_path}",
    "breeze": "breeze exec pytest {test_path}"
  },
  "preferred_context": "host",
  "parameters": {
    "test_path": {
      "type": "string",
      "required": true,
      "description": "Path to test file or directory"
    }
  },
  "prerequisites": ["run-static-checks"],
  "success_criteria": "exit_code == 0"
}
```

## Usage

These skills are consumed by:

1. AI coding agents
2. Human developers (for reference)
3. Generated `skills.json` (for machines)

To regenerate `skills.json`:

```bash
python scripts/ci/prek/validate_skills.py --fix
```

To check for drift:

```bash
python scripts/ci/prek/validate_skills.py --check
```
