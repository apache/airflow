---
name: airflow-contributor
description: >
  Resolve contributor workflow commands at runtime via context_detect.py.
  Activate when running tests, linting, formatting, static checks, or building docs.
  Never hardcode commands — always call context_detect.py to get the exact command
  for your current environment (host vs Breeze container).
license: Apache-2.0
---
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Airflow Contributor Skill — Runtime Command Resolver

## Why this skill exists

The Apache Airflow contributing docs are the canonical source of truth for
contributor workflows. This skill bridges the docs and the agent:

- **Skill definitions live in the contributing docs** as `.. agent-skill::` directives,
  co-located with the prose that describes them.
- **skills.json is generated** from those directives — never hand-edited.
- A **prek commit hook** (`context_detect.py --check`) blocks commits when
  skills.json drifts from the RST source. Drift is structurally prevented.

This means: when a maintainer updates a command in the contributing docs, the
skill updates automatically. No separate file to maintain.

## Command resolution

Always resolve commands at runtime — never hardcode from memory or CLAUDE.md:

```bash
python scripts/ci/prek/context_detect.py <skill-id> [param=value ...]
```

Discover available skills:

```bash
python scripts/ci/prek/context_detect.py --list
```

Run EXACTLY what context_detect.py returns — it already knows your environment
(host vs Breeze). Do not override its output with your own reasoning.

## Which skill to invoke

| Situation | Skill ID |
|---|---|
| Running a test with no `db_test` marker | `run-single-test` |
| Running a test with `@pytest.mark.db_test` | `run-db-test` |
| After editing any Python file | `format-and-lint` |
| Before committing | `run-static-checks` |
| Before opening a PR | `run-manual-checks` |
| After changing `.rst` files | `build-docs` |
| Setting up Breeze for the first time | `setup-breeze-environment` |

## Where skill definitions live

Skills are embedded in the contributing docs they describe:

| Skill(s) | Source of truth |
|---|---|
| `setup-breeze-environment` | `contributing-docs/03a_contributors_quick_start_beginners.rst` |
| `run-static-checks`, `run-manual-checks`, `format-and-lint` | `contributing-docs/08_static_code_checks.rst` |
| `build-docs` | `contributing-docs/11_documentation_building.rst` |
| `run-single-test`, `run-db-test` | `contributing-docs/testing/unit_tests.rst` |
