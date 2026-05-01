<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# AGENTS instructions

## Purpose

This file is a short map for agents. Load this first, then load CONSTITUTION.md for
project-wide modification rules and constraints.

## Main entrypoints

- Install prek: `uv tool install prek`
- Enable commit hooks: `prek install`
- Install breeze shim (one-time, per machine): `scripts/tools/setup_breeze` — installs `~/.local/bin/breeze` that runs breeze via `uvx` from the current git worktree's `dev/breeze` (so each worktree, including ephemeral agent worktrees, gets its own breeze tied to its sources). See [ADR 0017](dev/breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md).
- **Never run pytest, python, or airflow commands directly on the host** — prefer `breeze run` for integration tests. Unit tests can run on the host via `uv run`.
- Place temporary scripts in `dev/` (mounted as `/opt/airflow/dev/` inside Breeze).
- **DevContainers & Worktrees:** DevContainers are treated as host environments (no `BREEZE` var). Worktrees are supported via the `breeze` shim.

## Fast test and check commands

- Run scripts tests: `uv run --project scripts pytest scripts/tests/ -xvs`
- Run static checks: `prek run --from-ref <target_branch> --stage pre-commit`
- Run manual checks: `prek run --from-ref <target_branch> --stage manual`
- Run one test: `uv run --project <PROJECT> pytest path/to/test.py::TestClass::test_method -xvs`

## Quality gates

Before marking a task complete:

1. Run static checks for changed files:
   - `prek run --from-ref <target_branch> --stage pre-commit`
2. Run manual checks when required:
   - `prek run --from-ref <target_branch> --stage manual`
3. Run relevant tests and confirm they pass.
4. Validate branch diff contains only intentional task files.

## Skill loading

Machine-readable skills are under `.agents/skills/`. Skills are NOT loaded
automatically — load them on demand when the task requires it.

- When staging files for commit → load `.agents/skills/stage-changes/SKILL.md`
- When running static checks → load `.agents/skills/run-static-checks/SKILL.md`
- When running tests → load `.agents/skills/run-unit-tests/SKILL.md`

## Required deeper rules

Load `CONSTITUTION.md` before making code changes or preparing a PR.

## Source-of-truth docs

- `contributing-docs/03a_contributors_quick_start_beginners.rst`
- `contributing-docs/05_pull_requests.rst`
- `contributing-docs/08_static_code_checks.rst`
- `contributing-docs/09_testing.rst`
- `contributing-docs/19_execution_api_versioning.rst`
