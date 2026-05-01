<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- markdownlint-disable MD022 -->
---
name: run-static-checks
description: Run prek (pre-commit) static checks including ruff linting, formatting, and type checks on Airflow code. Works identically on host and inside Breeze.
---
<!-- markdownlint-enable MD022 -->

Run Static Checks
=================

Run the Airflow pre-commit pipeline (`prek`) to lint, format, and type-check changed
files. This must pass before any PR is merged.

Source of truth for this workflow is `contributing-docs` contributor guidance; the
commands below are the Breeze-aware execution mapping for agents.

Context Detection
-----------------

`prek` is installed both on the **host** and inside **Breeze**. The command is
identical in both environments — no context switching is needed.

Commands
--------

```bash
# Run all checks on files changed since main (standard pre-PR flow)
prek run --from-ref main --stage pre-commit

# Run slower manual checks too
prek run --from-ref main --stage manual

# Run only ruff linting
prek run ruff --from-ref main

# Run only ruff formatter
prek run ruff-format --from-ref main

# Run a single check by hook ID (e.g. mypy)
prek run mypy --from-ref main
```

Workflow Context
----------------

This is **Scenario 1, Step 2** of the standard Airflow contributor workflow:

1. stage-changes
2. → **run-static-checks** (this skill)
3. run-unit-tests

Always run static checks **before** running unit tests to catch simple formatting
or import errors early.

Prerequisites
-------------

- Changes must be staged (`git add`) before running — see the `stage-changes` skill.
- `prek` must be installed: `uv tool install prek`
- Hooks must be enabled: `prek install`

Interpreting Failures
---------------------

| Exit Code | Meaning |
|---|---|
| 0 | All checks passed |
| 1 | One or more checks failed — read the output to identify which hooks failed |

When a check fails, `prek` prints the hook name and the specific files/lines that
failed. Fix the reported issues and re-run. Many hooks (ruff, ruff-format) auto-fix
on first run — check `git diff` after failure to see auto-fixes.

Common Issues
-------------

- **`ruff` lint error:** Fix manually based on the error message, then re-stage and retry.
- **`ruff-format` failure:** Run `uv run ruff format <file>` then re-stage and retry.
- **`mypy` type error:** Fix the type annotation issue flagged by the error message.
- **`check-xml` / other infra checks:** Usually auto-fixed by the hook itself.

Success Criteria
----------------

`prek run --from-ref main --stage pre-commit` exits with code 0 and prints
`Passed` for every hook.
