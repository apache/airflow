<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

<!-- markdownlint-disable MD022 -->
---
name: run-unit-tests
description: Run Airflow unit tests with context-aware command selection. Uses uv on the host and breeze exec inside the container. Supports targeted test paths to avoid running the full suite.
---
<!-- markdownlint-enable MD022 -->

Run Unit Tests
==============

Run a targeted subset of Airflow unit tests. The correct command differs depending
on whether you are running on the **host** or inside a **Breeze** container.

Source of truth for test workflow semantics is `contributing-docs`; this skill maps
that guidance into context-aware host/Breeze command execution.

Context Detection
-----------------

Check for the Docker container marker before choosing a command:

```bash
# You are inside Breeze if this file exists:
ls /.dockerenv
```

| Context | Detection | Command |
|---|---|---|
| **Host** | `/.dockerenv` absent | `uv run --project <PROJECT> pytest <path>` |
| **Breeze** | `/.dockerenv` present | `breeze exec pytest <path>` |

You can also call `python scripts/ci/prek/breeze_context.py` to auto-detect and
print `host` or `breeze`.

Commands
--------

On the Host
-----------

`<PROJECT>` is the folder containing the relevant `pyproject.toml`, e.g.
`airflow-core`, `providers/amazon`, `task-sdk`, `scripts`.

```bash
# Run a single test method
uv run --project <PROJECT> pytest path/to/test.py::TestClass::test_method -xvs

# Run all tests in a file
uv run --project <PROJECT> pytest path/to/test.py -xvs

# Run all tests in a package
uv run --project <PROJECT> pytest path/to/package/ -xvs

# If uv fails due to missing system dependencies, fall back to Breeze:
breeze testing core-tests --run-in-parallel
```

Inside Breeze
-------------

```bash
# Run a single test method
breeze exec pytest path/to/test.py::TestClass::test_method -xvs

# Run all tests in a file
breeze exec pytest path/to/test.py -xvs

# Run Breeze's built-in parallel test runners
breeze testing core-tests --run-in-parallel
breeze testing providers-tests --run-in-parallel
```

Workflow Context
----------------

This is **Scenario 2** of the standard Airflow contributor workflow:

1. stage-changes
2. run-static-checks
3. → **run-unit-tests** (this skill)

Run static checks first to avoid failing tests due to trivial formatting issues.

Prerequisites
-------------

- **Host:** `uv` must be installed and the project synced.
- **Breeze:** Docker must be running and the Breeze image built. Start a Breeze
  shell with `breeze shell` if not already running.
- Tests requiring a database backend: use `--backend postgres` or `--backend mysql`.

Interpreting Results
--------------------

| Exit Code | Meaning |
|---|---|
| 0 | All selected tests passed |
| 1 | One or more tests failed — check `FAILED` lines in output |

Use `-xvs` flags for verbose output and early exit on first failure. This makes
it easier to read the root cause when tests fail.

Success Criteria
----------------

`pytest` exits with code 0 and reports `passed` for all selected tests with no
`FAILED` or `ERROR` lines.
