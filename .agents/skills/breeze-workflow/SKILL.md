---
name: breeze-workflow
description: >
  Orchestrate testing, code quality checks, and development commands across host
  and Apache Airflow Breeze environments. Detects execution context (Breeze container,
  devcontainer, or host), selects between fast local `uv run --project <PROJECT>`
  execution and containerized `breeze run` / `breeze testing`, and enforces guardrails
  against nested Docker execution and un-isolated test runs.
license: Apache-2.0
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Breeze Contributor Workflow & Command Routing

This skill guides coding agents and contributors to efficiently run tests, perform static checks,
and execute Airflow development commands across different execution environments (Host, Breeze container,
or Devcontainer).

The single authoritative source for contributor documentation is `contributing-docs/`. This skill provides
the decision logic and guardrails for choosing the right toolchain and execution context.

---

## 1. Environment Detection

Before running tests or CLI commands, determine the active execution context:

| Context | Detection Marker | Can Run `breeze`? | Test Execution Mode |
| :--- | :--- | :--- | :--- |
| **Breeze Container** | `BREEZE="true"` | **NO** (nested Docker fails) | Direct `pytest <path> -xvs` |
| **Devcontainer** | `CODESPACES="true"` / `REMOTE_CONTAINERS="true"` | Caution (requires Docker socket) | `uv run --project <PROJECT>` or `breeze` |
| **Host Machine** | Neither `BREEZE` nor container markers | **YES** | `uv run --project <PROJECT>` first, fallback to `breeze` |

You can inspect the environment programmatically at any time:

```bash
# Output JSON context and command recommendation for a target path
python scripts/tools/breeze_context.py --path airflow-core/tests/models/test_dag.py --json

# Quick check if running inside Breeze container (exit code 0 if in Breeze, 1 if not)
python scripts/tools/breeze_context.py --check-breeze
```

---

## 2. Test Execution Decision Tree

### When Inside the Breeze Container (`BREEZE="true"`)

All Airflow dependencies, system libraries, and databases are pre-installed in the container environment.

- **Run single test:** `pytest path/to/test.py::TestClass::test_method -xvs`
- **Run test file:** `pytest path/to/test.py -xvs`
- **Run without DB:** `pytest path/to/test.py --skip-db-tests -xvs`
- **Run Airflow CLI:** `airflow dags list`

> [!WARNING]
> **Never run `breeze` commands inside the Breeze container.**
> Running `breeze` inside Breeze attempts to start nested Docker containers and will fail.

---

### When on the Host Machine

Follow this hierarchy for test execution:

#### Step 1: Fast Path — Local Workspace Execution (`uv run`)

For unit tests that do not require external system C-libraries or running databases (Postgres/MySQL):

1. Identify the UV workspace member `<PROJECT>` corresponding to the code:
   - `airflow-core` for core engine (`airflow-core/tests/...`)
   - `task-sdk` for task SDK (`task-sdk/tests/...`)
   - `providers/<provider-name>` for providers (e.g., `providers/amazon`, `providers/google`, `providers/apache/beam`)
   - `scripts` for CI and development scripts (`scripts/tests/...`)
   - `airflow-ctl` for airflowctl CLI (`airflow-ctl/tests/...`)
   - `shared/<library>` for shared libraries (`shared/logging`, `shared/state`, etc.)

2. Execute using `uv run --project <PROJECT>`:

   ```bash
   uv run --project <PROJECT> pytest path/to/test.py -xvs
   uv run --project <PROJECT> pytest path/to/test.py::TestClass::test_method -xvs
   ```

> [!IMPORTANT]
> **Never run bare `pytest` on the host without `uv run --project <PROJECT>`.**
> Bare host pytest will either fail due to un-isolated virtual environments or test against the wrong dependencies.

---

#### Step 2: Fallback Path — Breeze Execution (`breeze run` / `breeze testing`)

Use Breeze when:

- Tests require database backends (Postgres, MySQL).
- Tests require native C-libraries or complex OS dependencies.
- Local `uv run` tests fail due to missing host dependencies.

- **Run single test or file through Breeze:**

  ```bash
  breeze run pytest path/to/test.py -xvs
  ```

- **Run test suite in parallel:**

  ```bash
  breeze testing <test_group> --run-in-parallel
  # Test groups: core-tests, providers-tests, task-sdk-tests, airflow-ctl-tests, helm-tests
  ```

- **Run DB tests only:**

  ```bash
  breeze testing <test_group> --run-db-tests-only --run-in-parallel
  ```

- **Run single provider full test suite:**

  ```bash
  breeze testing providers-tests --test-type "Providers[amazon]"
  ```

- **Run Airflow CLI command:**

  ```bash
  breeze run airflow dags list
  ```

---

## 3. Static Code Checks & Formatting

Maintain codebase quality using Airflow's pre-commit (`prek`) and formatting toolchain:

- **Format Python file:**

  ```bash
  uv run ruff format <file_path>
  ```

- **Lint Python file:**

  ```bash
  uv run ruff check --fix <file_path>
  ```

- **Run fast static checks on staged/modified files:**

  ```bash
  prek run --from-ref main --stage pre-commit
  ```

- **Type-check non-providers:**

  ```bash
  prek run mypy-<project> --all-files
  # Examples: prek run mypy-airflow-core --all-files, prek run mypy-task-sdk --all-files
  ```

- **Type-check providers:**

  ```bash
  breeze run mypy path/to/provider
  ```

---

## 4. Git Worktrees & Ephemeral Agent Environments

Airflow supports multi-worktree development. When developing in a secondary git worktree:

- Run `scripts/tools/setup_breeze` once to configure the worktree-aware Breeze shim.
- Each worktree runs Breeze using its own local sources (`dev/breeze`) and dependencies locked by `dev/breeze/uv.lock`.
- Temporary scripts should be placed in `dev/` (mounted as `/opt/airflow/dev/` in Breeze containers).

---

## 5. Verification & Selective Checks

Before finalizing a contribution or PR:

- **Verify changes matching CI matrix:**

  ```bash
  breeze verify
  # Add --json for structured output
  ```

- **Check which test suites CI will run:**

  ```bash
  breeze ci selective-check --commit-ref <commit_sha>
  ```
