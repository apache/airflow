<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# Project Rules for Code Modifications

Load this before making any code change, commit, or PR in the Airflow repository.

## Repository layout

Airflow is a **uv workspace monorepo**. Key projects:

| Project | Path | pyproject.toml |
|---|---|---|
| Airflow Core | `airflow-core/` | `airflow-core/pyproject.toml` |
| Task SDK | `task-sdk/` | `task-sdk/pyproject.toml` |
| Airflow CTL | `airflow-ctl/` | `airflow-ctl/pyproject.toml` |
| Breeze (dev tool) | `dev/breeze/` | `dev/breeze/pyproject.toml` |
| Scripts | `scripts/` | `scripts/pyproject.toml` |
| Providers | `providers/<name>/` | `providers/<name>/pyproject.toml` |
| Helm Chart | `chart/` | — |

Always pass the correct `--project` when running `uv run` or `pytest`.

## Shared libraries

- shared libraries provide implementation of some common utilities like logging, configuration where the code should be reused in different distributions (potentially in different versions)
- we have a number of shared libraries that are separate, small Python distributions located under `shared` folder
- each of the libraries has it's own src, tests, pyproject.toml and dependencies
- sources of those libraries are symbolically linked to the distributions that are using them (`airflow-core`, `task-sdk` for example)
- tests for the libraries (internal) are in the shared distribution's test and can be run from the shared distributions
- tests of the consumers using the shared libraries are present in the distributions that use the libraries and can be run from there

## Host vs Breeze boundary

This is the most common source of agent errors.

| Operation | Where to run | Why |
|---|---|---|
| `git add`, `git commit`, `git push` | **Host only** | `.git` is not mounted inside Breeze |
| `pytest` (unit tests) | Host (`uv run`) or Breeze (`breeze run`) | System deps may require Breeze |
| `prek` (static checks) | Either | Works identically in both |
| `breeze shell`, `breeze run` | **Host only** (launches Breeze) | These are host CLI commands |

**Detection:** `BREEZE` environment variable is set → you are inside Breeze. Otherwise → host.

## DevContainers and Worktrees

- **DevContainers:** When running inside a devcontainer, the environment is typically pre-configured with all system dependencies and the `.git` directory is mounted. In this context, the `BREEZE` environment variable is **not** set. Agents should treat DevContainers as a "Host" environment where `git` and `uv` commands work directly without needing Breeze.
- **Git Worktrees:** Agents fully support git worktrees. When working in a worktree, the `scripts/tools/setup_breeze` command installs a shim that correctly routes `breeze` commands to the local source tree, ensuring that Breeze always runs the code from the current worktree.

## Commands

`<PROJECT>` is the folder containing the relevant `pyproject.toml`, e.g. `airflow-core`, `providers/amazon`, `task-sdk`, `scripts`.
`<target_branch>` is the branch the PR targets — usually `main`.

- **Run a single test:** `uv run --project <PROJECT> pytest path/to/test.py::TestClass::test_method -xvs`
- **Run a test file:** `uv run --project <PROJECT> pytest path/to/test.py -xvs`
- **Run all tests in package:** `uv run --project <PROJECT> pytest path/to/package -xvs`
- **If uv fails due to missing system deps, fall back to Breeze:** `breeze run pytest <tests> -xvs`
- **Run scripts tests:** `uv run --project scripts pytest scripts/tests/ -xvs`
- **Run core or provider tests in parallel:** `breeze testing <test_group> --run-in-parallel` (groups: `core-tests`, `providers-tests`)
- **Run Airflow CLI:** `breeze run airflow dags list`
- **Type-check (non-providers):** `prek run mypy-<project> --all-files`
- **Type-check (providers):** `breeze run mypy path/to/code`
- **Static checks:** `prek run --from-ref <target_branch> --stage pre-commit`
- **Manual checks:** `prek run --from-ref <target_branch> --stage manual`
- **Build docs:** `breeze build-docs`
- **Find impacted tests:** `breeze selective-checks --commit-ref <commit_with_squashed_changes>`

SQLite is the default backend. Use `--backend postgres` or `--backend mysql` for integration tests that need those databases.

## Security Model

The authoritative reference is `airflow-core/docs/security/security_model.rst`.

**Distinguish between:**

1. **Actual vulnerabilities** — code violating the documented model (e.g., worker gaining DB access).
2. **Known limitations** — documented gaps tracked for future improvement (e.g., DFP/Triggerer DB access).
3. **Deployment hardening** — measures for Deployment Managers, not code-level issues.

## Coding standards

- **Always format and check Python files with ruff immediately after writing or editing them:** `uv run ruff format <file_path>` and `uv run ruff check --fix <file_path>`. Do this for every Python file you create or modify, before moving on to the next step.
- **No `assert` in production**: Use standard exceptions instead.
- **Exceptions**: Define dedicated exception classes or use existing exceptions such as `ValueError` instead of raising the broad `AirflowException` directly.
- **Session handling**: In `airflow-core`, functions with a `session` parameter must not call `session.commit()`. Use keyword-only `session` parameters.
- **Imports**: Imports at top of file. Valid exceptions: circular imports, lazy loading for worker isolation, `TYPE_CHECKING` blocks.
- **Duration**: Use `time.monotonic()`, not `time.time()`.
- **Operators**: Only assign fields in `__init__`. Validate in `execute()`.

## Testing Standards

- Add tests for new behavior — cover success, failure, and edge cases.
- Use pytest patterns, not `unittest.TestCase`.
- Use `spec`/`autospec` when mocking.
- Use `time_machine` for time-dependent tests. Do not use `datetime.now()`
- Use `@pytest.mark.parametrize` for multiple similar inputs.
- Use `@pytest.mark.db_test` for tests that require database access.
- Test fixtures: `devel-common/src/tests_common/pytest_plugin.py`.
- Test location mirrors source: `airflow/cli/cli_parser.py` → `tests/cli/test_cli_parser.py`.
- Do not use `caplog` in tests, prefer checking logic and not log output.

## Commits and PRs

Write commit messages focused on user impact, not implementation details.

- **Good:** `Fix airflow dags test command failure without serialized Dags`
- **Good:** `UI: Fix Grid view not refreshing after task actions`
- **Bad:** `Initialize DAG bundles in CLI get_dag function`

Add a newsfragment for user-visible changes:
`echo "Brief description" > airflow-core/newsfragments/{PR_NUMBER}.{bugfix|feature|improvement|doc|misc|significant}.rst`

- **AI Disclosure:** If generative AI was used, include a disclosure in the PR body using the project template.
- **Attribution:** Any agent-drafted messages posted to GitHub (comments, reviews) must end with an attribution footer:

  ```
  ---
  Drafted-by: <Agent Name and Version> (no human review before posting)
  ```

- **Co-Authored-By:** NEVER add `Co-Authored-By` with yourself as co-author. Agents are assistants, not authors.

### Git remote naming conventions

Airflow standardises on two git remote names: **`upstream`** (canonical repo) and **`origin`** (your fork). Always push branches to `origin`.

### Tracking issues for deferred work

- When applying a workaround, version cap, mitigation, or partial fix rather than solving the underlying problem (e.g., upper-binding a dependency to avoid a breaking upstream release), the deferred work must be captured in a GitHub tracking issue.
- **Link in Code:** The tracking issue URL must appear as a comment at the workaround site in the code. Use the full issue URL (e.g., `tracked at https://github.com/apache/airflow/issues/65609`).
- **PR Description:** Reference the issue in the PR body (e.g., "full migration is tracked in #65609").


## Boundaries

- **Ask First**: Large refactors, new dependencies, destructive migrations.
- **Never**: Commit secrets, edit generated files manually, or use destructive git operations unless asked.

## References

- `contributing-docs/03a_contributors_quick_start_beginners.rst`
- `contributing-docs/05_pull_requests.rst`
- `contributing-docs/07_local_virtualenv.rst`
- `contributing-docs/08_static_code_checks.rst`
- `contributing-docs/12_provider_distributions.rst`
- `contributing-docs/19_execution_api_versioning.rst`
