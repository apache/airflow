 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# AGENTS instructions

## Environment Setup

- Install prek: `uv tool install prek`
- Enable commit hooks: `prek install`
- **Never run pytest, python, or airflow commands directly on the host** — always use `breeze`.
- Place temporary scripts in `dev/` (mounted as `/opt/airflow/dev/` inside Breeze).

## Commands

- **Run a single test:** `breeze run pytest path/to/test.py::TestClass::test_method -xvs`
- **Run a test file:** `breeze run pytest path/to/test.py -xvs`
- **Run a Python script:** `breeze run python dev/my_script.py`
- **Run Airflow CLI:** `breeze run airflow dags list`
- **Type-check:** `breeze run mypy path/to/code`
- **Lint/format (runs on host):** `prek run --all-files`
- **Lint with ruff only:** `prek run ruff --all-files`
- **Format with ruff only:** `prek run ruff-format --all-files`
- **Build docs:** `breeze build-docs`

SQLite is the default backend. Use `--backend postgres` or `--backend mysql` for integration tests that need those databases. If Docker networking fails, run `docker network prune`.

## Repository Structure

UV workspace monorepo. Key paths:

- `airflow-core/src/airflow/` — core scheduler, API, CLI, models
  - `models/` — SQLAlchemy models (DagModel, TaskInstance, DagRun, Asset, etc.)
  - `jobs/` — scheduler, triggerer, Dag processor runners
  - `api_fastapi/core_api/` — public REST API v2, UI endpoints
  - `api_fastapi/execution_api/` — task execution communication API
  - `dag_processing/` — Dag parsing and validation
  - `cli/` — command-line interface
  - `ui/` — React/TypeScript web interface (Vite)
- `task-sdk/` — lightweight SDK for Dag authoring and task execution runtime
  - `src/airflow/sdk/execution_time/` — task runner, supervisor
- `providers/` — 100+ provider packages, each with its own `pyproject.toml`
- `airflow-ctl/` — management CLI tool
- `chart/` — Helm chart for Kubernetes deployment

## Architecture Boundaries

1. Users author Dags with the Task SDK (`airflow.sdk`).
2. Dag Processor parses Dag files in isolated processes and stores serialized Dags in the metadata DB.
3. Scheduler reads serialized Dags — **never runs user code** — and creates Dag runs / task instances.
4. Workers execute tasks via Task SDK and communicate with the API server through the Execution API — **never access the metadata DB directly**.
5. API Server serves the React UI and handles all client-database interactions.
6. Triggerer evaluates deferred tasks/sensors in isolated processes.

## Coding Standards

- No `assert` in production code.
- `time.monotonic()` for durations, not `time.time()`.
- In `airflow-core`, functions with a `session` parameter must not call `session.commit()`. Use keyword-only `session` parameters.
- Imports at top of file. Valid exceptions: circular imports, lazy loading for worker isolation, `TYPE_CHECKING` blocks.
- Guard heavy type-only imports (e.g., `kubernetes.client`) with `TYPE_CHECKING` in multi-process code paths.
- Apache License header on all new files (prek enforces this).

## Testing Standards

- Add tests for new behavior — cover success, failure, and edge cases.
- Use pytest patterns, not `unittest.TestCase`.
- Use `spec`/`autospec` when mocking.
- Use `time_machine` for time-dependent tests.
- Use `@pytest.mark.parametrize` for multiple similar inputs.
- Test fixtures: `devel-common/src/tests_common/pytest_plugin.py`.
- Test location mirrors source: `airflow/cli/cli_parser.py` → `tests/cli/test_cli_parser.py`.

## Commits and PRs

Write commit messages focused on user impact, not implementation details.

- **Good:** `Fix airflow dags test command failure without serialized Dags`
- **Good:** `UI: Fix Grid view not refreshing after task actions`
- **Bad:** `Initialize DAG bundles in CLI get_dag function`

Add a newsfragment for user-visible changes:
`echo "Brief description" > airflow-core/newsfragments/{PR_NUMBER}.{bugfix|feature|improvement|doc|misc|significant}.rst`

## Boundaries

- **Ask first**
  - Large cross-package refactors.
  - New dependencies with broad impact.
  - Destructive data or migration changes.
- **Never**
  - Commit secrets, credentials, or tokens.
  - Edit generated files by hand when a generation workflow exists.
  - Use destructive git operations unless explicitly requested.

## References

- [`contributing-docs/03a_contributors_quick_start_beginners.rst`](contributing-docs/03a_contributors_quick_start_beginners.rst)
- [`contributing-docs/05_pull_requests.rst`](contributing-docs/05_pull_requests.rst)
- [`contributing-docs/07_local_virtualenv.rst`](contributing-docs/07_local_virtualenv.rst)
- [`contributing-docs/08_static_code_checks.rst`](contributing-docs/08_static_code_checks.rst)
- [`contributing-docs/12_provider_distributions.rst`](contributing-docs/12_provider_distributions.rst)
- [`contributing-docs/19_execution_api_versioning.rst`](contributing-docs/19_execution_api_versioning.rst)
