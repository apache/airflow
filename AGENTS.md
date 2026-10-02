 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# AGENTS instructions

## Naming

Write **Dag** (title case) in all prose. Keep the all-caps or lowercase
spelling only when reproducing a literal code token — never rewrite these,
even inside fenced code blocks:

- Python: the SDK class `DAG` (`from airflow.sdk import DAG`,
  `dag = DAG("my_dag", ...)`); identifiers like `dag_id`, `dag`, `my_dag`.
- CLI: `airflow dags list`, `airflow dags test`, etc.
- Paths and config keys: `dag_processing/`, `dagprocessor`, `get_dag`, etc.
- Anti-pattern quotes that show the wrong form to teach the rule itself
  (e.g., `Use "DAG" — always write "Dag"`).

Don't spell out **Directed Acyclic Graph** except for historical context.

## Environment Setup

- Install prek: `uv tool install prek`
- Enable commit hooks: `prek install`
- Install breeze shim (one-time, per machine): `scripts/tools/setup_breeze` — installs `~/.local/bin/breeze` that runs breeze via `uv run --locked` from the current git worktree's `dev/breeze`, with dependencies pinned by `dev/breeze/uv.lock` (so each worktree, including ephemeral agent worktrees, gets its own breeze tied to its sources). See [ADR 0017](dev/breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md).
- **Never run pytest, python, or airflow commands directly on the host** — always use `breeze`.
- Place temporary scripts in `dev/` (mounted as `/opt/airflow/dev/` inside Breeze).

## Commands

`<PROJECT>` is folder where pyproject.toml of the package you want to test is located. For example, `airflow-core` or `providers/amazon`.
`<target_branch>` is the branch the PR will be merged into — usually `main`, but could be `v3-1-test` when creating a PR for the 3.1 branch.

<!-- START generated-commands, please keep comment here to allow auto update -->
- **Run a single test:** `uv run --project <PROJECT> pytest path/to/test.py::TestClass::test_method -xvs`
- **Run a test file:** `uv run --project <PROJECT> pytest path/to/test.py -xvs`
- **Run all tests in package:** `uv run --project <PROJECT> pytest path/to/package -xvs`
- **If uv tests fail with missing system dependencies, run the tests with breeze**: `breeze run pytest <tests> -xvs`
- **Run a Python script:** `uv run --project <PROJECT> python dev/my_script.py`
- **Run core or provider tests suite in parallel:** `breeze testing <test_group> --run-in-parallel` (test groups: `core-tests`, `providers-tests`)
- **Run core or provider db tests suite in parallel:** `breeze testing <test_group> --run-db-tests-only --run-in-parallel` (test groups: `core-tests`, `providers-tests`)
- **Run core or provider non-db tests suite in parallel:** `breeze testing <test_group> --skip-db-tests --use-xdist` (test groups: `core-tests`, `providers-tests`)
- **Run single provider complete test suite:** `breeze testing providers-tests --test-type "Providers[PROVIDERS_LIST]"` (e.g., `Providers[google]` or `Providers[amazon]` or "Providers[amazon,google]")
- **Run Helm tests in parallel with xdist** `breeze testing helm-tests --use-xdist`
- **Run Helm tests with specific K8s version:** `breeze testing helm-tests --use-xdist --kubernetes-version 1.35.0`
- **Run specific Helm test type:** `breeze testing helm-tests --use-xdist --test-type <type>` (types: `airflow_aux`, `airflow_core`, `apiserver`, `dagprocessor`, `other`, `redis`, `security`, `statsd`, `webserver`)
- **Run other suites of tests** `breeze testing <test_group>` (test groups: `airflow-ctl-tests`, `docker-compose-tests`, `task-sdk-tests`)
- **Run scripts tests:** `uv run --project scripts pytest scripts/tests/ -xvs`
- **Run Airflow CLI:** `breeze run airflow dags list`
- **Type-check (non-providers):** run the prek hook — `prek run mypy-<project> --all-files` (e.g. `mypy-airflow-core`, `mypy-task-sdk`, `mypy-shared-logging`; each `shared/<dist>` workspace member has its own `mypy-shared-<dist>` hook). The hook uses a dedicated virtualenv and mypy cache under `.build/mypy-venvs/<hook>/` and `.build/mypy-caches/<hook>/`; mypy itself is installed from `uv.lock` via the `mypy` dependency group (`uv sync --group mypy`), so it never mutates your project `.venv`. The hook prefers `uv` from the project's main `.venv/bin/uv` (installed by `uv sync` — `uv` is part of the `dev` dependency group via the `all` extras) for a project-pinned uv version; it falls back to `uv` on `$PATH` with a warning if that binary is missing. Clear with `breeze down --cleanup-mypy-cache`.
- **Type-check (providers):** `breeze run mypy path/to/code`
- **Lint with ruff only:** `prek run ruff --from-ref <target_branch>`
- **Format with ruff only:** `prek run ruff-format --from-ref <target_branch>`
- **Run regular (fast) static checks:** `prek run --from-ref <target_branch> --stage pre-commit`
- **Run manual (slower) checks:** `prek run --from-ref <target_branch> --stage manual --skip compile-ui-assets-dev --skip view-skill-eval --skip run-skill-eval-codex` (the skipped hooks start long-running local servers or provision the opt-in Codex environment rather than run checks that complete)
- **Build docs:** `breeze build-docs`
- **Determine which tests to run based on changed files:** `breeze ci selective-check --commit-ref <commit_with_squashed_changes>`
<!-- END generated-commands, please keep comment here to allow auto update -->

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
- `dev/` — development utilities and scripts used to bootstrap the environment, releases, breeze dev env
- `scripts/` — utility scripts for CI, Docker, and prek hooks (workspace distribution `apache-airflow-scripts`)
  - `ci/prek/` — prek (pre-commit) hook scripts; shared utilities in `common_prek_utils.py`
  - `tests/` — pytest tests for the scripts; run with `uv run --project scripts pytest scripts/tests/`

The `uv.lock` file is generated by `uv lock`, `uv sync` and is committed to the repo - it contains snapshot of
versions of all dependencies used for development of Airflow. If at any point in time you have a conflict
with `uv.lock`, simply delete it and run `uv lock` to regenerate it.


## Architecture Boundaries

1. Users author Dags with the Task SDK (`airflow.sdk`).
2. Dag File Processor parses Dag files in separate processes and stores serialized Dags in the metadata DB. Software guards prevent individual parsing processes from accessing the database directly and enforce use of the Execution API, but these guards do not protect against intentional bypassing by malicious or misconfigured code.
3. Scheduler reads serialized Dags — **never runs user code** — and creates Dag runs / task instances.
4. Workers execute tasks via Task SDK and communicate with the API server through the Execution API — **never access the metadata DB directly**. Each task receives a short-lived JWT token scoped to its task instance ID.
5. API Server serves the React UI and handles all client-database interactions.
6. Triggerer evaluates deferred tasks/sensors in separate processes. Like the Dag File Processor, software guards steer it through the Execution API rather than direct database access, but these guards do not protect against intentional bypassing by malicious or misconfigured code.
7. Shared libraries that are symbolically linked to different Python distributions are in `shared` folder.
8. Airflow uses `uv workspace` feature to keep all the distributions sharing dependencies and venv
9. Each of the distributions should declare other needed distributions: `uv --project <FOLDER> sync` command acts on the selected project in the monorepo with only dependencies that it has

## Security Model

When reviewing code, writing security documentation, or performing security research, keep in
mind the following aspects of Airflow's security model. The authoritative reference is
[`airflow-core/docs/security/security_model.rst`](airflow-core/docs/security/security_model.rst)
and [`airflow-core/docs/security/jwt_token_authentication.rst`](airflow-core/docs/security/jwt_token_authentication.rst).

Particularly, the intentional design choices, that are not security vulnerabilities and should not be
reported as such are described in "What is NOT considered a security vulnerability" chapter of the security model.

**When flagging security concerns, distinguish between:**

1. **Actual vulnerabilities** — code that violates the documented security model (e.g., a worker
   gaining database access it shouldn't have, a Scheduler executing user code, an unauthenticated
   user accessing protected endpoints).
2. **Known limitations** — documented gaps where the current implementation doesn't provide full
   isolation (e.g., DFP/Triggerer database access, shared Execution API resources, multi-team
   not enforcing task-level isolation). These are tracked for improvement in future versions and
   should not be reported as new findings.
3. **Deployment hardening opportunities** — measures a Deployment Manager can take to improve
   isolation beyond what Airflow enforces natively (e.g., per-component configuration, asymmetric
   JWT keys, network policies). These belong in deployment guidance, not as code-level issues.

# Shared libraries

- shared libraries provide implementation of some common utilities like logging, configuration where the code should be reused in different distributions (potentially in different versions)
- we have a number of shared libraries that are separate, small Python distributions located under `shared` folder
- each of the libraries has it's own src, tests, pyproject.toml and dependencies
- sources of those libraries are symbolically linked to the distributions that are using them (`airflow-core`, `task-sdk` for example)
- tests for the libraries (internal) are in the shared distribution's test and can be run from the shared distributions
- tests of the consumers using the shared libraries are present in the distributions that use the libraries and can be run from there

## Coding Standards

- **Always format and check Python files with ruff immediately after writing or editing them:** `uv run ruff format <file_path>` and `uv run ruff check --fix <file_path>`. Do this for every Python file you create or modify, before moving on to the next step.
- No `assert` in production code.
- **Comment only when context is not readily apparent from the code.**
  Generic purpose explanation do not qualify: `# Log for debugging` above `logging.debug(...)`,
  `# Validate for safety`, `# Retry for reliability` and similar provide no useful information.
  Useful comments record a specific constraint, invariant, compatibility quirk, or tradeoff.
  Before adding a comment, identify the misunderstanding or incorrect change it would prevent.
  If removing a comment will lose non-code-related context, omit it. Do not narrate code, repeat code,
  or invent a rationale in comments. Keep necessary explanations short, precise and at the source of truth.
  Do not repeat comments at each call site. Judge each comment value by the preserved context only.
- `time.monotonic()` for durations, not `time.time()`.
- In `airflow-core`, functions with a `session` parameter must not call `session.commit()`. Use keyword-only `session` parameters.
- Imports at top of file. Valid exceptions: circular imports, lazy loading for worker isolation, `TYPE_CHECKING` blocks.
- Guard heavy type-only imports (e.g., `kubernetes.client`) with `TYPE_CHECKING` in multi-process code paths.
- Define dedicated exception classes or use existing exceptions such as `ValueError` instead of raising the broad `AirflowException` directly. Each error case should have a specific exception type that conveys what went wrong. **Never add new direct `raise AirflowException(...)` usages — the community is actively reducing them, not adding more, and the `check-no-new-airflow-exceptions` prek hook enforces this across `airflow-core`, `airflow-ctl`, `task-sdk`, `providers`, and `shared`.** Prefer a Python built-in (`ValueError`, `TypeError`, `OSError`, …) or a dedicated class in the appropriate `exceptions.py`. The only acceptable way an `AirflowException` line may move is relocating an already-existing one verbatim during a refactor (e.g. moving code between files) — that is not a new usage. When you touch code that already raises `AirflowException`, prefer narrowing it to a more specific exception rather than leaving or duplicating it.
- Translate domain-layer exceptions to `HTTPException` at FastAPI route boundaries. In `airflow-core/src/airflow/api_fastapi/core_api/` route handlers, catch errors raised by domain code (e.g., `ValueError` from `airflow.state.metastore.MetastoreBackend` for a missing row or invalid input) and re-raise as `HTTPException` with the right status (`404` for not-found, `400` for invalid input). Otherwise they propagate as `500 Internal Server Error`, leaking internals and misleading clients.
- Bulk `DELETE`/`UPDATE` in the scheduler loop or any synchronous interval task (e.g. `call_regular_interval` callbacks) must be batched with `LIMIT` and committed between batches — never issue a single unbounded bulk write against a user-driven table. Unbounded bulk writes hold row locks for the entire transaction (blocking concurrent writers) and stall the scheduler main loop. Filter columns used by the cleanup must be indexed. Follow the batching pattern in `airflow-core/src/airflow/utils/db_cleanup.py`.
- Name functions and methods with action verbs: `get_`, `extract_`, `find_`, `compute_`, `build_`, etc. Avoid noun-only names like `_serialize_keys` or `_base_names` — they read as attributes, not callables. Predicates (`is_`, `has_`) are the one exception.
- Apache License header on all new files (prek enforces this).
- **Keep selective-checks behaviour and its documentation in sync.** The CI optimisation logic lives in `dev/breeze/src/airflow_breeze/utils/selective_checks.py` (run-mode decisions, file-group matching, test-type selection, prek-hook skipping). Whenever you change a rule there — add/rename a file group, change what forces `full_tests_needed`/`all_versions`, alter how providers or test types are selected, or change which prek hooks are skipped — update [`dev/breeze/doc/ci/04_selective_checks.md`](dev/breeze/doc/ci/04_selective_checks.md) in the same PR (the decision-rules list, the diagrams, the outputs table, and the worked examples as applicable) and add/adjust tests in `dev/breeze/tests/test_selective_checks.py`. The doc is the human-facing explanation of that file; letting them drift makes CI behaviour impossible to reason about.
- Newsfragments are only used by distributions whose release process consumes them via towncrier — currently `airflow-core/newsfragments/`, `chart/newsfragments/`, and `dev/mypy/newsfragments/` — and only for major or breaking changes. **Golden rule: never create a newsfragment unless you are certain the change is user-facing.** If you are not sure the change is visible to users — build/release tooling, CI, packaging, internal refactors with no behavior change, dev-only scripts, and test-only changes are *not* user-facing — do **not** add one. Default to omitting it; a maintainer will ask for a newsfragment during review if the change warrants one. Adding a spurious newsfragment for a non-user-facing change is a defect, not a safe default. **Never add newsfragments for `providers/` or `airflow-ctl/`** — those distributions are released from `main` and their release managers regenerate the changelog from `git log`, so per-PR newsfragments are not consumed (see `dev/README_RELEASE_PROVIDERS.md` and `dev/README_RELEASE_AIRFLOWCTL.md`). For a user-visible note in those distributions, edit the changelog directly: `providers/<provider>/docs/changelog.rst` for providers, `airflow-ctl/RELEASE_NOTES.rst` for airflow-ctl. Changes to `task-sdk/` ship in `airflow-core` — use `airflow-core/newsfragments/`.

## Testing Standards

- Target exactly 100% coverage of what the PR changes — no more, no less. Every changed or added behaviour must have a test; every test must fail without the PR's change. Do not add tests for pre-existing logic that was already present before the PR, and do not test standard-library or third-party functions. The exception is deliberate behaviour or integration tests, which may cross those boundaries by design.
- Use pytest patterns, not `unittest.TestCase`.
- Use `spec`/`autospec` when mocking.
- Prefer `@mock.patch` decorators over `with mock.patch(...)` context managers for patching. Use `conf_vars` (from `tests_common.test_utils.config`) for Airflow config overrides — as a decorator when the value is fixed, as a context manager when it varies via `@pytest.mark.parametrize`.
- Use `time_machine` for time-dependent tests. Do not use `datetime.now()`
- Use `@pytest.mark.parametrize` for multiple similar inputs — consolidate tests that only differ in input/expected values into a single parametrized test.
- Use `@pytest.mark.db_test` for tests that require database access.
- Test fixtures: `devel-common/src/tests_common/pytest_plugin.py`.
- Test location mirrors source: `airflow/cli/cli_parser.py` → `tests/cli/test_cli_parser.py`.
- Do not assert on raw log text (`caplog.text` for example), these are legacy string matching APIs planned for removal. Structured assertions via `caplog` (which resolves to `cap_structlog` under structlog) are fine and preferred: `"event name" in caplog` or `{"event": ..., "field": ...} in caplog`.

## Output conventions

- Put any files you generate (PR reviews, reports, scratch output) under `files/`.
- Create `files/` if it doesn't exist.


## Commits and PRs

Use the contribution skills below when available. If the runtime does not support
skill discovery, read the linked `SKILL.md` directly. If either skill is
unavailable, do not publish, push, create or modify GitHub issues or pull
requests, or rename or add Git remotes. You may draft text or propose commands,
but must state that the relevant skill could not be read and request confirmation
before any remote-changing action.

- [Draft contribution text](.agents/skills/airflow-pr-draft-summary/SKILL.md):
  commit messages, PR titles/descriptions, release notes and GitHub messages.
- [Publish changes](.agents/skills/airflow-publish-changes/SKILL.md):
  existing PR checks, remotes, pre-push review, publication and deferred-work issues.

Before working on an issue, check for existing PRs. Push only to the user's fork,
never upstream or `main`. Never list an agent as a commit co-author. Retain the
required Gen-AI disclosure and GitHub-message attribution; do not tag individuals
outside the exceptions in the message guidance. Drafting does not authorize posting.

## apache-magpie framework

This repo uses the [`apache/magpie`](https://github.com/apache/magpie)
framework, installed from its plugin marketplace. The framework provides
the `pr-management-*` skills (triage, code-review, stats, mentor) among
others. Nothing framework-related is committed here — install it in your
own agent harness. In Claude Code:

```text
/plugin marketplace add apache/magpie
/plugin install magpie-pr-management@apache-magpie
```

`magpie@apache-magpie` installs every family at once; other families
(`magpie-security`, `magpie-release-management`, …) install individually.
`magpie-release-management` is in the project floor (`.apache-magpie.lock`):
its `verify-rc` skill verifies a release candidate and, per
[`.apache-magpie-overrides/release-verify-rc.md`](.apache-magpie-overrides/release-verify-rc.md),
can test your own changes against a providers wave.
The contributor-facing summary lives in the [Agent-assisted contribution
section of `README.md`](README.md#agent-assisted-contribution-apache-magpie).

Airflow-specific modifications to framework-skill workflows live in
[`.apache-magpie-overrides/`](.apache-magpie-overrides/) — the installed
plugin reads them at run time. Never edit the installed plugin itself;
framework changes go via PR to
[`apache/magpie`](https://github.com/apache/magpie).

### Reviewing pull requests

With the `magpie-pr-management` plugin installed, use the
`magpie-pr-management:pr-management-code-review` skill for PR code review.
It posts findings as **inline review comments** anchored to `file:line`,
presented **individually for accept/skip** before anything is submitted —
prefer it over an ad-hoc review pass or a generic review command. A
body-only review is the explicit opt-out (`inline:off`).

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
