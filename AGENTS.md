 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# AGENTS instructions

## Environment Setup

- Install prek: `uv tool install prek`
- Enable commit hooks: `prek install`
- Install breeze shim (one-time, per machine): `scripts/tools/setup_breeze` — installs `~/.local/bin/breeze` that runs breeze via `uvx` from the current git worktree's `dev/breeze` (so each worktree, including ephemeral agent worktrees, gets its own breeze tied to its sources). See [ADR 0017](dev/breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md).
- **Never run pytest, python, or airflow commands directly on the host** — always use `breeze`.
- Place temporary scripts in `dev/` (mounted as `/opt/airflow/dev/` inside Breeze).

## Commands

`<PROJECT>` is folder where pyproject.toml of the package you want to test is located. For example, `airflow-core` or `providers/amazon`.
`<target_branch>` is the branch the PR will be merged into — usually `main`, but could be `v3-1-test` when creating a PR for the 3.1 branch.

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
- **Run manual (slower) checks:** `prek run --from-ref <target_branch> --stage manual`
- **Build docs:** `breeze build-docs`
- **Determine which tests to run based on changed files:** `breeze selective-checks --commit-ref <commit_with_squashed_changes>`

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
- `time.monotonic()` for durations, not `time.time()`.
- In `airflow-core`, functions with a `session` parameter must not call `session.commit()`. Use keyword-only `session` parameters.
- Imports at top of file. Valid exceptions: circular imports, lazy loading for worker isolation, `TYPE_CHECKING` blocks.
- Guard heavy type-only imports (e.g., `kubernetes.client`) with `TYPE_CHECKING` in multi-process code paths.
- Define dedicated exception classes or use existing exceptions such as `ValueError` instead of raising the broad `AirflowException` directly. Each error case should have a specific exception type that conveys what went wrong.
- Apache License header on all new files (prek enforces this).
- Newsfragments are only added if a major change or breaking change is applied. This is usually coordinate during review. Please do not add newsfragments per default as in most cases this needs a reversion during review.

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

- NEVER add Co-Authored-By with yourself as co-author of the commit. Agents cannot be authors, humans can be, Agents are assistants.

### Git remote naming conventions

Airflow standardises on two git remote names, and the rest of this file, the
contributing docs, and the release docs all assume them:

- **`upstream`** — the canonical `apache/airflow` repository (fetch from here).
- **`origin`** — the contributor's fork of `apache/airflow` (push PR branches here).

Always push branches to `origin`. Never push directly to `upstream` (and never
push directly to `main` on either remote).

**Before running any remote-based command, run `git remote -v` and verify the
names match this convention.** If they do not — for example, the upstream remote
is called `apache`, or `origin` points at `apache/airflow` with the fork under a
different name like `fork` — **do not silently go along with the existing
names**. Surface the mismatch to the user and propose the exact rename commands
to bring the checkout in line with the convention, then ask the user to confirm
before running them. Examples:

- Upstream is named `apache`, fork is `origin` (common legacy layout):

  ```bash
  git remote rename apache upstream
  ```

- `origin` points at `apache/airflow` and the fork is named `fork` (release-manager
  / "cloned upstream directly" layout):

  ```bash
  git remote rename origin upstream
  git remote rename fork origin
  ```

- Upstream is missing entirely:

  ```bash
  git remote add upstream https://github.com/apache/airflow.git
  # or, for SSH:
  git remote add upstream git@github.com:apache/airflow.git
  ```

- Fork is missing entirely:

  ```bash
  gh repo fork apache/airflow --remote --remote-name origin
  ```

After any rename/add, re-run `git remote -v` to confirm the new state before
continuing with commands that assume `upstream` / `origin`.

If a doc, script, or command you're about to run uses the old `apache` name (or
any other variant), **translate it to the `upstream` convention** in what you
propose to the user, rather than perpetuating the old name. Flag the stale
documentation so it can be fixed in a follow-up.

### Creating Pull Requests

**Always push to the user's fork (`origin`)**, not to `upstream` (`apache/airflow`).
Never push directly to `main`.

Before pushing, confirm the remote setup matches the conventions above
(`upstream` → `apache/airflow`, `origin` → your fork). Run `git remote -v` and,
if the names don't match, propose renames as described in "Git remote naming
conventions" — ask the user to confirm before running them.

If the fork remote does not exist at all, create one:

```bash
gh repo fork apache/airflow --remote --remote-name origin
```

Before pushing, perform a self-review of your changes following the Gen-AI review guidelines
in [`contributing-docs/05_pull_requests.rst`](contributing-docs/05_pull_requests.rst) and the
code review checklist in [`.github/instructions/code-review.instructions.md`](.github/instructions/code-review.instructions.md):

1. Review the full diff (`git diff main...HEAD`) and verify every change is intentional and
   related to the task — remove any unrelated changes.
2. Read `.github/instructions/code-review.instructions.md` and check your diff against every
   rule — architecture boundaries, database correctness, code quality, testing requirements,
   API correctness, and AI-generated code signals. Fix any violations before pushing.
3. Confirm the code follows the project's coding standards and architecture boundaries
   described in this file.
4. Run regular (fast) static checks (`prek run --from-ref <target_branch> --stage pre-commit`)
   and fix any failures. This includes mypy checks for non-provider projects (airflow-core, task-sdk, airflow-ctl, dev, scripts, devel-common).
5. Run manual (slower) checks (`prek run --from-ref <target_branch> --stage manual`) and fix any failures.
6. Run relevant individual tests and confirm they pass.
7. Find which tests to run for the changes with selective-checks and run those tests in parallel to confirm they pass and check for CI-specific issues.
8. Check for security issues — no secrets, no injection vulnerabilities, no unsafe patterns.

Before pushing, always rebase your branch onto the latest target branch (usually `main`)
to avoid merge conflicts and ensure CI runs against up-to-date code:

```bash
git fetch upstream <target_branch>
git rebase upstream/<target_branch>
```

If there are conflicts, resolve them and continue the rebase. If the rebase is too complex,
ask the user for guidance.

Then push the branch to your fork (`origin`) and open the PR creation page in the browser
with the body pre-filled (including the generative AI disclosure already checked):

```bash
git push -u origin <branch-name>
gh pr create --web --title "Short title (under 70 chars)" --body "$(cat <<'EOF'
Brief description of the changes.

closes: #ISSUE  (if applicable)

---

##### Was generative AI tooling used to co-author this PR?

- [X] Yes — <Agent Name and Version>

Generated-by: <Agent Name and Version> following [the guidelines](https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#gen-ai-assisted-contributions)

EOF
)"
```

The `--web` flag opens the browser so the user can review and submit. The `--body` flag
pre-fills the PR template with the generative AI disclosure already completed.

Remind the user to:

1. Review the PR title — keep it short (under 70 chars) and focused on user impact.
2. Add a brief description of the changes at the top of the body.
3. Reference related issues when applicable (`closes: #ISSUE` or `related: #ISSUE`).

### Tracking issues for deferred work

When a PR applies a **workaround, version cap, mitigation, or partial fix**
rather than solving the underlying problem (for example: upper-binding a
dependency to avoid a breaking upstream release, disabling a feature
behind a flag, reverting a change that needs a better replacement, or
papering over a bug so a release can ship), the deferred work must be
captured in a GitHub tracking issue **and** the tracking issue URL must
appear as a comment at the workaround site in the code.

1. **Open the tracking issue first**, before finalising the PR body.
2. **Reference it in the PR body by number** — e.g. "full migration is
   tracked in #65609" — so anyone reviewing the PR can see what was
   deferred and why.
3. **Add a link to the tracking issue as a comment at the workaround
   itself**, so the reference survives after the PR merges and anyone
   reading the source later can click straight through to the follow-up
   work. Use the **full issue URL**, not bare `#NNNNN` — bare references
   do not auto-link outside GitHub's web UI (e.g. when grepping in an
   editor, browsing a checkout, or reading the file in a terminal).
   For example:

   ```toml
   # pyproject.toml
   # Remove the <1.0 cap after migrating to httpx 1.x;
   # tracked at https://github.com/apache/airflow/issues/65609
   "httpx>=0.27.0,<1.0",
   ```

   ```python
   # some_module.py
   # Delete this fallback once the new client is on all workers;
   # tracked at https://github.com/apache/airflow/issues/65609
   if old_client:
       ...
   ```

4. **Do not** write vague forward-looking phrases like "will open a
   tracking issue" or "to be filed later" in the PR body or in code
   comments. Open the issue, link it in both places, then submit the PR.
5. The tracking issue should describe: what the workaround is, why it
   was chosen, the concrete follow-up work needed, and any acceptance
   criteria for removing the workaround.

If a PR you already opened has such forward-looking language, open the
tracking issue, add a PR comment referencing the issue URL, and push a
follow-up commit that adds the tracking-issue URL as a comment at the
workaround site in the code.

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
