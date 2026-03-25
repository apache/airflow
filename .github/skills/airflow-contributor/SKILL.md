---
name: airflow-contributor
description: >
  Full contributor workflow for Apache Airflow: set up Breeze, make a code or
  doc change, run the right tests, run static checks, and open a PR. Use when
  helping a contributor fix a bug, add a feature, or update documentation.
  Covers environment setup, test routing (regular vs db_test), prek checks, and
  commit/push/PR steps.
license: Apache-2.0
---
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Airflow Contributor Workflow

## Determining the Type of Change

Before doing anything else, identify what kind of change is being made:

- **Translation change** (UI strings in `airflow-core/src/airflow/ui/public/i18n/locales/`) →
  use the `airflow-translations` skill instead of this one.
- **Documentation-only change** (`.rst` files under `contributing-docs/` or provider docs) →
  follow Environment Setup, then skip to **Build Docs**.
- **Code change** (`airflow-core/`, `providers/`, `task-sdk/`, etc.) →
  follow all sections in order.

---

## Environment Setup

### Start Breeze

If you are already inside the Breeze container (prompt shows `/opt/airflow`), skip this section.

From the host machine:

```bash
breeze start-airflow
```

This starts a shell with tmux and launches all Airflow components. First run builds the image
and takes several minutes. Subsequent runs are fast.

### Verify Breeze is running

Open `http://localhost:28080` in a browser. Log in with user `admin`, password `admin`.
If the UI loads, Breeze is healthy.

**If `breeze start-airflow` fails:**

```bash
docker network prune
breeze start-airflow
```

If it still fails, check Docker is running: `docker info`. If Docker is not running, start
Docker Desktop (or the Docker daemon) and retry.

---

## Making the Change

Edit the relevant source files. Key paths:

- Core scheduler, API, models: `airflow-core/src/airflow/`
- Task SDK: `task-sdk/src/airflow/sdk/`
- Providers (100+ packages): `providers/{name}/`
- Contributor docs: `contributing-docs/`

Tests mirror source: `airflow-core/src/airflow/cli/cli_parser.py` →
`airflow-core/tests/cli/test_cli_parser.py`.

---

## Running Tests

### Step 1: Determine the test type

Inspect the test file for the `@pytest.mark.db_test` marker:

```bash
grep -n "db_test" path/to/test_file.py
```

- **Marker found** → the test needs a live database. Go to **DB Tests**.
- **No marker** → Go to **Regular Tests**.

### Regular Tests (no db_test marker)

Try uv first — it is faster and debuggable in an IDE:

```bash
uv run --project {project} pytest {test_path} -xvs
```

Where `{project}` is the distribution folder containing `pyproject.toml`, e.g. `airflow-core`
or `providers/amazon`.

**If uv fails** with missing system dependencies (e.g. `mysql`, `kubernetes` libraries):

```bash
breeze run pytest {test_path} -xvs
```

**If already inside Breeze:**

```bash
pytest {test_path} -xvs
```

Success signal: output contains `passed`.

### DB Tests (@pytest.mark.db_test)

Do **not** try uv. Go directly to Breeze — uv on the host cannot provision a live database.

From the host:

```bash
breeze run pytest {test_path} -xvs
```

Inside Breeze:

```bash
pytest {test_path} -xvs
```

Success signal: output contains `passed`.

### Running a full test suite

```bash
# All core tests in parallel
breeze testing core-tests --run-in-parallel

# All DB tests only
breeze testing core-tests --run-db-tests-only --run-in-parallel

# All non-DB tests
breeze testing core-tests --skip-db-tests --use-xdist

# Provider tests
breeze testing providers-tests --test-type "Providers[amazon]"
```

---

## Running Static Checks

### Before committing (fast)

```bash
prek run --from-ref main --stage pre-commit
```

This runs ruff formatting, ruff lint, mypy, license header checks, and other fast checks.
Fix all reported issues before committing.

Success signal: `All checks passed.`

### Before opening a PR (thorough)

```bash
prek run --from-ref main --stage manual
```

This runs slower checks (e.g. full mypy, helm lint). Run this once before pushing your
final commit.

Success signal: `All checks passed.`

---

## Build Docs (documentation changes only)

If any `.rst` files changed, build docs to verify no broken references or formatting errors.

Build all docs (slow):

```bash
breeze build-docs
```

Build a single package (faster, use when only one provider's docs changed):

```bash
breeze build-docs --package-filter apache-airflow-providers-{name}
```

Success signal: output contains `Build finished.`

---

## Commit and Open PR

### Commit

```bash
git checkout -b {branch-name}
git add {files}
git commit -m "{message}"
```

Commit messages should focus on user impact:

- Good: `Fix airflow dags test command failure without serialized Dags`
- Bad: `Initialize DAG bundles in CLI get_dag function`

### Push and open PR

```bash
git push -u origin {branch-name}
```

Then open the PR. Always include the generative AI disclosure if AI tooling assisted.
See `AGENTS.md` → "Creating Pull Requests" for the full PR template.

### Sync with upstream (if needed)

```bash
git fetch upstream main
git rebase upstream/main
git push --force-with-lease
```
