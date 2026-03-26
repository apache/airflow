---
name: airflow-contributor
description: >
  Guide for Apache Airflow contributors: decide which skill to use and why.
  Exact commands are resolved at runtime from the command registry.
license: Apache-2.0
---
<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Airflow Contributor Skill Guide

## Command resolution

Do not hardcode commands from memory. Always resolve them at runtime:

```bash
python scripts/ci/prek/context_detect.py <skill_id> [key=value ...]
```

The script reads `contributing-docs/workflows/agent_skills.rst` directly,
detects whether you are on the host or inside Breeze, and prints the exact
command with parameters substituted. Fallback and success signal are printed
to stderr.

List all available skills:

```bash
python scripts/ci/prek/context_detect.py --list
```

---

## Decision trees

### Environment

Check your context before anything else.

- Prompt shows `/opt/airflow` → you are inside Breeze. No setup needed.
- Otherwise → run skill `setup-breeze-environment` before tests or checks.

---

### Running tests

1. Find the test file. Test location mirrors source:
   `airflow-core/src/airflow/cli/cli_parser.py` → `airflow-core/tests/cli/test_cli_parser.py`

2. Check for the db_test marker:

   ```bash
   grep -n 'db_test' path/to/test_file.py
   ```

3. Choose skill:
   - **Marker found** → `run-db-test`
     Never use uv — it cannot provision a live database. Always goes to Breeze.
   - **No marker** → `run-single-test`
     Preferred: uv (fast, IDE-debuggable). Fallback: Breeze (when system deps missing).

4. Resolve and run:

   ```bash
   # example
   python scripts/ci/prek/context_detect.py \
     run-single-test project=providers/vertica test_path=providers/vertica/tests/unit/
   ```

**Important:** `{project}` is the folder containing `pyproject.toml`
(e.g. `airflow-core`, `providers/vertica`, `providers/amazon`).
Always scope `uv run` to the provider — without `--project`, uv resolves the
entire monorepo workspace and fails on hosts missing MySQL or other native deps.

---

### Formatting and linting

After writing or editing **any** Python file, immediately run `format-and-lint`
before moving on. Same `--project` scoping rule applies — never run ruff
without scoping to the provider.

```bash
python scripts/ci/prek/context_detect.py \
  format-and-lint project=providers/vertica file_path=providers/vertica/hooks/vertica.py
```

---

### Static checks

- Before committing → `run-static-checks` (fast: ruff, mypy, license headers).
- Before opening a PR → `run-manual-checks` (slower, more thorough).
  `run-manual-checks` requires `run-static-checks` to pass first.

`{target_branch}` is the branch the PR targets — usually `main`,
sometimes `v3-1-test` for patch releases.

---

### Documentation

If any `.rst` files changed → `build-docs`.

Pass `package=apache-airflow-providers-{name}` to build only that provider's
docs — significantly faster than a full build.

---

## Skill registry

Full command definitions (steps, parameters, prereqs, fallbacks) are in:
`contributing-docs/workflows/agent_skills.rst`
