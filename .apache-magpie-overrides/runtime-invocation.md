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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — runtime invocation](#apache-airflow--runtime-invocation)
  - [Build prerequisite](#build-prerequisite)
  - [Run a single file](#run-a-single-file)
  - [Capture conventions](#capture-conventions)
  - [Network and dependency handling](#network-and-dependency-handling)
  - [Cross-references](#cross-references)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — runtime invocation

How to invoke the project's runtime on a single source file. Used
by [`issue-reproducer`](../../skills/issue-reproducer/SKILL.md)
when running extracted code from issue descriptions.

The `<runtime>` placeholder resolves from the *Run a single file*
section below.

**Hard rule** (from [`AGENTS.md`](../AGENTS.md)): never run `pytest`,
`python` or `airflow` directly on the host. Every reproducer runs inside
the [Breeze](../dev/breeze/doc/README.rst) container, which is the same
environment Airflow CI uses. The only host-side exception is a pure-unit
pytest (no database, no system dependencies) run through
`uv run --project <PROJECT> pytest ...`; fall back to Breeze as soon as it
fails on a missing system dependency.

## Build prerequisite

Breeze must be installed and the CI image built for the Python version
you run with:

- Install the per-worktree `breeze` shim once per machine:
  `scripts/tools/setup_breeze` (installs `~/.local/bin/breeze`, which runs
  Breeze via `uv run --locked` from the current worktree's `dev/breeze` —
  see [ADR 0017](../dev/breeze/doc/adr/0017-use-uvx-to-run-breeze-from-local-sources.md)).
- Docker must be running (see
  [`dev/breeze/doc/01_installation.rst`](../dev/breeze/doc/01_installation.rst)).
- Build or refresh the CI image before a campaign so the first reproducer
  does not pay the build cost inside its timeout:
  `breeze ci-image build --python <python>`.
- Python versions: `3.10` (default), `3.11`, `3.12`, `3.13`, `3.14` —
  `ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS` in
  `dev/breeze/src/airflow_breeze/global_constants.py`; `requires-python`
  in `airflow-core/pyproject.toml`. Use the default unless the issue names
  a specific Python version.
- Metadata-database backend: `sqlite` (default), `postgres` or `mysql`
  via `--backend`. Use the backend the issue reports when the bug is
  backend-specific (migrations, locking, SQL dialect); otherwise the
  default.

Paths inside the container: the repository is mounted at `/opt/airflow`,
the gitignored `files/` folder at `/files`. Scratch reproducer scripts go
in `files/` or `dev/` (`dev/` is mounted as `/opt/airflow/dev/`) — never
elsewhere in the source tree.

## Run a single file

Three runtime shapes, chosen by what the reproducer is. `<file>` is the
in-container path of the reproducer (host `files/<...>` →
`/files/<...>`); `<args>` is optional argv.

Recipe — plain Python script against `main` sources:

```text
breeze run python <file> <args>
```

Recipe — Dag file (the common case for Airflow bug reports). Point the
Dags folder at the reproducer's own directory so no other Dag is parsed,
migrate the throwaway metadata DB, then run one Dag run in-process:

```text
breeze run bash -c "airflow db migrate >/dev/null && \
  AIRFLOW__CORE__DAGS_FOLDER=$(dirname <file>) airflow dags test <dag_id>"
```

Recipe — pytest reproducer placed in the matching tests directory
(`airflow-core/tests/unit/...`, `providers/<name>/tests/unit/...`,
`task-sdk/tests/...`):

```text
breeze run pytest <file> -xvs
# pure-unit, no DB / system deps only:
uv run --project <PROJECT> pytest <file> -xvs
```

Add `--backend postgres|mysql` and/or `--python <X.Y>` to any `breeze run`
line when the issue requires it.

Reproducing against a **released** version instead of `main`:
`breeze run` has no `--use-airflow-version`; use `breeze shell`, which
accepts the command as trailing arguments and reinstalls Airflow from
PyPI at entry:

```text
breeze shell --use-airflow-version <X.Y.Z> [--airflow-extras <extras>] \
  "<same command as above>"
```

`--use-airflow-version` also accepts `wheel` / `sdist` (from `dist/`),
`<owner>/<repo>:<branch>`, or a PR number — useful for checking whether an
open PR fixes the issue. Documented in
[`dev/breeze/doc/03_developer_tasks.rst`](../dev/breeze/doc/03_developer_tasks.rst).

## Capture conventions

How to capture stdout, stderr, and exit code in a way the skill can
parse.

| Stream | Convention |
|---|---|
| `stdout` | Capture in full. Breeze prints its own banner and entrypoint output before the command's output; the verdict is based only on the command's output that follows it. |
| `stderr` | Capture and merge into `run.log` (`2>&1`). Airflow logs, task tracebacks and Python warnings go to stderr. |
| `exit code` | `breeze run` / `breeze shell "<cmd>"` propagate the inner command's exit code: 0 = success, non-zero = failure. For `airflow dags test`, do not rely on the exit code alone — also read the final Dag run / task instance states and any traceback in the output. |
| `timeout` | 600s per invocation. Container start-up and (for `--use-airflow-version`) the PyPI reinstall at entry take minutes before the reproducer body starts; a run that times out during start-up is `inconclusive`, not `fails`. |

## Network and dependency handling

The Breeze CI image already contains Airflow core, the Task SDK and all
providers installed from local sources, so a reproducer against `main`
does not resolve dependencies at runtime. Network access is needed only
when building the image or when `--use-airflow-version` /
`--airflow-extras` install from PyPI at container entry.

- A reproducer that needs a provider or library not in the image must
  not `pip install` it silently. When running a released version, pass it
  via `--airflow-extras`; otherwise record the missing dependency in the
  verdict as `inconclusive`.
- A failed PyPI install at entry, an image-build failure, or a
  `ModuleNotFoundError` for a provider is an **environment** failure —
  check for it in the output before classifying the run as `passes` or
  `fails`.
- Reproducers needing an external system (Kafka, MongoDB, …) use
  `breeze --integration <name>` (see
  [`dev/breeze/doc/03_developer_tasks.rst`](../dev/breeze/doc/03_developer_tasks.rst));
  cloud-service reproducers that need real credentials cannot be run and
  are recorded as `inconclusive`.
- Each `breeze run` starts a fresh container, so the metadata DB and
  installed packages do not leak between reproducers; no per-campaign
  cache isolation is needed.

| Key | Value |
|---|---|
| `resolves_dependencies_at_runtime` | `false` |
| `cache_isolation_flag` | *(not applicable — each `breeze run` uses a fresh container)* |

## Cross-references

- [`reassess-pool-defaults.md`](reassess-pool-defaults.md) — named
  pools for `issue-reassess` sweeps.
- [`reproducer-conventions.md`](reproducer-conventions.md) —
  per-issue evidence-package directory layout.
- [`issue-tracker-config.md`](issue-tracker-config.md) — tracker
  URL and project key.
