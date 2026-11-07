 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# scripts/ci/prek/ guidelines

## Overview

This directory contains prek (pre-commit) hook scripts. Shared utilities live in
`common_prek_utils.py` — always check there before duplicating logic.

## Breeze CI image scripts

Some prek scripts require the Breeze CI Docker image to run (e.g. `mypy-providers`, OpenAPI
spec generation, provider validation). These scripts use the `run_command_via_breeze_run`
helper from `common_prek_utils.py` to execute commands inside the container. Non-provider
mypy hooks (`mypy-airflow-core`, `mypy-task-sdk`, `mypy-shared-<dist>`, etc.) run locally via
`run_mypy_full_dist_local_venv_or_breeze_in_ci.py`, which builds a dedicated virtualenv per hook under `.build/mypy-venvs/`
using `uv sync --frozen --project <X> --group mypy` — no Breeze image needed.

When adding a new breeze-dependent hook:

1. Import and use `run_command_via_breeze_run` from `common_prek_utils` — do not shell out
   to `breeze` directly.
2. Register the hook at the **end** of the relevant `.pre-commit-config.yaml` file (breeze
   hooks are slow and should run after fast, local checks). A hook in the `priority: 1000` group
   described below runs after the other hooks no matter where it sits in the file, so this rule
   does not apply to that hook.

## Adding new hooks

- Scripts must be Python (not bash).
- Use helpers from `common_prek_utils.py` for path constants, console output, and breeze
  execution.
- Register the script in the appropriate `.pre-commit-config.yaml` (`/.pre-commit-config.yaml`
  for repo-wide hooks, `/airflow-core/.pre-commit-config.yaml` for core-specific hooks, or a
  provider-level config).
- Give every hook `env: {PYTHONPYCACHEPREFIX: .build/pycache}`. Hooks run in parallel and
  breeze deletes `__pycache__` directories on start-up, so a hook writing `.pyc` next to the
  sources can make a concurrent breeze hook fail with `Directory not empty`.
  `check-prek-hooks-pycache-prefix` enforces this.
- In `/.pre-commit-config.yaml`, the hooks with `priority: 1000` form one group. prek runs them in
  parallel, after every other hook in the file. Leave `priority:` unset on a hook unless it joins
  this group. A hook may join the group only if it follows all of these rules:
  - It never changes a tracked file, not even when it finds drift. prek runs `git diff` before and
    after the group. When tracked files change, prek fails the whole group and does not say which
    hook changed them.
  - It writes untracked files only where no other hook in the group reads them, for example under
    `.build/`.
  - It does not change the git index, refs or worktrees, and it does not use fixed Docker
    container, network or volume names.
  - It shares a virtualenv or cache with another hook only if the tool that writes the virtualenv
    or cache locks it or writes each file atomically. For example, uv locks a virtualenv while
    installing into it, and Python writes each `.pyc` file atomically.
  - Unless the hook is `check-template-fields-valid`, it does not run Breeze and it does not walk a
    directory tree with `Path.rglob()` on Python 3.11. `check-template-fields-valid` is the only
    hook in the group that runs Breeze, and Breeze deletes `__pycache__` directories at startup.
    On Python 3.11, `rglob()` fails when a directory disappears during the walk. So while
    `check-template-fields-valid` runs, a hook that walks a tree with `rglob()` on Python 3.11 can
    fail. `check-template-fields-valid` itself walks `providers/` with `rglob()` on Python 3.11
    inside the Breeze container, so a second hook that runs Breeze can make that walk fail.

  To add a hook to the group, set `priority: 1000` on it and list it in `PARALLEL_HOOKS` in
  `check_prek_parallel_hooks.py`. The `check-prek-parallel-hooks` hook fails if a hook in
  `/.pre-commit-config.yaml` sets a priority other than 1000, or sets 1000 but is not listed in
  `PARALLEL_HOOKS`. If you change a listed hook so that it breaks one of the rules above, take it
  out of the group.
