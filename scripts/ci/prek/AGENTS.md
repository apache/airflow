 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

# scripts/ci/prek/ guidelines

## Overview

This directory contains prek (pre-commit) hook scripts. Shared utilities live in
`common_prek_utils.py` — always check there before duplicating logic.

## Breeze CI image scripts

Some prek scripts require the Breeze CI Docker image to run (e.g. `mypy-providers`, OpenAPI
spec generation, provider validation). These scripts use the `run_command_via_breeze_shell`
helper from `common_prek_utils.py` to execute commands inside the container. Non-provider
mypy hooks (`mypy-airflow-core`, `mypy-task-sdk`, `mypy-shared-<dist>`, etc.) run locally via
`run_mypy_full_dist_local_venv_or_breeze_in_ci.py`, which builds a dedicated virtualenv per hook under `.build/mypy-venvs/`
using `uv sync --frozen --project <X> --group mypy` — no Breeze image needed.

When adding a new breeze-dependent hook:

1. Import and use `run_command_via_breeze_shell` from `common_prek_utils` — do not shell out
   to `breeze` directly.
2. Register the hook at the **end** of the relevant `.pre-commit-config.yaml` file (breeze
   hooks are slow and should run after fast, local checks).

## Adding new hooks

- Scripts must be Python (not bash).
- Use helpers from `common_prek_utils.py` for path constants, console output, and breeze
  execution.
- Register the script in the appropriate `.pre-commit-config.yaml` (`/.pre-commit-config.yaml`
  for repo-wide hooks, `/airflow-core/.pre-commit-config.yaml` for core-specific hooks, or a
  provider-level config).
