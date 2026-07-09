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

# Provider end-to-end tests

Each subdirectory here is a self-contained e2e suite for one provider, that can be run against a real built
Airflow image (`$DOCKER_IMAGE`) rather than the in-process `dag.test()` path used by provider system tests.

What "e2e" means, and how the stack under test gets stood up, is entirely up to the provider — see that provider's
own `README.md` for what its suite tests, how it works, and how to interpret its results. For an example implementation
(a docker-compose deployed stack), see [`openlineage`](openlineage) and its [README](openlineage/README.md).

## Running locally

Docker must be running. Every provider is driven by the same breeze command:

```bash
# Default: PROD image (current Airflow from sources + providers from sources)
breeze testing providers-e2e-tests <provider>

# An older released Airflow version, with the current providers installed from main
# (builds apache/airflow:<ver> + current provider wheels — no full PROD build)
breeze testing providers-e2e-tests <provider> --airflow-version 3.1.8

# Tear the stack down
breeze testing providers-e2e-tests <provider> --down
```

`<provider>` is any subdirectory of `providers-e2e-tests/` with a `pyproject.toml` — run
`breeze testing providers-e2e-tests --help` to see the current choices.

Useful flags, passed to every provider's pytest run as env vars for its own harness to honor:

- `--skip-docker-compose-deletion` (`SKIP_DOCKER_COMPOSE_DELETION`) — keep the stack up after the run to inspect it.
- `--skip-mounting-local-volumes` (`SKIP_MOUNTING_LOCAL_VOLUMES`) — run against the image only (what CI does).
Omit it during dev to mount and hot-reload local sources.
- Anything after the provider is passed through to pytest (e.g. `... <provider> -- -s`).

The first run builds the PROD image; subsequent runs are much faster.
When running non-interactively without a pre-built image, prefix `ANSWER=yes` so the image-build prompt auto-confirms.

## How this is wired into CI

```text
ci-amd.yml / ci-arm.yml
  └─ additional-prod-image-tests (job) ──uses──▶ additional-prod-image-tests.yml
       └─ test-providers-e2e-tests-<provider>[-compat] (job) ──uses──▶ providers-e2e-tests.yml
            └─ runs: breeze testing providers-e2e-tests <provider> [--airflow-version <ver>]
```

`additional-prod-image-tests.yml` is a single reusable workflow called identically from both
`ci-amd.yml` and `ci-arm.yml`, so any job defined there — including each provider's e2e tests —
automatically gets both ARM and AMD coverage from one job definition.
[`scripts/ci/prek/check_ci_workflows_in_sync.py`](../scripts/ci/prek/check_ci_workflows_in_sync.py)
enforces that the two `ci-*.yml` files stay identical outside a small documented allowlist, so
don't add provider-e2e jobs directly to `ci-amd.yml`/`ci-arm.yml` — they belong in
`additional-prod-image-tests.yml`.

Each provider's suite is gated by its own `run-providers-e2e-tests-<provider>` selective-checks output
(computed in `dev/breeze/src/airflow_breeze/utils/selective_checks.py` from the files changed in a
PR) — it always runs on canary/`main` runs, and on a PR only when that provider (or a file group it
depends on, e.g. `common` providers) changed.

## Adding a new provider

1. Create `providers-e2e-tests/<provider>/`. The breeze harness (`breeze testing
   providers-e2e-tests`, see `dev/breeze/src/airflow_breeze/commands/testing_commands.py` and
   `run_docker_compose_tests` in `dev/breeze/src/airflow_breeze/utils/run_tests.py`) only requires:
   - `pyproject.toml` — its presence is what makes `<provider>` a valid choice
     (`_available_e2e_providers()`). It only needs a `[tool.e2e-tests] required-providers = [...]`
     entry if the provider supports `--airflow-version` compat mode — that lists the provider
     distributions to build from `main` for that mode (see `_build_providers_e2e_compat_image`).
   - `tests/` — a pytest suite. Breeze runs `pytest tests/` from this directory with `$DOCKER_IMAGE`
     (and the flags above) set as env vars; what it does with that image is entirely up to the
     provider.
   - `Dockerfile` — only needed if the provider supports `--airflow-version` compat mode: breeze
     builds `apache/airflow:<ver>` + the provider wheels from `main` using it.
2. Wire the trigger into selective checks
   (`dev/breeze/src/airflow_breeze/utils/selective_checks.py`): add a `FileGroupForCi` member and
   its regex patterns, a `run_providers_e2e_tests_<provider>` cached property, and expose it as a
   `run-providers-e2e-tests-<provider>` output — mirror the existing `PROVIDERS_E2E_OPENLINEAGE_FILES` /
   `run_providers_e2e_tests_openlineage` pair. Update
   [`dev/breeze/tests/test_selective_checks.py`](../dev/breeze/tests/test_selective_checks.py) and
   [`dev/breeze/doc/ci/04_selective_checks.md`](../dev/breeze/doc/ci/04_selective_checks.md) in the
   same PR.
3. Thread the new output through **both** `ci-amd.yml` and `ci-arm.yml` identically: add it to the
   `build-info` job's `outputs:` and to the `additional-prod-image-tests` job's `with:` block.
4. Add the job(s) to `.github/workflows/additional-prod-image-tests.yml`, named
   `test-providers-e2e-tests-<provider>` (and `test-providers-e2e-tests-<provider>-compat` for the
   compat matrix job, if any) — mirror the existing `test-providers-e2e-tests-openlineage` pair.
   Call `providers-e2e-tests.yml` with `provider: "<name>"` and
   `provider-display-name: "<Display Name>"`, gated on
   `inputs.canary-run == 'true' || inputs.run-providers-e2e-tests-<provider> == 'true'`. The compat
   job additionally needs a `strategy.matrix` over `inputs.providers-compatibility-tests-matrix` and
   `airflow-version: ${{ matrix.compat.airflow-version }}`.
