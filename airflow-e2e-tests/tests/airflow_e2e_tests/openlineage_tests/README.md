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

# OpenLineage provider end-to-end tests

These tests deploy a **real, multi-process Airflow** (via docker-compose) and run the OpenLineage
provider's system-test DAGs against it — exercising the integration the way it actually runs in a
deployment (separate scheduler / dag-processor / triggerer / api-server / workers), rather than the
in-process `dag.test()` path.

They run as the `openlineage` mode of the shared `airflow-e2e-tests` suite, reusing its
docker-compose base, testcontainers conftest, and `AirflowClient`:

```bash
breeze testing airflow-e2e-tests --e2e-test-mode openlineage
```

## How it works

- The DAGs, `OpenLineageTestOperator`, `VariableTransport`, and expected-event JSON are the ones in
  [`providers/openlineage/tests/system/openlineage`](../../../../providers/openlineage/tests/system/openlineage) —
  that directory stays the single source of truth. `prepare_dags.py` copies them into a (gitignored)
  `dags/` folder at runtime and strips the pytest-only `get_test_run` footer so they parse in a
  deployment.
- The `openlineage` mode adds only the [`docker/openlineage.yml`](../../../docker/openlineage.yml)
  overlay (OpenLineage env config + `dag_doc.md` mount) on top of the shared compose base; the
  conftest's `_setup_openlineage_integration` wires it up.
- OpenLineage is configured to emit through the test `VariableTransport` (events land in Airflow
  Variables); each DAG's terminal `OpenLineageTestOperator` task validates the emitted events against
  the expected templates. **A DAG run that ends `success` means its lineage matched.**
- `harness.py` triggers every DAG through the REST API, waits for completion, retries a failed DAG
  once, and asserts all expected DAGs succeeded.
- A small `MockVersionedLocalDagBundle` (in `dags_extra/`) provides a versioned bundle (so
  `dag_bundle_version` is populated) while still serving DAGs from the local dags folder.

## Checking results

- **pytest summary:** `1 passed` means every expected DAG is green. On failure it prints the
  offenders, e.g. `... not successful: {'openlineage_docs_file_dag': 'failed'}`, and logs
  `⚠ DAGs that passed only on retry (flaky first run): [...]`.
- **On failure, the relevant task logs are printed right in the pytest output** (`check_events` plus
  any task that didn't succeed, filtered to warning/error lines — see `task_logs.py`) — no need to
  open the log artifact for the common case, e.g. the exact mismatch
  (``Path `job > facets > ...`: expected X but got Y``).
- **Per-DAG logs** are still written in full under `airflow-e2e-tests/logs/` for deeper digging
  beyond what gets printed.
- With `--skip-docker-compose-deletion`, the stack stays up: get a token from
  `curl -s http://localhost:8080/auth/token` and check `GET /api/v2/importErrors` for DAG parse
  errors, or browse the runs in the UI at <http://localhost:8080> (login `airflow` / `airflow`).

## Compatibility with older Airflow

A compat matrix reruns the suite against older *released* Airflow versions with the current provider
code from main — the same idea as the provider compatibility tests, catching core-vs-provider
breakages early. It builds a lightweight image (`apache/airflow:<version>` + current
OpenLineage-related provider wheels, see
[`docker/openlineage-compat.Dockerfile`](../../../docker/openlineage-compat.Dockerfile)) and runs:

```bash
breeze testing airflow-e2e-tests --e2e-test-mode openlineage --airflow-version 3.1.8
```

The matrix is costly, so it does **not** run on every OpenLineage PR: it runs on canary (scheduled /
main) or when a maintainer sets the **`full tests needed`** label on the PR (both folded into the
`run-openlineage-e2e-compat-tests` selective-check output). Ordinary PRs run the PROD-image job only.

Some DAGs require a newer Airflow than the compat targets and are dropped for older versions in
`prepare_dags.py` (`MIN_AIRFLOW_VERSION_FOR_DAG`) — e.g. `example_openlineage_hitl_dag` needs 3.1+
(its operators import-raise on older cores). Add an entry there when a new DAG is version-gated.

## Layout

```text
prepare_dags.py             # sources DAGs from the provider system tests into dags/ at runtime
harness.py                  # triggers every DAG via the REST API and collects final run states
task_logs.py                # locates and prints the relevant task logs on failure
test_openlineage_e2e.py     # the pytest entrypoint
dags_extra/                 # harness-only DAGs/modules (warmup DAG, versioned bundle) copied into dags/
```

The OpenLineage-specific compose overlay lives at
[`airflow-e2e-tests/docker/openlineage.yml`](../../../docker/openlineage.yml); the compose lifecycle
and the shared `AirflowClient` come from the parent `airflow-e2e-tests` suite.
