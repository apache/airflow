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

See the [top-level README](../README.md) for how to run any provider's e2e suite with breeze, how
this is wired into CI, and what's involved in adding a new provider.

## How it works

- The DAGs, `OpenLineageTestOperator`, `VariableTransport`, and expected-event JSON are the ones in
  [`providers/openlineage/tests/system/openlineage`](../../providers/openlineage/tests/system/openlineage) —
  that directory stays the single source of truth. `prepare_dags.py` copies them into a (gitignored)
  `dags/` folder at runtime and strips the pytest-only `get_test_run` footer so they parse in a
  deployment.
- OpenLineage is configured to emit through the test `VariableTransport` (events land in Airflow
  Variables); each DAG's terminal `OpenLineageTestOperator` task validates the emitted events against
  the expected templates. **A DAG run that ends `success` means its lineage matched.**
- `harness.py` triggers every DAG through the REST API, waits for completion, retries a failed DAG
  once, and asserts all expected DAGs succeeded.
- A small `MockVersionedLocalDagBundle` provides a versioned bundle (so `dag_bundle_version` is
  populated) while still serving DAGs from the local dags folder.

## Checking results

- **pytest summary:** `1 passed` means every expected DAG is green. On failure it prints the
  offenders, e.g. `... not successful: {'openlineage_docs_file_dag': 'failed'}`, and logs
  `⚠ DAGs that passed only on retry (flaky first run): [...]`.
- **On failure, the relevant task logs are printed right in the pytest output** (`check_events` plus
  any task that didn't succeed, filtered to warning/error lines — see `tests/task_logs.py`) — no
  need to open the log artifact for the common case, e.g. the exact mismatch
  (``Path `job > facets > ...`: expected X but got Y``).
- **Per-DAG logs** are still written in full to `logs/dag_id=<dag>/.../task_id=<task>/attempt=1.log`
  for deeper digging beyond what gets printed.
- With `--skip-docker-compose-deletion`, the stack stays up: get a token from
  `curl -s http://localhost:8080/auth/token` and check `GET /api/v2/importErrors` for DAG parse
  errors, or browse the runs in the UI at <http://localhost:8080> (login `airflow` / `airflow`).

## Version-specific DAGs

Some DAGs require a newer Airflow than the compat targets and are dropped for older versions in
`prepare_dags.py` (`MIN_AIRFLOW_VERSION_FOR_DAG`) — e.g. `example_openlineage_hitl_dag` needs 3.1+
(its operators import-raise on older cores). Add an entry there when a new DAG is version-gated.

## Layout

```text
docker-compose.yaml         # the deployed stack (postgres + apiserver + scheduler + dag-processor + triggerer)
docker-compose-local.yaml   # overlay that mounts local sources (used unless --skip-mounting-local-volumes)
Dockerfile                  # lightweight image for --airflow-version: apache/airflow:<ver> + current providers
prepare_dags.py             # sources DAGs from the provider system tests into dags/ at runtime
dags_extra/                 # harness-only DAGs/modules (warmup DAG, versioned bundle) copied into dags/
tests/                      # conftest (compose lifecycle), harness, constants, and the pytest entrypoint
```
