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

# M0 scheduler-hosting experiment — September 26, 2026

The scheduler can host the existing `ParseOrchestrator` against registered inventory
while a separate Celery runner handles placement. A real scheduler parsed and reparsed
two definitions, scheduled a recurring Dag and continued making progress during a slow
import, a broker pause and SQLite contention. No Dag processor was running.

This is a feasibility result for one scheduler, one bundle and a disposable SQLite
database. It does not establish production readiness or a performance advantage over
the existing manager.

## Implementation

- `airflow scheduler --parsing-config <JSON>` opts into a periodic parsing callback.
  The ordinary scheduler path creates no parsing host.
- `SchedulerParsingHost` reuses `ParseOrchestrator.step()` and the standalone host's
  ownership lock. Configuration bounds batch size, route capacity and step budget.
- Scheduler transactions have zero lock wait and a cooperative SQL progress deadline
  (25 ms by default). Expiry rolls back the transaction; a busy database defers the step.
  This is not a hard wall-clock guarantee: filesystem, kernel and Python execution
  cannot be preempted by SQLite's progress handler.
- `ParsingExecutorRunner` supports a separately hosted Celery executor. Remote success
  releases a reservation only with all successful/import-error receipts. Missing results,
  uncertain submission and remote failure still require external recovery evidence.
  Local publication-failure evidence is never treated as remote termination evidence.
- The scheduler receives no bundle path or Dag source mount. The worker has the source,
  a workload token and the API address, with no metadata database mount or signing key.
  Trusted bootstrap discovery registers the inventory before the scheduler starts.

## Live results

Final run: `poc-runs/scheduler-hosting-20260926-04/summary.json`.
Each phase lasted ten seconds; the slow definition slept three seconds during importing.

| Phase | Parsing step median / p95 / maximum | Scheduler loop median / p95 / maximum | Deferred steps | Additional completed EmptyOperator tasks |
| --- | --- | --- | --- | --- |
| Healthy, including slow imports | 1.49 / 3.85 / 4.96 ms | 17.07 / 32.27 / 49.20 ms | 2 | 5 |
| Redis broker paused | 1.47 / 3.22 / 5.13 ms | 15.95 / 44.77 / 76.03 ms | 0 | 5 |
| SQLite write contention | 1.29 / 2.69 / 4.78 ms | 17.02 / 42.77 / 85.18 ms | 36 | 5 |

The final database contained 13 accepted fast-definition parses, six accepted slow-definition
parses and 19 completed tasks. Median time from scheduled run time to EmptyOperator
completion was 82 ms; maximum was 558 ms. All reservations drained after scheduler
shutdown. Mount inspection and the worker startup probe recorded the deployment boundary.

The earlier successful run (`scheduler-hosting-20260926-02`) also finished with 19 tasks,
19 accepted parses and no reservations. Its parsing callback maximum was 6.81 ms and
its contention phase deferred 31 times. The two runs are small feasibility samples,
not a statistically controlled performance comparison.

SQLite contention affects existing scheduler transactions as well as parsing; the loop
maximum is not the parsing callback's duration. EmptyOperator completion tests scheduler
progress without executing user task code. These small samples have no matched disabled
baseline and do not measure task-worker throughput, large inventories or long outages.

The first attempt stopped before any parsing because the clean scheduler container had
no username for the CLI. The harness now supplies `USER` and detects exited containers
at startup. Its logs are retained in `poc-runs/scheduler-hosting-20260926-01/`.
A third attempt hit a Docker exit 125 during broker startup, before parsing; the original
wrapper did not print stderr, so its underlying cause is unknown. The harness now reports
Docker errors and registers cleanup before container startup. Run 04 passed using the
documented invocation and the active Breeze image.

## Validation and review

- 242 core, CLI, scheduler, metadata, worker and orchestration regressions passed,
  including the real opt-in Dag processor command.
- 104 measurement and durable recovery tests passed.
- Final repository static checks passed, including core/development mypy, Ruff,
  Bandit and metrics-registry validation (`scheduler-hosting-final-static.log`).
  The registry scanner uses tracked paths; validation used a temporary index containing
  the new module and left the actual staging area unchanged.
- Review covered default-path isolation, lock release, budget rollback, metadata
  registration, remote completion evidence, process cleanup and measurement validity.
  Fixes included preserving stderr in captured container logs, detecting missing
  metrics, reusing the active Breeze image and registering the new metrics.

## Reproduce

From this worktree, with a working Breeze image and Docker:

```bash
breeze run -- uv run --no-project --python /usr/python/bin/python python \
  dev/dag_parsing_poc/run_scheduler_parsing.py \
  --output /files/dag-parsing-aip/poc-runs/scheduler-hosting --phase-seconds 10
```

The output directory must be new. The driver creates its own database, Redis container,
Celery worker, scheduler and network, and removes its containers on exit. It leaves logs,
metrics, the database and mount evidence under the worktree's `files/` directory.

## Still open

Remote discovery and bundle preparation; ownership and adoption across multiple
schedulers; production database/schema/API integration; callback and priority semantics;
source removal; recovery after a Celery runner restart; provider outage policy; larger
inventories, a matched scheduler baseline and agreed latency thresholds. Scheduler
hosting is now exercised in M0, but the Dag processor remains required for normal deployments.
