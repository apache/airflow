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

# Executor parsing core integration review

Reviewed the uncommitted changes after `1231ebe3d3` on
`codex/dag-parsing-executor-poc`, including relocated implementation, command
lifecycle, discovery, scheduling, API persistence and recovery.

## Findings addressed

1. **Major — Later batch members missed the start deadline.**
   A slow first import left later definitions unclaimed after the deadline.
   The [worker](../../airflow-core/src/airflow/dag_processing/executor_worker.py)
   now claims every pending definition before importing any. Execution and
   acknowledgment deadlines remain separate.
2. **Major — Finite runs repeated finished definitions.**
   With multiple batches, a finished definition became due again while another
   batch was running. The [host](../../airflow-core/src/airflow/dag_processing/executor_manager.py)
   now computes remaining paths for this invocation; the
   [orchestrator](../../airflow-core/src/airflow/dag_processing/orchestrator.py)
   filters these before applying its admission limit.
3. **Major — Restart could dispatch stale, unsent work.**
   The runner could submit a restored reservation before discovery or run-count
   baselines were established. Startup now
   [retires unsubmitted reservations](../../airflow-core/src/airflow/dag_processing/parsing_state.py)
   atomically before starting the runner. Claims fence retirement, and submitted
   work still requires confirmed termination and recovery.
4. **Major — Config order could starve later bundles.**
   A continuously due first bundle could take every newly available slot.
   Successful admission now moves that bundle to the back of the scheduling order.
5. **Minor — Different database extensions shared a lock.**
   `airflow.db` and `airflow.sqlite` previously mapped to the same lock filename.
   Locks now preserve the entire database filename.

Regression tests reproduced findings 1–4 before their fixes. Additional tests cover
lock independence, atomic retirement, retirement racing submission, and filtering
before admission limits. The real command test also changes a reserved source before
restart and verifies that only the current source is imported.

## Scope limits

This remains an opt-in local experiment using SQLite auxiliary tables. It does not
establish HA adoption, automatic recovery of uncertain submissions, production
remote isolation, or scheduler-safe discovery. Callbacks, priority requests,
stale-Dag deactivation and production migrations remain deferred. These limitations
are explicit in the command documentation and work log.

## Validation

- Final regression run: **635 passed, one existing skip**.
- Real command integration: **passed**, including restart with stale unsent work.
- Discovery, command host, orchestrator and runner: **100% statement and branch coverage**.
- Repository checks, including core/development mypy: **passed**.

No remaining actionable findings were identified within this PoC review scope.
Logs and validation details are recorded in [the work log](WORK_LOG.md).

Generated-by: Codex (GPT-6).
