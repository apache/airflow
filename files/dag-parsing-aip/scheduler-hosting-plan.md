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

# M0 scheduler-hosting experiment

Completed the single-scheduler checkpoint on September 26, 2026. See
[implementation, measurements and remaining work](scheduler-hosting-20260926.md).

Run periodic parsing admission in the real scheduler while Celery placement and
importing remain outside its process. Use one scheduler, one dedicated parsing route,
an externally registered inventory and the existing experimental SQLite/API contracts.
This is a feasibility experiment, not HA or production scheduler support.

1. Add an explicit scheduler CLI opt-in and a metadata-only host. Reuse the processor
   ownership lock. Bound each tick's work and SQL execution; defer immediately when
   its write lock is busy. Preserve the ordinary scheduler path when disabled.
2. Reuse the executor runner in a separate process with Celery. Remote completion
   requires a successful terminal event and complete successful/import-error receipts.
   Unknown submissions and failures retain reservations; local exit evidence is never
   trusted for remote recovery.
3. Exercise actual scheduler parsing, periodic reparsing and scheduling without a Dag
   processor. Keep source and worker configuration outside the scheduler. Record step
   duration, scheduler heartbeats and task-scheduling progress during slow imports,
   stalled provider I/O and parsing transaction contention.
4. Run focused regressions, a real process/HTTP/Celery experiment and a self-review.
   Document measured results, failures and the remaining discovery/HA work.

SQL progress checks and zero busy waits bound the experiment's database work; they
cannot preempt filesystem or kernel stalls. Whole-database outages can also block
Airflow's existing scheduling transactions. The report must distinguish those from
delay introduced by the parsing callback.

Implementation belongs under `airflow-core`; experiment fixtures and measurement
drivers stay under `dev/`. Existing uncommitted recovery and measurement work is retained.
