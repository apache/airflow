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

# 2. Deferrable paths reach the same terminal states as the sync path

Date: 2026-07-20

## Status

Accepted

## Context

This provider ships 18 triggers under `cloud/triggers/` (BigQuery, Dataproc,
Dataflow, Cloud Run, Cloud Build, Cloud Batch, Cloud Composer, Cloud SQL, Data
Fusion, Dataplex, GCS, GKE, Pub/Sub, Vertex AI), each a *second implementation* of
an operation the operator already implements synchronously — and two
implementations of the same job semantics drift, in three places specifically:

*Terminal-state mapping.* Sync path and trigger each own a stop list and a
success/failure rule; a state added to one and not the other means the deferred
task hangs on a finished job or reports the wrong outcome.

*Cancellation.* A trigger is cancelled both when the user kills the task and when
the triggerer shuts down or reschedules — not the same event, and neither is
success. A `run()` that catches `asyncio.CancelledError` and returns leaves
`execute_complete` with no event, so a killed task passes or stays `deferred`
forever. `BaseTrigger.on_kill()` (Airflow 3.3+, gated by `AIRFLOW_V_3_3_PLUS`)
separates user kills from triggerer cancellation; triggers here are migrating onto
it.

*Serialization.* Only what `serialize()` returns survives a triggerer restart; an
unserialized constructor field reverts to its default on resume — the poll interval
changes, or `impersonation_chain` is lost and the trigger authenticates as a
different principal.

Transient errors compound all three: Google APIs return 503/429/409 for
non-failures, so treating a transient 503 as terminal fails a mid-restart job while
retrying a permanent error polls forever. This is the dominant defect shape here.

## Decision

A deferrable implementation is a different *execution mechanism* for the same
operation, never a different *behaviour*. The operator's completion and error
semantics are the specification; the trigger conforms to them.

- **Every exit path of `run()` yields a terminal `TriggerEvent`.** Success,
  service error, timeout and cancellation all produce an event. A `run()` that can
  return or raise without yielding is a bug: the task stays deferred.
- **Cancellation is never reported as success.** `asyncio.CancelledError` is
  either re-raised or converted into an explicit failure/cancelled event, after any
  necessary remote cleanup. Use `on_kill()` for user-initiated cancellation on
  Airflow 3.3+, gated by `AIRFLOW_V_3_3_PLUS`, keeping the pre-3.3 path working
  behind the gate.
- **The trigger's terminal-state set matches the operator's.** Adding, removing or
  reclassifying a state is done in both paths, in the same change.
- **`serialize()` round-trips every field the resumed trigger needs** — polling
  interval, `gcp_conn_id`, `impersonation_chain`, `project_id`, `region`,
  `cancel_on_kill`, and anything else the constructor accepts and `run()` reads.
- **Transient service errors are retried, permanent ones are not.** 503 / 429 /
  in-progress 409 are retried with backoff; a 4xx that will never succeed
  terminates the trigger with a failure event.
- **Nothing blocks the event loop.** Synchronous hook calls used from a trigger are
  wrapped in `sync_to_async`; the async hook is obtained through
  `GoogleBaseAsyncHook` so it shares the credential chain of ADR 1.
- **A trigger change ships with trigger tests** covering the terminal states, the
  cancellation path and the `serialize()` round-trip.

## Consequences

- A user can flip `deferrable=True` and get the same outcomes without an occupied
  worker slot — the whole value proposition, and it does not survive drift.
- A deferrable mode roughly doubles an operator's review surface and makes a
  state-mapping change a two-file change by rule. Accepted; the alternative is
  defects that only appear on a restarted triggerer or a killed task.
- Some triggers carry a version-gated `on_kill()` branch until the Airflow floor
  moves past 3.3 — temporary and deliberate.

A change **violates** this decision when it:

- adds or modifies a trigger whose `run()` has a path that returns, breaks or
  raises without yielding a terminal `TriggerEvent`;
- catches `asyncio.CancelledError` and lets the task complete successfully, or
  swallows it without cleanup and without a failure event;
- changes the terminal-state or success/failure mapping in the operator without
  making the same change in the trigger (or the reverse);
- adds a constructor parameter to a trigger without adding it to `serialize()`;
- treats a transient 503 / 429 / in-progress 409 as a terminal failure, or retries
  a permanent error without bound;
- calls a blocking hook method directly from an async trigger path instead of
  wrapping it in `sync_to_async`;
- adds a deferrable mode with no test for the cancellation and serialization paths.

## Evidence

- #67050 — `CloudRunExecuteJobOperator` deferrable silently passing on cancel: the
  canonical instance of this failure mode.
- #63730 — same `CancelledError` defect in BigQuery; #66704, #65742 — the
  structural fix, migrating BigQuery/Dataproc triggers to `on_kill()`.
- #62082, #67638 — the no-terminal-event failure mode (Dataproc stuck deferred),
  twice.
- #66968, #67053 — fields (poll interval, `impersonation_chain`) lost across a
  triggerer restart, including a credential-scoping one.
- #67219, #66293 — transient 503 mis-classified as terminal, in two independent
  triggers.
- #61546, #63533 — deferred path producing a different result than the sync path.
- #66355 — `schedule_timeout_seconds` that stopped applying once the task deferred.
- #63230 — wrapped blocking `get_job` in `sync_to_async` off the event loop.
- #65982 — system test covering `on_kill` cancel against the real service.
