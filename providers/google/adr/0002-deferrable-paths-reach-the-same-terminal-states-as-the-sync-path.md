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

This provider ships 18 triggers under `cloud/triggers/` covering BigQuery,
Dataproc, Dataflow, Cloud Run, Cloud Build, Cloud Batch, Cloud Composer,
Cloud SQL, Data Fusion, Dataplex, GCS, GKE, Pub/Sub and Vertex AI. Every one of
them exists as a *second implementation* of an operation the operator already
implements synchronously. `DataprocSubmitTrigger.run()` polls
`hook.get_job(...)` and decides that `DONE`, `CANCELLED` and `ERROR` are terminal
— a judgement the operator's non-deferrable branch makes independently, in
different code.

Two implementations of the same job semantics drift. They drift in three places
specifically:

*Terminal-state mapping.* The sync path and the trigger each own a list of states
they stop on and a rule turning those states into success or failure. A state
added to one list and not the other means the deferred task either hangs on a
finished job, or reports the wrong outcome.

*Cancellation.* A trigger is cancelled both when the user kills the task and when
the triggerer itself shuts down or reschedules the trigger. Those are not the
same event, and neither of them is success. A `run()` that catches
`asyncio.CancelledError` and simply returns leaves the operator's
`execute_complete` with no event to react to — the observed symptom is a killed
task that passes, or a task stuck in `deferred` indefinitely. `BaseTrigger.on_kill()`
(Airflow 3.3+, gated in this provider by `AIRFLOW_V_3_3_PLUS`) exists to separate
user-initiated kills from triggerer-initiated cancellation, and triggers here are
being migrated onto it.

*Serialization.* A trigger's live state does not survive a triggerer restart —
only what `serialize()` returns does. A constructor field that is not serialized
silently reverts to its default on resume: the polling interval changes, or worse,
`impersonation_chain` is lost and the resumed trigger authenticates as a different
principal than the operator intended.

The transient-error dimension compounds all three. Google APIs return 503, 429 and
409 for conditions that are not failures, and a trigger that treats a transient
503 as terminal fails a job that was merely mid-restart — while a trigger that
retries a permanent error polls forever.

The provider's fix history for these is long enough that the pattern is not
incidental; it is the dominant defect shape in this area.

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

- A user can flip `deferrable=True` on an operator and get the same outcomes, only
  without an occupied worker slot. That is the entire value proposition of the
  deferrable mode, and it does not survive behavioural drift.
- Adding a deferrable mode roughly doubles the review surface of an operator, and
  a state-mapping change becomes a two-file change by rule. This is accepted: the
  alternative is defects that only appear in production, on a restarted triggerer,
  or on a killed task.
- Some triggers carry a version-gated branch for `on_kill()` until the provider's
  Airflow floor moves past 3.3. That duplication is temporary and deliberate.

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

- #67050 — "Fix `CloudRunExecuteJobOperator` deferrable mode silently passing on
  cancel": a cancelled task reported as success. The canonical instance of this
  decision's failure mode.
- #63730 — "Fix `BigQueryInsertJobTrigger` not propagating `CancelledError`" — the
  same defect in BigQuery, and #66704 — "Migrate `BigQueryInsertJobTrigger` to
  `on_kill()` for user-initiated kills" and #65742 — "Migrate `DataprocSubmitTrigger`
  and `DataprocSubmitJobDirectTrigger` to `on_kill()`", the structural fix.
- #62082 — "fix `DataprocSubmitTrigger` deferred tasks stuck forever" and #67638 —
  "Fix `DataprocCreateBatchOperator` stuck in deferred state for a long time": the
  no-terminal-event failure mode, twice.
- #66968 — "Serialize `poll_interval` and `impersonation_chain` on
  `DataFusionStartPipelineTrigger`" and #67053 — "Preserve
  `BigQueryIntervalCheckTrigger` params after triggerer restart": fields lost across
  a restart, including a credential-scoping one.
- #67219 — "Fix Cloud Run deferrable trigger handling of transient 503" and #66293 —
  "Fix Dataflow deferrable trigger handling of transient 503": transient errors
  mis-classified as terminal, in two independent triggers.
- #61546 — "Fix deferrable mode in `CloudRunExecuteJobOperator`" and #63533 — "Fix
  `S3ToGCSOperator` deferrable mode to return list of copied files": the deferred
  path producing a different result than the sync path.
- #66355 — "Pass `schedule_timeout_seconds` through to `GKEStartPodTrigger` from
  `GKEStartPodOperator`": an operator parameter that stopped applying once the task
  deferred.
- #63230 — "wrap sync `get_job` with `sync_to_async` in `BigQueryAsyncHook`": a
  blocking call on the event loop.
- #65982 — "Add system tests for Dataproc trigger `on_kill` cancel behavior": the
  cancellation semantics covered by a test that exercises the real service.
