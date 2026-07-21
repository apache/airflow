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

# 2. Pod lifecycle is explicit and leak-free

Date: 2026-07-20

## Status

Accepted

## Context

Creating a pod looks cheap and reversible; it is neither. A pod holds cluster
resources until something explicitly removes it, and it is the *only* place its
logs live until they are read. Airflow is the only party that knows when a pod
should stop existing, and the party most likely to lose track of it — there are
far more exit paths than the happy one. `KubernetesPodOperator` alone leaves the
pod via `execute_sync` completing or raising, `on_kill`, `execute_async`
deferring and `trigger_reentry` resuming, the trigger timing out, or
`execution_timeout` firing while the pod is still starting; the executor adds the
watcher, `_adopt_completed_pods`, and `cleanup_stuck_queued_tasks` / `revoke_task`.

Two opposite mistakes are common. **Leaking** — an exception skips deletion —
burns quota and can leave a container writing to external systems for a task
Airflow already marked failed. **Deleting too eagerly** destroys the only copy of
the evidence: logs, events, termination reasons, XCom sidecar contents. So
`process_pod_deletion` runs deliberately after `_read_pod_events`,
`_read_pod_container_states`, `_write_logs` and `extract_xcom`; hoisting deletion
earlier, or letting a collection-step exception fall through into it, turns a
debuggable failure opaque. Deletion is therefore user-configurable
(`OnFinishAction`: `delete_pod`, `delete_succeeded_pod`, `keep_pod`,
`delete_active_pod`; `OnKillAction`) — contracts users build practice around. The
deferrable path multiplies this: the creating process goes away, the triggerer
takes over (`triggers/pod.py`), the pod may be GC'd/evicted/preempted between
polls, and the cleanup decision must be reachable from both trigger and resumed
operator without being taken twice. And "leak" is not only pods — the executor
runs inside the scheduler, so a helper process, queue, thread or watcher acquired
per operation and never released is a scheduler-lifetime leak.

## Decision

Every path that can create or abandon a Kubernetes resource decides its fate
explicitly, and that decision happens only after the resource's observable state
has been captured. Concretely:

- **Every exit path resolves the pod.** Success, failure, timeout, `on_kill`,
  trigger re-entry, executor revoke, and adoption-of-a-completed-pod each reach a
  deliberate keep-or-delete decision. "Falls out of scope" is not a decision.
- **Collect before deleting.** Logs, pod events, container states and XCom are
  read while the pod exists; deletion follows. A failure inside collection is
  logged and degraded, and must not skip the deletion decision — nor be repaired
  by moving deletion earlier.
- **`on_finish_action` and `on_kill_action` are honoured on every path**,
  including the deferrable one. No code path may delete a pod the user asked to
  keep, or retain one the user asked to delete, because of where in the lifecycle
  it happened to be.
- **Absence is a normal outcome, not an error.** A pod that has been evicted,
  preempted or garbage-collected between polls is handled; the cleanup code does
  not assume the pod it created is still there.
- **Log streaming degrades, never propagates.** API errors, throttling,
  undecodable bytes, empty writes and a stream that ends early affect log
  collection only. They never fail an otherwise-successful task and never abort
  cleanup.
- **The same rule applies to host-side resources.** Processes, managers, queues,
  threads and watchers created by the executor are released or reused. Nothing is
  allocated per API call and left to the garbage collector.

## Consequences

- A failed task is diagnosable: its logs and events exist because they were read
  before anything was deleted.
- Quota does not drift, and the scheduler does not accumulate helper processes.
- Cleanup code is verbose — the same keep-or-delete reasoning appears in the
  operator, the trigger, and the executor, because those are three genuinely
  distinct lifecycles.
- Some failures cost an extra API round trip (re-reading a pod that may be gone)
  rather than being handled by assumption.
- `keep_pod` deployments do their own garbage collection — an accepted consequence
  of making retention a user decision.

A change **violates** this decision when it:

- adds or modifies an exit path — an `except` branch, a timeout, a trigger event,
  a revoke — that leaves a created pod without a keep-or-delete decision;
- deletes a pod before its logs, events, container states or XCom have been
  collected, or moves deletion ahead of collection to simplify control flow;
- ignores `on_finish_action` / `on_kill_action` on any path, or hardcodes a
  deletion behaviour that overrides them;
- treats a missing pod as an error that aborts cleanup, rather than as an expected
  outcome of eviction, preemption or garbage collection;
- lets a log-streaming, event-reading or XCom-reading failure propagate far enough
  to fail a successful task or skip cleanup;
- allocates a process, manager, queue, thread or watcher per operation in the
  executor without a release path, or leaves a watcher that will not terminate.

A reviewer should reject any change to pod creation, cleanup, or the deferrable
path whose behaviour on the failure and kill paths is unstated.

## Evidence

- #61839 — the baseline pod leak this decision exists to prevent.
- #67333 — the same leak shape re-appearing in `KubernetesJobOperator`.
- #68800 — a host-side leak (a Manager process per log read) inside the scheduler.
- #52662, #63789 — watcher and queue teardown made bounded rather than best-effort.
- #64962 — cleanup accounting for the XCom sidecar before tearing the pod down.
- #59160 — retention extended as an explicit user-facing option.
- #65741 — deferrable cleanup relocated to the exit path that actually fires.
- #62401 — kill semantics made explicit and consistent across operator and trigger.
- #66716, #56976 — pod absence between polls treated as a normal outcome.
- #68328 — a pod that dies before it ran, handled rather than assumed away.
- #55479, #54761, #64471, #67652 — log streaming hardened to degrade instead of
  failing the task.
