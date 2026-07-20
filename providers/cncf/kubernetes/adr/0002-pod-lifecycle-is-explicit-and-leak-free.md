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

Creating a pod is cheap and reversible-looking; it is neither. A pod holds cluster
resources — CPU and memory reservations, GPUs, a node slot — until something
explicitly removes it, and it is the *only* place its logs live until they are
read. Airflow is the only party that knows when a pod should stop existing, and
Airflow is also the party most likely to lose track of it.

There are far more exit paths than the happy one. `KubernetesPodOperator`
(`operators/pod.py`) alone can leave the pod via `execute_sync` completing,
`execute_sync` raising, `on_kill` (the user marked the task failed or the worker
was terminated), `execute_async` deferring and `trigger_reentry` resuming, the
trigger timing out, or the operator's own `execution_timeout` firing while the
pod is still starting. The executor path adds more: the watcher observing a
terminal phase, `_adopt_completed_pods` patching a finished pod so the current
scheduler's watcher will delete it, and `cleanup_stuck_queued_tasks` /
`revoke_task` removing a pod for a task the scheduler has given up on. Each of
those is a separate place where the question "does the pod still exist, and
should it?" has to be answered.

Two opposite mistakes are both common. **Leaking** — an exception on the failure
path skips deletion — burns quota indefinitely and, worse, can leave a container
still writing to external systems for a task Airflow has already marked failed.
**Deleting too eagerly** destroys the only copy of the evidence: once the pod is
gone, so are its logs, its events, its container termination reasons and its XCom
sidecar contents. A user debugging a failed task then has a state transition and
nothing else.

The ordering constraint is therefore not stylistic. `process_pod_deletion` runs
after `_read_pod_events`, `_read_pod_container_states`, `_write_logs` and
`extract_xcom` deliberately: log streaming happens against a live pod, and the
XCom sidecar has to be read before the pod is torn down. Anything that hoists
deletion earlier, or that lets an exception in the collection step fall through
into deletion, converts a debuggable failure into an opaque one.

Because deletion is destructive, it is also user-configurable rather than
implicit. `OnFinishAction` (`delete_pod`, `delete_succeeded_pod`, `keep_pod`,
`delete_active_pod`) and `OnKillAction` are public operator parameters, and
`process_pod_deletion` is where they are honoured. These are contracts users
build operational practice around — a deployment that relies on `keep_pod` for
post-mortem inspection breaks if a code path decides on its own to clean up.

The deferrable path multiplies all of this. When the operator defers, the process
that created the pod goes away and the triggerer takes over
(`triggers/pod.py`); the pod may be garbage-collected, evicted or preempted
between polls, and the cleanup decision has to be reachable from both the trigger
and the resumed operator without being taken twice. This is the seam where
lifecycle bugs concentrate.

Finally, "leak" is not only about pods. The executor runs inside the scheduler,
so a helper process, queue, thread or watcher acquired per operation and never
released is a scheduler-lifetime leak with the same shape and a wider blast
radius.

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
- Quota does not drift. A deployment does not accumulate orphaned pods from tasks
  that failed in unusual ways, and the scheduler does not accumulate helper
  processes.
- Cleanup code is verbose. The same keep-or-delete reasoning appears in the
  synchronous operator, the trigger, and the executor, because those are genuinely
  three lifecycles — the alternative is one of them silently diverging.
- Some failures cost an extra API round trip (re-reading a pod that may be gone)
  rather than being handled by assumption.
- `keep_pod` deployments must do their own garbage collection; that is an accepted
  consequence of making retention a user decision.

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

- #61839 — "k8s executor - ensure pods cleaned up": the baseline leak this
  decision exists to prevent.
- #67333 — "Fix monitoring-pod leak in KubernetesJobOperator": the same leak
  shape re-appearing in a sibling operator.
- #68800 — "Fix KubernetesExecutor leaking a Manager process when reading running
  task logs": a host-side leak, per log read, inside the scheduler process.
- #52662 — "Kill kube watcher instance if it doesnt terminate gracefully in 60
  seconds" and #63789 — "K8s: use joinable manager queues": watcher and queue
  teardown made bounded rather than best-effort.
- #64962 — "Consider XCOM sidecar container during pod cleanup": cleanup that has
  to account for the sidecar before tearing the pod down.
- #59160 — "Add `delete_active_pod` cleanup option": retention behaviour extended
  as an explicit user-facing option rather than an implicit rule.
- #65741 — "Move KubernetesPodTrigger pod cleanup from cleanup() to on_kill()":
  the deferrable path's cleanup relocated to the exit path that actually fires.
- #62401 — "Add cancel_on_kill and safe_to_cancel support to KubernetesPodOperator
  and trigger": kill semantics made explicit and consistent across operator and
  trigger.
- #66716 — "Fix deferrable KPO trigger_reentry crash when pod is GC'd before
  re-entry" and #56976 — "improve deferrable KPO handling of deleted pods in
  between polls": pod absence treated as a normal outcome.
- #68328 — "Kubernetes Pod Operator - handle pod preemption before container
  creation": a pod that dies before it ever ran, handled rather than assumed away.
- #55479 — "Add more error handling in pod_manager consume_logs", #54761 —
  "Throttle HTTPError during consume pod logs", #64471 — "Add retries for
  `_write_logs` method in `KubernetesPodOperator`", and #67652 — "Fix
  KubernetesPodOperator emitting orphan timestamps for empty container writes":
  log streaming hardened to degrade instead of failing the task.
