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

# ADR-0017: Task Loops Across Lang SDKs

## Status

Proposed.

## Terms

| Term | Means |
|---|---|
| Iteration | One pass through the loop body. Iterations count from 1. |
| Body | The tasks of the loop's task group. |
| Terminal task | The body's single leaf. Its result is that iteration's answer. |
| Gate | The task that runs after the terminal task and decides whether another iteration follows. |
| `until` | The author's callable that the gate runs. A loop without one runs a fixed count. |
| Decision | What `until` returns: stop or continue, with an optional reason. |
| Region | The row that scopes one iteration, as it scopes one expansion in [ADR-0016](0016-mapped-tasks-across-lang-sdks.md). |

## Decision

1. **Iteration creation stays in core for every language.** Regions, instance creation, rerun trajectories and the scope of every XCom read are core's. An SDK emits a loop group and runs one callable; it never creates an instance and never decides which iteration a read belongs to.
2. **A loop is a task group with four more serialized fields** — `max_iterations`, `has_until`, `terminal_task_id` and `gate_task_id`. The gate is found by `gate_task_id` and never by an operator class name, which [ADR-0007](0007-taskflow-across-language-boundary.md) decision B already rejected: a gate emitted by another SDK cannot honestly claim `LoopGateOperator`.
3. **Loop validation and normalization belong to the Dag processor**, so every language gets the same errors: one terminal task, no nested loop, no loop inside a mapped task group, no `expand` over a body task from outside, and every body-to-outside edge rewired to leave from the gate. An SDK may repeat any check for earlier feedback; core's answer is the one that counts.
4. **A fixed-count loop runs no user code.** With `has_until: false` the scheduler completes the gate itself, so counting needs no worker in any language.
5. **The decision travels on `SucceedTask.loop_decision`**, which the supervisor schema already carries. An SDK sends `{stop, reason}`; core revalidates it and fails the gate when `stop` is false at `max_iterations`.
6. **The loop context arrives on `TIRunContext.loop`** — `index`, `max_iterations`, `terminal_task_id`, `terminal_is_mapped` and the region fields. No SDK counts iterations or resolves a trajectory.
7. **Loops add no XCom call of their own.** Reading the previous iteration is the existing call with `previous_iteration: true`; reading every iteration is ADR-0016 decision 8's lazy sequence, ordered iteration first, then map index.
8. **The reason is masked in the supervisor**, once for every language. Python redacts it in the task
   process today, but the supervisor already holds whatever a task added through `MaskSecret`, so
   moving the redaction there leaves each SDK one existing call rather than a masker of its own.

## Context

Task loops land in Python with the authoring API, the serialization, the scheduler and the worker
each holding a share. An SDK that copied that split would re-implement the scheduler's half in its
own language and the two would drift, which is the same trap [ADR-0016](0016-mapped-tasks-across-lang-sdks.md)
avoids for mapped tasks.

Regions already carry the split. One expansion is scoped to its enclosing iteration at no extra
cost, so an SDK that implements both ADRs gets mapping inside a loop for free.

This ADR fixes what each side owns. What the construct looks like in an author's hands is each
SDK's own, and is recorded in that SDK's own ADR directory.

## Who runs what

| Process | Language | Its share of a loop |
|---|---|---|
| Dag processor | Python | Validates the loop and rewires the edges out of the body. |
| Scheduler | Python | Creates iteration 1, and completes a fixed-count gate with no worker. |
| API server, Execution API | Python | Sends `TIRunContext.loop`; on the gate's success, validates the decision and creates the next iteration in the same transaction. |
| Supervisor / coordinator | Python, in the worker process | Masks the reason; forwards every XCom request with the caller's iteration already pinned by its token. |
| SDK runtime | the SDK's own language | Runs the body handlers and the `until` callable, and returns the decision. |

## Authoring interface

What an author writes differs per language. These rules do not, so each SDK ADR records only its own
spelling.

1. **A loop is a task group with a bound.** No SDK adds a second kind of group.
2. **The gate is an ordinary task**, declared with the group and carrying the same task options as any
   other.
3. **The terminal task is the body's single leaf**, never something the gate names. The SDK orders the
   leaf before the gate, so a gate that binds another task still runs after the terminal. The gate's
   own data arguments are ordinary task wiring, so wiring the terminal into them is what gets the
   decision checked against what the body produces. A surface with no data wiring reads the terminal
   off the loop context instead, in the shape that SDK's `getXCom` has.
4. **The loop context arrives the way `Client` and `Context` already do** in that SDK. It is never
   mixed into a handler's data arguments.
5. **`previous` is one accessor**, in the shape that SDK's `getXCom` has. A mapped terminal makes it
   ADR-0016's sequence instead of a plain value.
6. **A body task's reference is read from outside the loop unchanged.** The consumer's declared type
   picks one iteration's value or every iteration's.
7. **Ordering edges leave from the loop, not from a body task.** A data edge out of a body task is kept
   and leaves from the gate.
8. **A plain boolean return is a decision with no reason.**

Rule 4 is where the SDKs look least alike and agree most. Python passes `loop` among the context
kwargs every handler already takes; a TypeScript handler takes only its own data, so the context is a
getter; a Go handler takes `airflow.Context` first, so it hangs off that; a Java method takes what it
declares, so it is injected. Each follows its own SDK, and none invents a rule for loops.

## What an SDK emits

A loop group is an ordinary serialized task group with one more key. The gate is an ordinary task
of that group.

```jsonc
"refine": {
  "_group_id": "refine",
  "children": {
    "refine.improve": ["task", "refine.improve"],
    "refine.evaluate": ["task", "refine.evaluate"],
    "refine.accurate_enough": ["task", "refine.accurate_enough"]
  },
  "loop": {
    "max_iterations": 10,
    "has_until": true,
    "terminal_task_id": "refine.evaluate",
    "gate_task_id": "refine.accurate_enough"
  }
}
```

`has_until` is the only thing that says whether a worker runs the gate. The callable itself is never
serialized: the gate re-enters the SDK's own bundle to find it, the way a `map()` callable is found
by ADR-0016 decision 6.

## What the wire carries

Every message a loop needs is already in Execution API `2026-10-30` and the supervisor schema that
mirrors it. An SDK implements this ADR against the protocol as it stands.

| Where | What | Means |
|---|---|---|
| `TIRunContext.loop` | `node_id`, `index`, `max_iterations`, `terminal_task_id`, `terminal_is_mapped`, `region_id`, `trajectory`, `forked_from_region_id`, `resumes_from_index` | The iteration this task runs in. Absent when the task is not in a loop. |
| `SucceedTask.loop_decision`, `TaskState.loop_decision` | `LoopDecisionPayload {stop, reason}` | What the gate decided. `reason` is at most 512 bytes of UTF-8 and holds no NUL; the payload model rejects the rest. |
| `GetXCom`, `GetXComCount`, `GetXComSequenceItem`, `GetXComSequenceSlice` | `previous_iteration: bool` | Reads the iteration before this one instead of this one. |
| `MaskSecret` | `value`, `name` | Already how a task process tells the supervisor what to redact. |
| Serialized Dag, `task_group.loop` | `max_iterations`, `has_until`, `terminal_task_id`, `gate_task_id` | Declared in `serialization/schema.json`, so an SDK's Dag validates with no schema change. |

### What changes

No field is added or removed, so **no new Execution API version is needed**. The decisions above are
core-internal, and two of them subtract from the wire rather than add to it.

| Decision | Change |
|---|---|
| 2 | Gate identification stops reading `task_instance.operator == "LoopGateOperator"` and resolves `gate_task_id` off the serialized group instead. Four call sites: the two task-instance route modules, `serialization/definitions/dag.py` and `models/taskinstance.py`. |
| 4 | A gate with `has_until: false` completes in the scheduler, so it sends no `ti_run` and no `SucceedTask` at all. |
| 8 | `redact(reason)` moves out of the task process (`execution_time/loop.py`) into the supervisor, beside the redaction it already does for logs. |

## One loop, end to end

Three iterations of `refine` — `improve` and `evaluate` in the body, `accurate_enough` as the gate,
`report` waiting outside — with `max_iterations: 10`. L is the loop region.

```
 SCHEDULER                 WORKER                                   API SERVER
 (Python)                  supervisor + SDK runtime                 (Execution API)
 ─────────                 ────────────────────────                 ───────────────
 create_dagrun
 region L, iteration 1
 improve/evaluate/gate
 created at (L, 1)
    │
    ├─ improve (L,1) ─────▶ ti_run ───────────────────────────────▶ TIRunContext.loop
    │                       { index: 1, max_iterations: 10,          no previous at index 1
    │                         terminal_task_id, terminal_is_mapped }
    │                       handler runs
    │                       SucceedTask ─────────────────────────▶ return_value XCom
    │
    ├─ evaluate (L,1) ────▶ the terminal task ────────────────────▶ return_value XCom
    │
    └─ gate (L,1) ────────▶ binding: the terminal result of (L,1)
                            until(result) → { stop: false, reason }
                            supervisor redacts reason
                            SucceedTask{loop_decision} ──────────▶ lock DagRun + gate
                                                                   validate_loop_decision
                                                                   store the reason XCom
 ┌──────────────────────────────────────────────────────────────── create the body at (L, 2)
 ▼                                                                 in the same transaction
 iteration 2 is scheduled
 like any other task
    │
    ├─ improve (L,2) ─────▶ GetXCom{ previous_iteration: true } ──▶ (L,1)'s terminal result
    │                       handler runs, SucceedTask
    │
    ├─ evaluate (L,2) ────▶ … ────────────────────────────────────▶ return_value XCom
    │
    └─ gate (L,2) ────────▶ until(result) → { stop: true }
                            SucceedTask{loop_decision} ──────────▶ validate, create nothing
 the gate succeeded, so
 what follows the loop
 becomes schedulable
    │
    └─ report ────────────▶ binding: sequence over "refine.evaluate"
                            GetXComCount / GetXComSequenceItem ──▶ every iteration, in order
```

Three things the picture fixes:

- **The gate is the loop's only exit.** `report` is downstream of the gate, not of `evaluate`, because
  decision 3 rewired that edge at parse time.
- **The next iteration is created by the API server**, inside the transaction that accepts the
  decision, not by the SDK and not by a second call.
- **A fixed-count loop skips the whole middle column.** With `has_until: false` the scheduler
  completes each gate itself (decision 4), so no worker is involved in deciding anything.

## What an SDK runtime does

Nothing here is language-specific.

1. Read `ti_context.loop` from `StartupDetails`. Its absence means the task is not in a loop.
2. Expose `index`, `max_iterations` and the previous iteration's terminal result to a body handler.
   `previous` is the existing XCom call with `previous_iteration: true`, and is absent at `index` 1.
   It is one accessor, not two: a mapped terminal makes it ADR-0016's sequence rather than a plain
   value, which is what Python's single `loop.previous` property does.
3. On the gate, read this iteration's terminal result — one value, or ADR-0016's sequence when
   `terminal_is_mapped` is true — and run `until`.
4. Send the result on `SucceedTask.loop_decision`. A `stop` of false at `max_iterations` is core's to
   reject, so the SDK sends what the callable returned. A secret the reason might carry is registered
   with `MaskSecret` as it would be for a log line; the SDK does not redact.
5. A callable that throws fails the gate like any other task body, and the loop stops there.

## Consequences

- **A loop costs an SDK one context object, one wire field and one callable.** Everything else is
  reuse: the group it is built on, the XCom calls, and the sequence ADR-0016 already defines.
- **A fixed-count loop is available before an SDK implements anything.** Decision 4 leaves no code to
  run, so `has_until: false` works as soon as the SDK can emit the group.
- **A gate's decision is advice, not authority.** Core revalidates, so a buggy or malicious SDK
  cannot run past `max_iterations`.
- **Edges out of the body are rewired, not rejected.** Decision 3 keeps Python's behaviour, so a Dag
  that an SDK emits and a Dag that Python emits serialize to the same graph. An SDK that would rather
  reject the edge at its own call site may, and both end up at the same serialized Dag.
- **Python keeps its own authoring checks.** Decision 3 moves where the error is authoritative, not
  where an author first sees it; the Task SDK's parse-time checks stay as the faster feedback.
- **Mapping inside a loop needs no further decision.** The terminal task may be mapped, and
  `terminal_is_mapped` is the only thing a runtime reads to know it.
- **No new Execution API version.** Every field a loop needs shipped with `2026-10-30`, so an SDK can
  implement this ADR before any of the core changes land, and each change can land on its own.
- **Python's reason is redacted one process later.** Decision 8 moves `redact` from the task process
  to the supervisor, which is a behaviour change for Python tasks even though no field moves: a task
  that masks a value and then puts it in a reason is covered by the `MaskSecret` it already sends, but
  one that redacts by hand before returning is unaffected either way.
