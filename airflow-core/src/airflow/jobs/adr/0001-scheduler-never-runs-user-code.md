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

# 1. The scheduler never runs user code

Date: 2026-07-18

## Status

Accepted

## Context

The scheduler is Airflow's vital control loop. A single `SchedulerJobRunner`
(replicated for HA) drives the entire cluster: it discovers runnable Dag runs,
evaluates task-instance dependencies, and dispatches work to executors. Two
forces make this loop special.

- **Security.** Airflow's architecture boundary (see the project security
  model) states that user-authored Dag code is untrusted. It is parsed in the
  Dag File Processor and executed only in workers and the triggerer — all of
  which are isolated and reach the metadata DB only through the Execution API.
  The scheduler, by contrast, holds a direct, privileged database session. If
  arbitrary author code — a `python_callable`, a custom timetable branch, a
  user-supplied dependency predicate, a `params` validator — were imported and
  executed inside the scheduler process, a malicious or buggy Dag would gain the
  scheduler's database credentials and its position inside the control loop.
- **Availability.** The loop is latency-sensitive and largely single-threaded
  per scheduler. Any user callable it evaluates can block, raise, leak memory,
  or spin — and one bad Dag would then stall scheduling for the whole cluster,
  not just its own tasks.

For these reasons the scheduler consumes only the *serialized* representation of
Dags produced by the Dag File Processor. It reconstructs enough structure to
make scheduling decisions (dependencies, timetables evaluated from serialized
data, pools, priorities) without ever importing the author's Python module.

The history in this area is a series of proposals that would have punched a hole
in this boundary and were redirected. Requests to let the scheduler evaluate
custom task-instance dependency classes, or to move executor/trigger callback
*payloads* (which can carry user logic) into the scheduler loop, are recurrent
because they look locally convenient — the scheduler already has the row in
front of it. The decision below is what keeps saying no to that shape.

## Decision

The scheduler reads serialized Dags and creates Dag runs and task instances,
but it never imports or executes Dag-author (user) code. All evaluation of user
code happens off the scheduler:

- **Dag parsing / import** happens in the Dag File Processor.
- **Task execution** (operators, `python_callable`, hooks) happens in workers
  via the Task SDK.
- **Deferred / sensor evaluation** happens in the triggerer.

Scheduling decisions in the loop must be derived exclusively from serialized,
data-only structures already persisted by the Dag File Processor. When new
scheduling flexibility is needed, the extension point must consume serialized
data or delegate the user-code evaluation to an isolated component — it must not
run the callable in the scheduler.

## Consequences

- The scheduler stays trustworthy and fast: a broken or hostile Dag cannot crash
  the control loop or exfiltrate the scheduler's DB session.
- New scheduling features that depend on author intent must serialize that
  intent (e.g. as declarative timetable/asset data) rather than shipping a
  callable the scheduler runs.
- Callback and executor plumbing must keep the *decision* in the scheduler and
  the *user payload* in the isolated executor/worker path.

A change **violates** this decision when, inside the scheduler loop or any code
reachable from `SchedulerJobRunner`, it:

- imports the user's Dag module / calls `DagBag`-style loading of author files,
  or dereferences a serialized field back into live author code;
- evaluates a user-supplied callable — custom `TIDep` subclasses, timetable
  branches that run author code, `python_callable`, `on_*_callback` bodies,
  `params` validators, custom priority-weight callables — in-process;
- adds an extension point whose contract is "the scheduler calls this
  user-provided function";
- moves a workload/payload that embeds user code from the executor/worker/
  triggerer path into the scheduler process.

A reviewer should reject any scheduler-loop diff that turns serialized data back
into executed author code.

## Evidence

- #63489 — "Allow direct queueing from triggerer": explored queueing paths that
  would have shifted user-payload handling toward the scheduler/trigger boundary;
  closed rather than blur where user workload runs.
- #37778 — "Support adding custom TI Deps to help DagRun make more flexible TI
  scheduling decisions": proposed user-defined task-instance dependency classes
  the scheduler would evaluate; not accepted, because the scheduler must not run
  author-supplied dependency code.
- #61153 — "Executor Synchronous callback workload": kept callback *workloads* on
  the executor/worker side so the scheduler dispatches without executing the
  user callback body in its own loop.
