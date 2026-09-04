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

The scheduler is Airflow's vital control loop: a single `SchedulerJobRunner`
(replicated for HA) discovers runnable Dag runs, evaluates dependencies, and
dispatches work. Two forces make it special.

- **Security.** The architecture boundary (see the project security model) treats
  user-authored Dag code as untrusted — parsed in the Dag File Processor, executed
  only in isolated workers and the triggerer, reaching the metadata DB only through
  the Execution API. The scheduler holds a direct, privileged database session; if
  author code (`python_callable`, a custom timetable branch, a dependency
  predicate, a `params` validator) ran in-process, a bad Dag would gain the
  scheduler's credentials and its position in the control loop.
- **Availability.** The loop is latency-sensitive and largely single-threaded per
  scheduler; a user callable can block, raise, leak, or spin, stalling scheduling
  for the whole cluster.

So the scheduler consumes only the *serialized* representation produced by the Dag
File Processor, reconstructing enough structure (dependencies, timetables, pools,
priorities) to decide without importing the author's module. Recurrent proposals
to let it evaluate custom dependency classes, or to move callback *payloads* that
carry user logic into the loop, look locally convenient and are what this decision
keeps saying no to.

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
- New features depending on author intent must serialize that intent (declarative
  timetable/asset data) rather than ship a callable the scheduler runs; callback
  and executor plumbing keep the *decision* in the scheduler and the *user
  payload* in the isolated executor/worker path.

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

- #63489 — "Allow direct queueing from triggerer": explored paths shifting user-payload handling toward the scheduler/trigger boundary; closed rather than blur where user workload runs.
- #37778 — "Support adding custom TI Deps": proposed user-defined dependency classes the scheduler would evaluate; not accepted.
- #61153 — "Executor Synchronous callback workload": kept callback workloads on the executor/worker side so the scheduler dispatches without running the callback body.
