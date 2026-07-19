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

# 1. Dependency checks are deterministic and side-effect-free

Date: 2026-07-19

## Status

Accepted

## Context

Task-instance dependencies (`BaseTIDep` subclasses in `deps/`) are the rules the
scheduler evaluates to decide whether a task instance may be queued or run. They
execute *inside* the scheduler loop, which holds a direct, privileged database
session and *never* runs user code (see `../../jobs/adr/0001`). A dependency
therefore has a narrow job: read serialized Dag structure and task-instance /
Dag-run state, and answer *met* or *not met* with a reason. Nothing else.

Two properties keep this safe. First, a dep must be **deterministic** — given
the same persisted state it must return the same answer, because the scheduler
re-evaluates deps repeatedly and multiple HA schedulers may evaluate the same
task. Second, a dep must be **side-effect-free** — evaluating whether a task
*can* run must not itself change the world. `DepContext` documents this
explicitly, and even the one sanctioned exception (`flag_upstream_failed`, which
writes `upstream_failed`/`skipped` while checking runnability) is called out in
the code as "a hack … this class should be pure (no side effects)." That single
contained wart is the ceiling, not a precedent to build on: a past bug leaked
`DepContext` mutation between evaluations and had to be fixed.

The recurring temptation is to let a dep do more than answer a question —
persist a derived flag as a shortcut, memoise across evaluations, or (the shape
`../../jobs/adr/0001` rejects) evaluate a user-supplied predicate so authors can
plug in "custom" scheduling logic. Each would make scheduling decisions depend
on evaluation order, on which scheduler ran, or on untrusted author code running
in the privileged loop.

## Decision

A task-instance dependency reads state and reports status; it does not run user
code and does not mutate persistent state as part of answering. Concretely:

- Dep evaluation is derived only from serialized Dag data and task-instance /
  Dag-run rows — never by importing the author's module or calling a
  user-supplied dependency/predicate callable.
- A dep is side-effect-free. The sole exception is the existing
  `flag_upstream_failed` transition, which is contained and documented; new deps
  must not add persistent mutations, and `DepContext` state must not leak between
  evaluations.
- Given identical persisted state, a dep returns the same `TIDepStatus` — no
  dependence on evaluation order, wall-clock beyond explicit period checks, or
  which scheduler instance is running.

## Consequences

- The scheduler loop stays trustworthy and safe to re-run and to replicate for
  HA: re-evaluating deps, or evaluating them from a second scheduler, cannot
  corrupt state or produce different answers.
- New scheduling flexibility that depends on author intent must be expressed as
  serialized, declarative data the dep reads — not as a callable the scheduler
  runs. User-defined custom `TIDep` classes are therefore not accepted.
- Any genuinely necessary state change discovered while checking runnability must
  be explicit and owned by the scheduler's own transition logic, not smuggled
  into a dep as a side effect.

A change **violates** this decision when it:

- imports author modules, or evaluates a user-supplied predicate / custom
  `TIDep` subclass, inside dep evaluation;
- adds a persistent state mutation to a dep beyond the existing, documented
  `flag_upstream_failed` transition, or lets `DepContext` carry mutation between
  evaluations;
- makes a dep's answer depend on evaluation order, on prior in-process caching,
  or on which scheduler instance evaluated it;
- replaces a deterministic read of serialized/DB state with a live re-derivation
  from author code.

## Evidence

- #62089 — "Fix `DepContext` mutation leak and restore `reschedule-mode` guard":
  a leaked mutation between dep evaluations is exactly the impurity this decision
  guards against, and the fix restored the side-effect-free contract.
- #67776 — "Fix exceptions of positional session use in airflow-core ti-deps":
  tightened the DB-session discipline these checks run under (keyword-only
  session), part of keeping dep evaluation predictable.
- The rejection of user-defined custom task-instance dependency classes is
  recorded in `../../jobs/adr/0001` — the scheduler must not evaluate
  author-supplied dependency code.
