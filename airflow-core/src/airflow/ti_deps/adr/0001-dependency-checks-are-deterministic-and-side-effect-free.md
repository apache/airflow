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
scheduler evaluates to decide whether a task instance may be queued or run. They run
*inside* the scheduler loop, which holds a privileged DB session and *never* runs
user code (see `../../jobs/adr/0001`). A dep's job is narrow: read serialized Dag
structure and task-instance / Dag-run state, and answer *met* or *not met* with a
reason.

Two properties keep this safe. A dep must be **deterministic** — the same persisted
state gives the same answer, because the scheduler re-evaluates repeatedly and HA
schedulers may evaluate the same task. And a dep must be **side-effect-free** —
checking whether a task *can* run must not change the world. Even the one sanctioned
exception (`flag_upstream_failed`) is flagged in code as "a hack … this class should
be pure (no side effects)"; a past bug that leaked `DepContext` mutation between
evaluations had to be fixed. The recurring temptation — persist a derived flag,
memoise across evaluations, or evaluate a user-supplied predicate (the shape
`../../jobs/adr/0001` rejects) — each makes decisions depend on evaluation order, on
which scheduler ran, or on untrusted author code in the privileged loop.

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

- The scheduler loop stays safe to re-run and replicate for HA: re-evaluating deps,
  or evaluating from a second scheduler, cannot corrupt state or change answers.
- Scheduling flexibility that depends on author intent must be serialized,
  declarative data the dep reads — not a callable the scheduler runs; user-defined
  custom `TIDep` classes are therefore not accepted.
- Any state change needed while checking runnability must be explicit and owned by
  the scheduler's transition logic, not smuggled into a dep.

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

- #62089 — "Fix `DepContext` mutation leak and restore `reschedule-mode` guard": a
  leaked mutation between evaluations is exactly the impurity this guards against.
- #67776 — "Fix ... positional session use in airflow-core ti-deps": tightened
  DB-session discipline (keyword-only session).
- User-defined custom task-instance dependency classes are rejected in
  `../../jobs/adr/0001` — the scheduler must not evaluate author-supplied dep code.
