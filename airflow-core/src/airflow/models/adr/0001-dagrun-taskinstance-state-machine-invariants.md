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

# 1. The DagRun/TaskInstance state machine is authoritative and enforced in the models

Date: 2026-07-18

## Status

Accepted

## Context

`DagRun` and `TaskInstance` carry the run/task lifecycle state (`queued`,
`running`, `success`, `failed`, `up_for_retry`, `up_for_reschedule`, `skipped`,
`removed`, `deferred`, `restarting`, etc.). Almost every other component acts on
this state without re-deriving it: the scheduler decides what to schedule next
from the set of TI states under a DagRun; the executor reports terminal states
back; the API and UI render and mutate state through endpoints; retries,
timeouts, and clearing all mutate it.

The invariants that hold this together are **not locally obvious from a diff**.
A DagRun's state is a function of its TI states (a run cannot be `success` while
a TI is still `running`; a `deferred` TI must have a live trigger; a mapped TI's
`map_index` and expansion count must agree). When a change adds a new state,
adds a new way to reach an existing state, or short-circuits a transition, the
reviewer cannot see from the changed lines alone whether a run can now settle
into an inconsistent combination. The cost of getting this wrong is a stuck or
silently-wrong DagRun that the scheduler will never resolve.

## Decision

State transitions and the guards that protect them live in the models
(`TaskInstance`, `DagRun`, and the helpers they call — e.g.
`set_state`, `handle_failure`, `update_state`, `schedulable_tis` /
`_get_ready_tis`, and the trigger/reschedule paths). They are the single source
of truth for what a valid transition is.

- A transition is only added or changed together with the guard that keeps the
  documented invariants true (terminal states are final unless explicitly
  cleared; a run's aggregate state is consistent with its leaf/teardown TI
  states; deferred/reschedule states have their backing rows; mapped-task
  bookkeeping stays consistent).
- Every new or changed transition is **justified in the PR** and covered by a
  test that **fails without the change** — asserting the resulting state, not
  just that the call did not raise.
- Callers outside the models (scheduler, API, executor) go through these model
  methods rather than writing state columns directly to reach a shortcut.

## Consequences

- The state machine stays reasoning-about-able: a reader can trust that a run in
  a terminal state is genuinely done and a scheduled run's TI set is coherent.
- New lifecycle features (new states, new hold/wait mechanisms) carry their
  guards and tests with them, so the invariants survive the addition.

A **violating** change looks like: introducing a state write that bypasses the
model transition methods; adding a transition with no guard so a TI can move out
of a terminal state or a DagRun can report `success`/`failed` while leaf TIs are
still runnable or `deferred`; letting a mapped TI's `map_index`/expansion count
diverge; or adding any of these with no test that would fail if the transition
were wrong. Reaching an inconsistent (TI, DagRun) combination is the defect this
ADR exists to prevent.

## Evidence

- #64522 — Add a way to mark a return value XCom as dag result: new
  result-carrying behaviour threaded through the TI lifecycle with tests.
- #61550 — Add the option to select bundle version on dag run trigger endpoint:
  a run-creation path that must keep the new DagRun consistent from the start.
