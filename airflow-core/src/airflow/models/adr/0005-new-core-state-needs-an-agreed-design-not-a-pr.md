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

# 5. New core state needs an agreed design first, and must not duplicate existing semantics

Date: 2026-07-20

## Status

Accepted

## Context

The models here are Airflow's public vocabulary. A field on `DagRun`, a
`TaskInstanceState` value, a new lifecycle behaviour, a new listener hook — each is
a permanent addition to what every operator, provider, UI surface, and downstream
tool must understand, and cannot be walked back in a patch release.

Two failure modes account for most closed PRs, with the same root — the design was
settled *in* the PR rather than before it. **The semantics already exist under
another name:** a proposed `target_date` on `DagRun` was exactly
`data_interval_start` / `data_interval_end`; a proposed `CHECKPOINTED` state and its
exception were unnecessary once the accepted task-state work showed the same
recovery through the existing `failed → up_for_retry` flow. **The change is a
semantic shift dressed as a feature:** Dag-run-level retries, task-group retries,
and run-level dependency semantics change what a run *means*; arriving as complete
PRs with tests does not make the first question — *should Airflow work this way?* —
cheaper to answer in a review thread than on the dev list or in an AIP. The answer
either way is "not here, not yet, and not in this order".

## Decision

- **Before adding a field, state, exception, or listener hook to a core model,
  identify the existing concept that already carries the meaning** — timetables
  and `data_interval` for run timing, `run_type` and `backfill_id` for run
  provenance, `try_number` and the retry flow for recovery, `task_state` for
  resumability — and explain in the PR body why it is genuinely insufficient.
- **A change to what a Dag run or task instance *means* goes to the dev list or
  an AIP first**, and the PR references the resulting agreement. Run-level
  retries, new lifecycle states, new dependency semantics, and new expansion
  models are all in this class.
- **Ship the smallest primitive that unblocks the use case.** If an existing hook
  plus a lookup covers it, that beats a new hook; if a keyword on an existing
  hook covers it, that beats a new state.
- **Do not bundle an unagreed concept with its plumbing.** A field, its migration,
  its serialization, its API exposure, and its UI are reviewable together only once
  the field itself is agreed. Before that, the extra layers make the review longer
  without making the design question any easier to answer.

This ADR governs **columns and enum values on the core models**. The parallel
question for attributes on the *serialized* Dag — including the
`run_on_latest_version` configuration hierarchy — is decided in
`../../serialization/adr/0005`; a change that only touches the serialized document
is judged there, not twice.

## Consequences

- Contributors with a real need wait longer, and some proposals are refused — the
  intended trade: an unneeded `dag_run` column is forever, a delayed feature is not.
- Reviewers answer the design question before reading an implementation, the only
  point at which the answer is cheap.
- The vocabulary stays small enough that the ADR 1 state-machine invariants remain
  statable; large features still land via an AIP that becomes the review's reference.

A change **violates** this decision when it:

- adds a column to `DagRun`, `TaskInstance`, `DagModel`, or another core model
  **without naming, in the PR body, the existing concept it was checked against**
  (run timing, run provenance, retry position, resumability) and why that concept
  is insufficient;
- adds a value to a state enum, a new `Airflow*` exception in the task lifecycle,
  or a new listener hook without an accepted design that calls for it;
- changes what a Dag run or task group *is* — run-level retries, group-level
  retries, new dependency or expansion semantics — with no linked dev-list thread
  or AIP;
- arrives as a large multi-layer implementation (model + migration + API + UI)
  for a concept that has not been agreed;
- stores a second, derived copy of state that another table or field is already
  authoritative for — a digest or cache column whose only purpose is to avoid
  loading the authoritative value is in scope when the PR states how it is kept
  in sync;
- persists data the metadata database is not the right home for — credentials,
  unbounded or opaque blobs — instead of fixing the layer that produced it.

Reviewer prompts — the judgement behind the first bullet, which the diff states but
does not settle:

- Does an existing field already express this? The author's answer is the artefact
  the bullet requires; whether it is *right* is the reviewer's call, and often turns
  on knowing that `data_interval` means what the proposer calls something else.
- Where was this agreed?

## Evidence

- #67329 — "Add `target_date` ... processing date for Dag runs": declined;
  `data_interval_start` / `data_interval_end` already carry that meaning.
- #63907, #61336 — Dag-level automatic retries, twice: both closed as needing
  discussion — run-level retry changes what a run means.
- #61809 — task-group retries PoC: judged a large change to what task groups are;
  dev-list discussion asked for first.
- #66402, #66410, #66445 — a proposed `CHECKPOINTED` state, hook, and plumbing: all
  closed once the accepted task-state work showed recovery via the existing flow.
- #48868, #42572 — XCom filtering and a streaming iterable operator: closed pending
  the lazy-task-expansion AIP.
- #54511 — defensive handling plus an index for concurrent rendered-field inserts:
  closed — reported once in years; changes wait for a demonstrated pattern.
