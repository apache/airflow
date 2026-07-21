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

# 5. New scheduling behaviour uses the existing dep set and state, not a new knob

Date: 2026-07-20

## Status

Accepted

## Context

The set of `TIDep` classes is small and closed by intent. Each is a named,
documented reason a task instance may not run yet, and together they form the UI's
answer to "why is my task not running?". Every addition enlarges the surface a
maintainer must reason about, and — because deps run for every task instance on
every pass — is a cost paid in the hot loop (`adr/0002`).

The pressure is one-directional: users want a new way to express a dependency, and
the first draft is a new user-facing parameter plus a new dep. A
`depends_on_previous_task_ids` parameter arrived twice and neither landed —
`depends_on_past` already carries a cross-run dependency, so the discussion was
whether the gap justified a second one. A proposal to run backfills while a Dag stays
paused introduced a `keep_dag_paused` field; review removed it, keying off
`DagRun.backfill_id` instead. A new dep or parameter is cheap to add and permanent to
remove, and the reason for refusal is not in the diff — it needs knowing that
`depends_on_past`, the trigger rules, pool state, or `backfill_id` already encode the
distinction. Larger reshapings (a task-group-retries PoC) get the same answer: a big
change to what task groups *are* belongs on the dev list first.

## Decision

New scheduling behaviour is expressed through the existing dep set and existing
recorded state wherever that is possible. Concretely:

- **Derive from state the system already records.** Before adding a field or
  parameter, check whether existing state (`backfill_id`, run type, task instance
  state, pool, `depends_on_past`) already distinguishes the case.
- **A new `TIDep` needs a reason no existing dep covers**, stated explicitly, plus
  an account of its per-task-instance cost in the scheduling loop.
- **A new user-facing dependency parameter is a semantic change**, not a feature
  addition: it goes to the dev list or an AIP before implementation, and it must
  say why `depends_on_past` and the trigger rules do not express the need.
- **Reshaping what an existing construct means** — task groups, trigger rules, the
  run/instance relationship — is discussed before the code, not during review of
  it.
- **The dep's failure message is part of the change.** A new or altered dep must
  produce a reason string that tells a user what to do, because that string is the
  primary diagnostic surface for stuck tasks.

## Consequences

- Real user needs are sometimes answered with "restructure the Dag" or "use
  `depends_on_past`" — the trade for a scheduling contract small enough to reason
  about.
- Contributors with a working implementation may find the discussion is whether the
  feature should exist at all; directing that to the dev list first saves the effort.
- Keeping the dep set closed pushes complexity into the existing deps, which grow
  conditionals — why the defects in `adr/0004` and `adr/0001` cluster in the same
  classes.
- The scheduling loop stays predictable in cost, since the deps-per-instance count
  does not drift upward release over release.

A change **violates** this decision when it:

- adds a user-facing parameter expressing a dependency that `depends_on_past` or
  an existing trigger rule already covers, without arguing the gap;
- adds a field to carry state the system already records elsewhere (for example a
  paused-state flag when `backfill_id` already distinguishes the run);
- adds a `TIDep` without stating which existing dep fails to cover the case and
  what it costs per task instance;
- changes the meaning of an existing construct (task group, trigger rule, run
  identity) in a pull request rather than in a prior discussion;
- adds or changes a dep without a corresponding user-facing reason string.

A reviewer should ask: what recorded state already distinguishes this case, and
what does the user see in the UI when this new dep blocks their task?

## Evidence

- #60385 — "Add `depends_on_previous_task_ids` parameter ..." and #60342 — "Add
  `prev_task_ids` for depends on past": two attempts at the same cross-run parameter,
  neither merged; discussion turned on why `depends_on_past` is insufficient.
- #60818 — "Add support for running backfill while keeping Dag paused": closed after
  review removed the `keep_dag_paused` field for keying off `DagRun.backfill_id`.
- #61809 — "PoC of task group retries": closed; needed a dev-list discussion first.
- #53959 — "Add `ALL_DONE_MIN_ONE_SUCCESS` trigger rule": the merged shape — a new
  rule in the existing vocabulary, not a new parameter and dep.
- #61227 — "Multi-team: verify a task uses a pool it has access to": a merged
  dep-level change through the existing pool-state check.
- #54774 — "Fix trigger rule error messages showing enum names instead of values":
  treats the reason string as contract.
