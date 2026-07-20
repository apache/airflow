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

The set of `TIDep` classes is small and closed by intent. Each one is a named,
documented reason a task instance may not run yet, and together they form the
answer the UI gives a user asking "why is my task not running?". Every addition
to that set enlarges the surface a scheduler maintainer must reason about, and —
because deps are evaluated for every task instance on every scheduling pass —
every addition is also a cost paid in the hot loop (`adr/0002`).

The pressure on this area is almost entirely one-directional: users want a new
way to express a dependency, and the natural first draft is a new user-facing
parameter with a new dep to enforce it. Proposals for a
`depends_on_previous_task_ids` parameter — a per-task dependency on specific tasks
from the previous Dag run — arrived twice from different contributors, with a
genuinely detailed use case (a stateful file-ingestion pipeline where the same
source file must not be reprocessed until the previous run completes). Neither
landed. The existing model already carries a cross-run dependency concept in
`depends_on_past`, and the discussion turned on whether the gap justified a second
one rather than on whether the code worked.

The same correction shows up in a milder form on state. A proposal to allow
backfills to run while a Dag stays paused introduced a `keep_dag_paused` field to
carry the new behaviour; review had the field removed entirely, and the change
reworked to key off `DagRun.backfill_id` — state the system already records — plus
the existing unpause path. The behaviour survived; the new knob did not.

Two forces make this decision worth writing down. First, a new dep or parameter is
cheap to add and permanent to remove: it becomes part of the scheduling contract
users write Dags against. Second, the reason a proposal is refused is usually not
visible in its diff — it requires knowing that `depends_on_past`, the trigger
rules, pool state, or `backfill_id` already encode the distinction, which is
knowledge concentrated in a few reviewers.

Larger reshapings of scheduling semantics have the same outcome by a different
route: a proof-of-concept for task-group-level retries was met with the observation
that it is a large change to what task groups *are*, and belongs on the dev list
before the code.

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
  `depends_on_past`", which is unsatisfying when the use case is legitimate. The
  project accepts that in exchange for a scheduling contract that stays small
  enough to reason about.
- Contributors arriving with a working implementation may find the discussion is
  about whether the feature should exist at all. Directing that conversation to
  the dev list first saves the implementation effort, which is why the decision
  says so explicitly.
- Keeping the dep set closed pushes complexity into the existing deps, which grow
  conditionals over time. That is a real cost, and it is why the per-index and
  mutation-leak defects in `adr/0004` and `adr/0001` cluster in the same few
  classes.
- The scheduling loop stays predictable in cost, since the number of deps
  evaluated per task instance does not drift upward release over release.

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

- #60385 — "Add `depends_on_previous_task_ids` parameter to allow tasks to depend
  on specific tasks from previous Dag runs" and #60342 — "Add `prev_task_ids` for
  depends on past": two independent attempts at the same new cross-run dependency
  parameter, neither merged, with the discussion turning on why `depends_on_past`
  is insufficient.
- #60818 — "Add support for running backfill while keeping Dag paused": closed
  after review had the new `keep_dag_paused` field removed entirely in favour of
  keying off `DagRun.backfill_id` and the existing unpause path.
- #61809 — "PoC of task group retries": closed; review's position was that this is
  a large change to what task groups are and needed a dev-list discussion before
  the code.
- #53959 — "Add `ALL_DONE_MIN_ONE_SUCCESS` trigger rule": the merged shape of a
  genuine addition — a new rule in the existing trigger-rule vocabulary rather than
  a new parameter and a new dep.
- #61227 — "Multi-team: verify a task uses a pool it has access to when
  scheduling": a merged dep-level change that expresses new behaviour through the
  existing pool-state check rather than a new user-facing knob.
- #54774 — "Fix trigger rule error messages showing enum names instead of values":
  a merged fix treating the dep's user-facing reason string as part of the
  contract.
