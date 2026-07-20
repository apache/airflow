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

# 4. Mapped-task dependencies are evaluated per map index

Date: 2026-07-20

## Status

Accepted

## Context

Trigger-rule evaluation was designed when a task had one upstream relationship
per edge. Dynamic task mapping broke that assumption: inside a mapped task group,
`task_b[3]` depends on `task_a[3]`, not on the aggregate state of every `task_a`
instance. A trigger rule that folds all upstream map indexes into a single
counted state gives the wrong answer for every index — and gives it silently,
because the result is a task that stays scheduled, gets skipped, or deadlocks
rather than one that raises.

This seam is the single most active source of defects in this area, and the most
active source of *failed* attempts to fix them. Fixes for per-index evaluation of
`ONE_FAILED` and for `none_failed_min_one_success` landed only after several
independent attempts at the same class of bug were closed unmerged: per-index
trigger-rule evaluation after upstream expansion, branch skips inside mapped task
groups, `ShortCircuitOperator` behaviour with mapped tasks, and unmapped-task
deadlock when upstream tasks are removed. The deadlock fix itself took two
attempts, the first closing after a reviewer asked whether a test case was
possible and none arrived.

There are two reasons these attempts fail so consistently. The first is that the
correct behaviour is not obvious from the rule's name: `none_failed_min_one_success`
inside a mapped group has to be read as a statement about *this index's* upstreams,
and the aggregate reading is a plausible misreading that passes casual inspection.
The second is that the bug only appears in a specific combination — a mapped task
group, a particular trigger rule, a branch or skip, and an expansion count that
differs from the naive one — so a change that looks right and has a passing unit
test can still be wrong. Without a test that reproduces the exact deadlock or
mis-skip, review has no way to tell a real fix from a plausible one, which is why
untested attempts here are closed rather than merged on inspection.

Removal of upstream tasks compounds this. When an upstream mapped task disappears
between runs, the dependency evaluation must decide what the surviving indexes
depend on, and a rule that counts upstreams without accounting for removals
produces a permanently unsatisfiable condition.

## Decision

Dependency and trigger-rule evaluation for mapped tasks is per map index, and
changes to it must be demonstrated with a reproducing test. Concretely:

- **Evaluate against the matching map index.** Inside a mapped task group, an
  instance's upstream state is the state of the upstream instances *at its own
  index*, not the aggregate across all indexes.
- **A trigger-rule change states its behaviour for the mapped case.** Any change
  to a rule's counting must say what it does inside a mapped task group, including
  when the upstream expansion count differs from the downstream one.
- **Removed and non-existent upstreams are handled explicitly.** Evaluation must
  not produce a condition that can never be satisfied when an upstream mapped
  instance is absent.
- **A fix in this seam ships with a test that reproduces the failure.** The test
  must construct the mapped-group / trigger-rule / skip combination and fail
  without the change; a unit test asserting on a counter is not sufficient
  evidence.
- **Skip propagation is per index too.** Branch and short-circuit operators
  upstream of a mapped group skip the matching indexes, not the whole group.

## Consequences

- Changes here are slow to land, and several correct-looking attempts have been
  closed for want of a reproducing test. That is the intended trade: a wrong
  trigger-rule fix strands user tasks in production, and the wrongness is invisible
  in the diff.
- Writing the reproducing test is often harder than writing the fix, because it
  requires building a mapped task group with the right expansion and upstream
  states. This is a real barrier to contribution in this area and is the main
  reason first attempts here fail.
- Per-index evaluation costs more than aggregate counting in the scheduling loop,
  which interacts with `adr/0002` — the evaluation must stay cheap per task
  instance, so per-index logic must not introduce a query per index.
- Contributors without prior context in this area should expect to be redirected
  to an existing in-flight fix rather than to have a parallel attempt reviewed.

A change **violates** this decision when it:

- folds upstream map indexes into an aggregate state when deciding whether a
  mapped instance may run;
- changes a trigger rule's counting without stating the behaviour inside a mapped
  task group;
- leaves a mapped instance with an unsatisfiable dependency when an upstream
  mapped instance has been removed;
- propagates a skip to a whole mapped group where only matching indexes should
  be skipped;
- lands a trigger-rule or mapped-dependency fix without a test that reproduces the
  original deadlock, skip, or stuck state;
- adds a per-index database query to the dependency path, trading a correctness
  fix for a scheduling-loop regression.

A reviewer should ask: which map index is this condition about, and what is the
test that fails without this change?

## Evidence

- #67684 — "Fix per-index evaluation of `ONE_FAILED` in mapped task groups": the
  merged fix establishing per-index evaluation for this rule.
- #67873 — "Fix `none_failed_min_one_success` trigger rule checks": the same class
  of correction for another rule.
- #62034 — "Handle unmapped task deadlock when upstream tasks are removed": the
  merged fix for the removed-upstream case; an earlier attempt (#56678) was closed
  after review asked for a test case and none was supplied.
- #68426 — "Per-index trigger rule evaluation broken for mapped task groups after
  upstream expansion": closed unmerged, one of several independent attempts at
  this defect class.
- #67439 — "Fix branch skips inside mapped task groups" and #55661 — "Ensure
  `ShortCircuitOperator` skips mapped tasks with
  `ignore_downstream_trigger_rules=True`": both closed unmerged, showing skip
  propagation into mapped groups as a repeat failure point.
- #62287 — "`LatestOnlyOperator` not working if direct upstream of dynamic task
  map": a merged fix where mapped upstream state produced a wrong skip decision.
- #62089 — "Fix `DepContext` mutation leak and restore reschedule-mode guard": a
  merged fix in the shared evaluation context, showing how state carried across dep
  evaluations produces cross-instance wrongness.
