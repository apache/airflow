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

Trigger-rule evaluation was designed when a task had one upstream relationship per
edge. Dynamic task mapping broke that: inside a mapped task group, `task_b[3]`
depends on `task_a[3]`, not on the aggregate state of every `task_a`. A rule that
folds all upstream indexes into one counted state gives the wrong answer for every
index — silently, as a task that stays scheduled, gets skipped, or deadlocks rather
than one that raises.

This seam is the most active source of defects here, and of *failed* fixes. Attempts
fail for two reasons: the correct behaviour is not obvious from the rule's name (the
aggregate reading of `none_failed_min_one_success` is a plausible misreading), and
the bug appears only in a specific combination — a mapped group, a particular rule, a
branch or skip, an expansion count differing from the naive one — so a change that
looks right with a passing unit test can still be wrong. Without a test reproducing
the exact deadlock or mis-skip, review cannot tell a real fix from a plausible one,
so untested attempts are closed. Removal of upstream tasks compounds this: a rule
counting upstreams without accounting for removals produces a permanently
unsatisfiable condition.

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

- Changes here are slow to land, and correct-looking attempts are closed for want of
  a reproducing test — a wrong trigger-rule fix strands user tasks, invisibly in the
  diff.
- Writing the reproducing test is often harder than the fix, a real barrier to
  contribution and the main reason first attempts fail.
- Per-index evaluation costs more than aggregate counting, which interacts with
  `adr/0002`: per-index logic must not introduce a query per index.
- Contributors without prior context should expect to be redirected to an existing
  in-flight fix rather than have a parallel attempt reviewed.

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
  merged fix establishing per-index evaluation.
- #67873 — "Fix `none_failed_min_one_success` trigger rule checks": same correction
  for another rule.
- #62034 — "Handle unmapped task deadlock when upstream tasks are removed": merged;
  an earlier attempt (#56678) closed for want of a test case.
- #68426 — per-index evaluation broken after upstream expansion: closed unmerged, one
  of several attempts at this defect class.
- #67439 — branch skips inside mapped groups — and #55661 — `ShortCircuitOperator`
  skipping mapped tasks: both closed unmerged, skip-propagation repeat failures.
- #62287 — `LatestOnlyOperator` upstream of a dynamic task map: merged, mapped
  upstream state produced a wrong skip.
- #62089 — "Fix `DepContext` mutation leak ...": merged; state carried across
  evaluations produces cross-instance wrongness.
