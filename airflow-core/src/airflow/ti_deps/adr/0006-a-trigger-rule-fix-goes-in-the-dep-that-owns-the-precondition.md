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

# 6. A trigger-rule fix goes in the dep that owns the precondition, and keeps the early decision

Date: 2026-07-20

## Status

Accepted

## Context

`TriggerRuleDep` is the most-patched file here, and closed PRs are dominated by
attempts to fix it that fail in two ways, both invisible in the diff.

**Fixing a precondition inside the rule.** `TriggerRuleDep` answers one question —
do this instance's upstreams and their states permit its trigger rule to run? — and
assumes the instance is already answerable, in particular that a mapped instance is
unmapped with upstreams resolving at its own index. When that does not hold the
symptom surfaces here and the natural fix is a guard here; but a dep already exists
to establish that precondition (`MappedTaskIsExpanded`), it was just not applied to
operators inside a mapped group.

**Trading the early decision for a wait.** Trigger rules decide as soon as the
answer is determined: with `ALL_SUCCESS`, one non-success upstream fails the
downstream immediately. Making it wait for all upstreams to reach a terminal state
is simpler but delays every failure and skip in every Dag — the rule is to wait
until you can deterministically decide, *but no longer*. Behind both, the counting
logic encodes deadlock fixes, so review requires a reproduction for count changes
rather than reasoning by inspection.

## Decision

A defect that surfaces in trigger-rule evaluation is fixed at the dep that owns
the broken assumption, and never by weakening when the rule decides. Concretely:

- **Locate the precondition before writing the guard.** If the wrong answer comes
  from state that another dep is responsible for establishing — expansion, prior
  skip, run state — the fix is that dep, or its membership in the right set in
  `dependencies_deps.py`, not a compensating branch inside `TriggerRuleDep`.
- **Preserve fast fail and fast skip.** A change must not convert an early
  decision into waiting for all upstream instances to reach a terminal state. The
  downstream waits exactly until the rule's answer is determined.
- **State what the change does to the deadlock cases.** Any change to how
  upstreams are counted says what happens when an upstream instance is removed,
  was never expanded, or is absent — and confirms that no combination produces a
  condition that can never be satisfied.
- **State the effect on the paths you did not aim at.** `TriggerRuleDep` is
  evaluated for every task instance. A fix targeting mapped task groups says what
  it does for non-mapped tasks, setup/teardown relationships, and plain task
  groups.
- **Prefer the smaller, better-placed alternative.** Where a competing PR fixes
  the same defect at the owning dep, that is the one that lands, even if it
  arrived later.

## Consequences

- Contributors who correctly diagnosed a bug are asked to move the fix somewhere
  they had no reason to look — why fixes here take several rounds, and why the
  diagnosis belongs in the PR: review can redirect a diagnosis, not a diff.
- Keeping the early decision keeps the counting logic intricate, accepted because
  the alternative is paid by every Dag on every run.
- Requiring a reproduction for count changes slows hard-to-reproduce fixes — an
  unreproduced count change has been as likely to add a deadlock as to remove one.
- `TriggerRuleDep` accumulates conditionals; periodic consolidation is expected work.

A change **violates** this decision when it:

- adds a check for expansion, mapping, or prior skip state inside
  `trigger_rule_dep.py` when an existing dep already establishes that
  precondition;
- makes a downstream task wait for all upstream instances to finish where it
  previously decided as soon as the rule's answer was determined;
- changes how upstream instances are counted without saying what happens to
  removed, unexpanded, or absent upstreams;
- claims a mapped-task-group fix without saying what it does to the non-mapped
  path;
- changes counting behaviour with no test that reproduces the reported failure
  and fails without the change.

A reviewer should ask: which dep is supposed to guarantee the state this fix is
compensating for, and does this change make any task wait longer than the rule
requires?

## Evidence

- #32701 — one-failed inside mapped group: correct diagnosis, fixed in
  `TriggerRuleDep`; closed for applying `MappedTaskIsExpanded` instead.
- #34138 — mapped task not waiting for upstream mapped: closed; review refused
  removing fast-fail/fast-skip.
- #33915 — removed upstreams for non-mapped tasks: closed; review required
  reproducing the guarded deadlock before extending.
- #33570 — waiting setup tasks not a direct upstream: closed for alternative fix
  #33903 placed elsewhere.
- #33820 — same defect as a separate dep rather than a `TriggerRuleDep` branch;
  lapsed without conclusion.
- #36462 — premature evaluation in mapped group: closed after an unanswered request
  for a reproducing test.
- #40963 — last-dagrun-still-running condition: drafted to add a before/after test
  and lapsed.
- #34578 — reduce DB round trips for setup tasks: a `TriggerRuleDep` change that
  lapsed without review.
