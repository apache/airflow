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

`TriggerRuleDep` is the most-patched file in this directory, and the closed-PR
record is dominated by attempts to fix it. Those attempts fail in two
characteristic ways, and both are invisible in the diff to anyone who does not
already know how the dep set is composed.

**The first is fixing a precondition inside the rule.** `TriggerRuleDep` answers
one question: given this task instance's upstreams and their states, does its
trigger rule permit it to run? It assumes the instance is already in a state
where that question is answerable — in particular, that a mapped instance has
been unmapped and its upstreams resolve at its own index. When that assumption
does not hold, the symptom surfaces in `TriggerRuleDep`, and the natural fix is a
guard there. One such attempt tracked down a genuine defect — for tasks inside a
mapped task group, `are_dependencies_met` ran before the expansion step, so
upstream lookup returned the wrong instances — and patched the counting logic to
compensate. Review agreed the diagnosis and refused the location: a dep already
exists whose whole purpose is to establish that precondition
(`MappedTaskIsExpanded`); it was simply not applied to operators inside a mapped
task group. Extending that dep's applicability restores the assumption
`TriggerRuleDep` is written against, instead of adding a second place that has to
know about expansion.

**The second is trading the early decision for a wait.** Trigger rules decide as
soon as the answer is determined, not when every upstream has finished: with
`ALL_SUCCESS`, one non-success upstream is enough to fail the downstream
immediately. An attempt to fix a mapped-task ordering bug made the downstream
wait for all upstream instances to reach a terminal state. That is simpler, it
fixed the reported symptom, and it was refused, because "wait for everything"
delays every failure and skip in every Dag to fix one ordering case. The stated
rule from that review is the precise one: the downstream waits until it can
deterministically decide what to do — *but no longer*.

Behind both is a third fact that makes this seam expensive: the current shape of
the counting logic encodes fixes for scheduling **deadlocks**, where an
unsatisfiable condition leaves a task permanently blocked. Several closed
attempts here changed counts to handle removed or absent upstream instances, and
review's response each time was to establish whether the deadlock the existing
shape guards against is still guarded, and whether the new one reproduces — not
to reason about the change by inspection. Attempts that could not produce that
reproduction lapsed; those that could were often superseded by an alternative fix
placed elsewhere.

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

- Contributors who correctly diagnosed a real bug are asked to move the fix
  somewhere they had no reason to look. This is the main reason fixes in this
  seam take several rounds, and it is why the diagnosis is worth writing into the
  PR description — a review can redirect a stated diagnosis, but cannot redirect
  a diff.
- Keeping the early decision means the counting logic stays intricate: each rule
  must know the smallest evidence sufficient to decide. That complexity is
  accepted deliberately, because the alternative is paid by every Dag on every
  run.
- Requiring a reproduction for count changes slows down fixes for bugs that are
  hard to reproduce. Review accepts that trade: an unreproduced count change has
  historically been as likely to introduce a deadlock as to remove one.
- `TriggerRuleDep` accumulates conditionals rather than shedding them, since
  fixes that belong elsewhere are pushed elsewhere but the rule logic itself
  keeps growing cases. Periodic consolidation of this file is expected work.

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

- #32701 — "Fix logic of trigger rule one failed inside mapped group": correct
  diagnosis (upstreams resolved before the mapped instance was expanded), fixed
  inside `TriggerRuleDep`; closed with review's position that the right fix is to
  apply the `MappedTaskIsExpanded` dep to operators in a mapped task group so the
  precondition holds by the time `TriggerRuleDep` runs.
- #34138 — "Fix mapped task not waiting for upstream mapped to complete": closed;
  review refused the removal of fast-fail and fast-skip, stating the downstream
  must wait until it can deterministically decide and no longer.
- #33915 — "Take into account removed upstream tasks for non-mapped tasks":
  closed unmerged; review required reproducing the deadlock the existing
  mapped-only condition was introduced to fix, and confirming it stays fixed,
  before extending the behaviour to non-mapped tasks.
- #33570 — "Fix waiting setup tasks when they are not a direct upstream": closed
  in favour of an alternative fix (#33903) placed differently.
- #33820 — "Try fixing non direct upstream — separate dep approach": the same
  defect attempted as a separate dep rather than a `TriggerRuleDep` branch;
  lapsed without review conclusion.
- #36462 — "Fix pre-mature evaluation of tasks in mapped task group": closed
  after an unanswered request for a test constructing the mapped-task-group and
  trigger-rule combination.
- #40963 — "Fix dep status check, add condition to check if last dagrun is still
  running": review's first response was that a test failing before and passing
  after the change is how a dep change explains itself; the PR was drafted to add
  one and lapsed.
- #34578 — "Reduce round trips to db when evaluating setup tasks": a
  `TriggerRuleDep` change that lapsed without review, illustrating how much
  traffic this one file absorbs.
