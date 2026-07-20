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

# 6. Dynamic mapping requires an expansion count the scheduler can determine

Date: 2026-07-20

## Status

Accepted

## Context

Dynamic task mapping (`expand()` / `expand_kwargs()`, AIP-42) looks like a lazy
sequence operation to the Dag author, but it is not one. Before any mapped task
runs, the scheduler must **materialise a task instance per map index**: it writes
rows, assigns map indexes, and evaluates dependencies per index. To do that it
needs a concrete count.

The count contract is split across two packages, and this ADR governs only the
authoring half. The scheduler-side counting lives in
`airflow-core/src/airflow/models/expandinput.py`, which owns
`get_parse_time_mapped_ti_count()` (the count from literal values known at parse
time), `get_total_map_length(run_id, *, session)` (the count at run time from the
recorded `TaskMap` length of the upstream task), and the public
`NotFullyPopulated` those raise when neither can produce a number. Those symbols
take a SQLAlchemy session and cannot exist in this package —
[ADR-3](0003-authoring-package-is-independent-of-airflow-core.md) and the
`check_core_imports_in_sdk` hook forbid it. They are an **external constraint** on
this directory, not code it can change.

What this package owns is the authoring shape the count is computed *from*.
`_internal/expandinput.py` defines `DictOfListsExpandInput` and
`ListOfDictsExpandInput`, whose `_get_map_lengths()` resolves one length per
mapped argument and raises the package-private `_NotFullyPopulated` when any
argument's length is not yet knowable; `xcom_arg.py` defines the `XComArg`
subclasses whose `__len__` the scheduler's count ultimately reduces to. The count
is not an optimisation — it is the thing that lets the mapped task exist at all,
and an authoring construct added here that no upstream length determines is one
the scheduler cannot materialise.

This is why a whole class of otherwise reasonable authoring proposals is refused.
A `filter` operation on `XComArg` was closed because filtering changes the length
of the sequence, and the length is exactly what cannot be unknown: the reviewer's
observation was that the scheduler needs to know how many task instances to run,
and the only workable shape would be to expand at the original length and mark the
unneeded instances skipped afterwards. A streaming `IterableOperator` /
`DeferredIterable` proposal was closed on a related basis — an operator that
consumes an unbounded sequence inside its own loop is, from the scheduler's point
of view, a parallel scheduler and executor implementation, not a mapped task. Both
were ultimately redirected to AIP-88 (lazy task expansion), which is the venue for
changing the count contract itself rather than working around it.

The second half of this decision is the dependency edge. Mapping is not only a
count; it is also a graph. A change intended to fix missing implicit downstream
dependencies for mapping sources in mapped task groups was withdrawn by its author
after the test suite showed it violated an invariant — it forced mapped tasks to
depend on mapping functions they do not consume. Because `XComArg` is simultaneously
the value reference and the graph edge, a change to how mapped inputs are resolved
silently changes what depends on what, and the failure shows up as a wrong Dag
shape rather than an exception.

## Decision

Any change to the mapping surface must preserve the scheduler's ability to
determine an expansion count, and must not alter the dependency edges implied by
`XComArg`. Concretely:

- **A mapped input must yield a count** either at parse time, from literal values,
  or at run time from the upstream `TaskMap` length. An authoring construct whose
  length is unknowable at both points is not expressible as a mapped task today.
- **A new `XComArg` operation's result length must be computable from the upstream
  lengths alone.** Changing the length is *not* the problem — `XComArg.zip` and
  `XComArg.concat` both change it and both ship: `_ZipResult.__len__` returns
  `min()` (or `max()` with a fill value) and `_ConcatResult.__len__` returns
  `sum()`. What each of those has is a length derivable by arithmetic on the
  upstream lengths, without looking at a single value. An operation whose length
  requires *inspecting the values* — `filter`, `dedupe`, `takewhile` — cannot be
  counted before the values exist, which is after the scheduler needed the count.
  Where that behaviour is wanted, the supported shape is to expand at the known
  length and skip the unneeded indexes.
- **The not-fully-populated signal is handled, not bypassed.** A change must not
  make a length path return a guess, a default, or a partial length in order to
  proceed; `_get_map_lengths()` raising `_NotFullyPopulated` (and the core-side
  `NotFullyPopulated` it corresponds to) is the correct outcome when an upstream
  length is not yet known.
- **`XComArg` changes preserve the graph edge.** A change to how a mapped input is
  resolved must not add a dependency on a source the mapped task does not consume,
  nor drop one it does.
- **Changing the count contract itself goes through the AIP** (AIP-88 lazy
  expansion), not through a change to the authoring classes.
- **In-task iteration is out of scope.** A construct that iterates within a
  single task instance — consuming a sequence inside one `execute()` and creating
  no mapped task instances, no map indexes, and no scheduler-visible expansion —
  is not dynamic mapping, and this decision does not govern it. Such a construct
  may still touch `_internal/expandinput.py`, `mappedoperator.py`, or
  `xcom_arg.py` to reuse the authoring plumbing; that alone is not a violation.
  What this decision governs is whether the *scheduler* can count what it must
  materialise, and it materialises nothing here.
- **The supported path for genuinely new capability is an AIP, landed as a single
  end-to-end change.** Read alongside its neighbours — `adr/0004` refusing new
  authoring surface, and the Execution API's `adr/0007` refusing a route not
  scoped to a current worker need — this decision could look like a closed loop
  with no way in. It is not: an AIP supplies the agreed-need argument no
  single-layer PR can supply for itself, and it lets the layers land together
  instead of each waiting on the others. A mapping-adjacent change under an
  accepted AIP is evaluated against the AIP's design, not refused for arriving as
  new surface.

## Consequences

- Several natural-looking authoring conveniences are simply unavailable, and the
  refusal has to be explained in terms of scheduler mechanics rather than API
  taste. This is a recurring cost paid in review.
- The expand-then-skip workaround creates task instances that never do work,
  which shows in the UI and in metrics. The project accepts that visible cost over
  an unknowable count.
- Mapping bugs are expensive to diagnose because a wrong dependency edge produces
  a wrong graph rather than an error, so tests for changes here must assert on the
  resulting graph and the resulting instance count, not just on a return value.
- Work that genuinely needs lazy expansion is blocked behind an AIP, which is slow
  — deliberately, because it changes an invariant the scheduler is built on.

A change **violates** this decision when it:

- adds an authoring construct that *creates mapped task instances* whose
  expansion length cannot be determined at parse time or from the upstream
  `TaskMap`. A construct that iterates inside one task instance and creates no
  mapped task instances is out of scope, as is one landing under an accepted AIP,
  even when the diff touches `_internal/expandinput.py`, `mappedoperator.py`, or
  `xcom_arg.py`;
- adds an operation on `XComArg` or a mapped input whose result length cannot be
  computed from the upstream lengths alone — i.e. one whose `__len__` would have to
  inspect the values (filter, dedupe, takewhile). Operations whose length is
  arithmetic on the upstream lengths, as `zip` and `concat` already are, are fine
  even though they change it;
- makes a length path return a fallback value instead of raising
  `_NotFullyPopulated`, or swallows that exception to let expansion proceed;
- changes mapped-input resolution such that a mapped task gains a dependency on a
  source it does not consume, or loses one it does;
- lets a mapped argument leak into the frozen non-mapped set of `partial()`, or
  collapses a single-expansion group to a bare value.

A reviewer should ask, of any mapping change: at the moment the scheduler creates
task instances for this task, where does the number come from — and does the
dependency graph still contain exactly the edges the author wrote?

## Evidence

- #48868 — "Implemented filter operation on XComs": closed; review established
  that the scheduler needs to know how many task instances to run, that a real
  filtered length cannot be known in advance — because it depends on the values,
  not on the upstream lengths — and that the only workable shape would be to
  expand at the original length and skip the remainder.
- #42572 — "Implemented streaming functionality with `IterableOperator` and
  `DeferredIterable`": closed as amounting to a parallel scheduler, executor, and
  triggerer implementation; redirected to AIP-88.
- #59561 — "Fix missing implicit downstream dependencies for mapping sources in
  mapped task groups": withdrawn by its author after failing tests showed it
  violated an invariant by forcing mapped tasks to depend on mapping functions
  they do not consume; reworked with a different approach.
- #62287 — "`LatestOnlyOperator` not working if direct upstream of dynamic task
  map": a merged fix in the same seam, where the interaction between a mapped
  input and dependency evaluation produced a wrong result rather than an error.
