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

# 4. New authoring surface must express a concept the model does not already have

Date: 2026-07-20

## Status

Accepted

## Context

Every name added here — a `DAG` parameter, a `BaseOperator` attribute, a class
exported from `airflow.sdk` — is permanent in practice: it ships, users write Dag
files against it, and the project then owes a deprecation cycle rather than a
deletion (see `adr/0001`). The cost is not the diff but the multi-release
obligation, the docs, the serialization field, and a second way of expressing
something users must now choose between.

So the *most common* reason a change here is refused is not "the code is wrong"
but "the model already says this", even when the implementation is clean and
tested. A Dag-level `target_date` is precisely what `data_interval_start`/
`data_interval_end` are — the concept under a different name; a `safe_dag()`
wrapper corresponds to nothing the Dag model has; listing variable keys implies a
capability the get-by-key backend contract does not provide. The same shape recurs
in mechanical churn (`type(self)` → `self.__class__`), closed once each site
needed independent justification. The failure is subtle: a duplicate concept
breaks nothing the day it merges, but breaks two releases later when the spellings
drift and the docs must explain which to reach for — and the person who would
object is not in the PR, so the burden falls on review.

## Decision

New authoring surface is added only when it expresses a concept the model does
not already carry. Concretely:

- **Map the proposal onto the existing model first.** Before adding a parameter,
  attribute, or class, state which existing construct (`data_interval`, the
  timetable, `default_args`, `depends_on_past`, an existing accessor) does *not*
  cover the need, and why.
- **A second spelling of an existing concept is not additive — it is a fork.**
  If the answer is "this is the same idea with a friendlier name", the change
  belongs in documentation, not in the authoring API.
- **Blanket mechanical rewrites of this package need per-site justification.**
  An equivalence that holds "almost always" is not a licence to sweep the
  package; the edge cases are the reason the code reads the way it does.
- **New surface that implies a capability the rest of the system does not
  provide is refused** until that capability exists — the authoring API must not
  advertise behaviour no backend, scheduler, or executor can honour.
- **The supported path for genuinely new capability is an AIP, landed as a single
  end-to-end change.** This decision, `sdk/api/adr/0004` (no client method before
  the spec entry), and the Execution API's
  `adr/0007` (no route not scoped to a current worker need) each refuse an
  increment that arrives alone, which taken pairwise looks like no path in at all.
  The AIP is the path: it supplies the agreed-need argument no single-layer PR can
  supply for itself, and it lets the layers land together instead of each waiting
  on the others. AIP-76 (#65447), AIP-103 (#66073, #66160) and AIP-105 (#65474)
  all merged this way.

## Consequences

- The authoring API stays small enough to hold in one's head and to keep
  deprecating responsibly on a release cadence.
- Genuinely new capability costs more: the author does the mapping work and argues
  the gap, slower than writing the parameter.
- Some real needs are answered with "that already exists as X" plus a docs fix,
  putting a burden on docs to make X discoverable.
- Reviewers must know the model well enough to recognise a restatement — knowledge
  concentrated in few people.

A change **violates** this decision when it:

- adds a `DAG` / `BaseOperator` parameter or an `airflow.sdk` export whose stated
  purpose is already served by `data_interval_start` / `data_interval_end`, the
  timetable, `default_args`, or an existing accessor, without saying why those do
  not fit;
- introduces a wrapper, helper, or convenience class that does not correspond to
  a construct the Dag model actually has;
- adds an authoring verb whose backing contract does not exist elsewhere in the
  system (for example a listing operation against backends whose contract is
  get-by-key);
- sweeps a mechanical equivalence across the package without justifying the
  sites where the two forms differ.

A reviewer should ask, of every new name in this package: which existing
construct fails to express this, and what does a user do today instead?

## Evidence

- #67329 — `target_date`; closed won't-fix because it is exactly
  `data_interval_start`/`data_interval_end`, generated by the timetable.
- #56066 — `safe_dag()` wrapper refused as not corresponding to a construct
  Airflow has.
- #61595 — list variable keys; refused because the backend contract is get-by-key,
  so the verb would promise a capability the system lacks.
- #53856 — `type(self)` → `self.__class__`; closed as "change for the sake of
  change" given the `lazy_object_proxy` difference.
- #63907, #61336 — two Dag-level automatic-retries proposals; both closed on how
  they relate to task retries already in `default_args`.
