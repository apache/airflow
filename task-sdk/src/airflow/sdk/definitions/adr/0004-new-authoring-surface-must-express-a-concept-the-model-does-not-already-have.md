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

Every name added here — a `DAG` parameter, a `BaseOperator` attribute, a new
class exported from `airflow.sdk` — is permanent in practice. It ships in a
release, users write Dag files against it, and from that point the project owes
it a deprecation cycle rather than a deletion (see `adr/0001`). The cost of a
new authoring symbol is therefore not the diff that adds it; it is the
multi-release obligation that follows, plus the documentation, the serialization
field, and the second way of expressing something users must now choose between.

This makes the *most common* reason a change here is refused not "the code is
wrong" but "the model already says this". The authoring surface is deliberately
small, and a proposal that adds a parameter for something the existing
data-interval / timetable / `default_args` model already expresses is rejected
even when the implementation is clean and tested. A request for a Dag-level
`target_date` ("what date are we processing?") was closed with the observation
that this is precisely what `data_interval_start` / `data_interval_end` are, and
that the half-open interval is produced by the timetable — the concept existed,
under a different name. A `safe_dag()` construction wrapper was refused because
it did not correspond to anything the Dag model actually has. A proposal to list
variable keys through the SDK was refused because the secrets-backend contract is
get-by-key, and adding a listing verb to the authoring surface would have implied
a capability no backend actually provides.

The same shape recurs at the level of pure mechanical churn: a sweep replacing
`type(self)` with `self.__class__` across the package was closed as "a change for
the sake of change" once it was pointed out that the two differ under
`lazy_object_proxy`, so each site needed independent justification rather than a
blanket rewrite.

The failure mode this guards against is subtle. A duplicate concept does not
break anything on the day it merges. It breaks things two releases later, when
the two spellings drift, when a bug is fixed in one and not the other, and when
the docs have to explain which one a user should reach for. Because the person
who would object — the user with an existing Dag file, or the maintainer who will
carry the deprecation — is not in the pull request, the burden falls on review.

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

- The authoring API stays small enough for a user to hold in their head, and for
  the project to keep deprecating responsibly on a release cadence.
- Genuinely new capability costs more to land: the author has to do the mapping
  work and argue the gap, which is slower than writing the parameter.
- Some real user needs are answered with "that already exists as X" plus a
  documentation fix. This is deliberate, and it puts a burden on the docs to make
  X discoverable — a burden this decision accepts rather than hides.
- Reviewers must know the existing model well enough to recognise a restatement.
  That knowledge is concentrated in few people, which is a cost this area already
  carries.

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

- #67329 — "Add `target_date`, a user-defined processing date for Dag runs":
  closed as won't-fix; the reviewer showed the concept is exactly
  `data_interval_start` / `data_interval_end`, generated by the timetable as a
  half-open interval.
- #56066 — "Add safe Dag creation wrapper and refactor `DagBag.import_errors`":
  the `safe_dag()` wrapper was refused because it did not correspond to a
  construct that exists inside Airflow.
- #61595 — "List variable keys with optional prefix filter via task-sdk": refused
  because no secrets backend implements listing; the backend contract is
  get-by-key, so the authoring verb would have promised a capability the system
  does not have.
- #53856 — "Replace `type(self)` with `self.__class__`": closed by the author
  after review pointed out the forms differ under `lazy_object_proxy` and each
  usage needed independent justification — "a change for the sake of change".
- #63907 and #61336 — two independent proposals for Dag-level automatic retries:
  both closed, with the discussion turning on how the proposal relates to task
  retries already available through `default_args` and what genuinely new
  semantics remained.
