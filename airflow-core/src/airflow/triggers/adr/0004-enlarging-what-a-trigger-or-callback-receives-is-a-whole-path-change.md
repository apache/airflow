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

# 4. Enlarging what a trigger or callback receives is a whole-path change

Date: 2026-07-20

## Status

Accepted

## Context

A trigger does not run where it was defined. The operator is built in a Dag file,
executed on a worker, then serialised to classpath and kwargs (`adr/0002`) and
reconstructed inside the triggerer — a different process, no metadata-DB access
(`adr/0003`). So anything a trigger or async callback is to *receive* arrives by
one of three routes: serialised into kwargs, fetched at execution time through the
Execution API, or persisted somewhere the triggerer can read.

The history is a sequence of attempts to enlarge the received context that merged
and then reverted. Template-field rendering for `start_from_trigger` landed, was
reverted, and re-landed on a third attempt. Fetching deadline-callback context
from the Execution API landed as a substantial change (comms messages, workload
plumbing, read-only route restrictions, bundle-hash verification, bundle init off
the event loop) and was reverted wholesale. Three proposals to add Jinja rendering
to deadline-callback kwargs were withdrawn by their author — the decisive one on
the reasoning that rendering on top of a context stored in the DB does not address
that the context should not be stored there at all.

What makes this seam revert-prone is that the change is never local: one field
touches the defer-time capture, the serialised payload, the reconstruction, the
Execution API surface it may call, and the token's security scope — and a gap in
any one appears only in a real deployment. A diff confined to the trigger class
looks complete and is not.

## Decision

Changing what a trigger or async callback receives at execution time is treated as
a change to the whole defer-to-resume path. Concretely:

- **Name the route.** State whether the new data arrives via serialised kwargs,
  an Execution API fetch at execution time, or persisted state — and why the other
  two are unsuitable.
- **Prefer fetching at execution time over persisting context.** Storing execution
  context in the metadata database to make it available later is the shape this
  area has repeatedly moved away from.
- **Rendering happens at a defined point, against the right field set.** An
  operator's `template_fields` and its trigger's attributes are distinct; a change
  must say which set is rendered, when, and what happens when they differ.
- **The Execution API surface and token scope are part of the change.** If the
  triggerer must call a new route, the route's method restrictions and the
  workload token's scope are reviewed with it, not after.
- **Evidence spans the whole path.** A change here is demonstrated end to end —
  defer, serialise, reconstruct, fetch, resume — not by a unit test on the trigger
  class.
- **Bundle and event-loop effects are checked.** Work added to the reconstruction
  path must not block the shared event loop (`adr/0001`) and must resolve the
  correct bundle.

## Consequences

- Changes here are large by necessity, against the general preference for small
  PRs: this area is an explicit exception, because a partial change is more
  dangerous than a big one.
- The revert history entitles reviewers to ask for more evidence than elsewhere;
  a working local demonstration is not sufficient.
- Keeping context out of the DB costs a round-trip per callback and depends on
  Execution API availability, with a deliberate failure mode — fail for retry
  rather than run degraded.
- Because the supported route has itself been reverted, confirm the current state
  of this path before building on it.

A change **violates** this decision when it:

- adds a field to what a trigger or callback receives without stating which of the
  three routes delivers it;
- persists execution context in the metadata database so a callback can read it
  later;
- renders template fields against a set other than the one it names, or ignores the
  case where an operator's `template_fields` differ from its trigger's attributes;
- has the triggerer call a new Execution API route without reviewing that route's
  method restriction and the workload token's scope;
- demonstrates the change only at the trigger class, with no end-to-end evidence
  across defer, reconstruction, and resume;
- adds blocking work or bundle loading to the reconstruction path on the shared
  event loop.

A reviewer should ask: where does this value come from at the moment the triggerer
needs it, and what breaks if that source is unavailable?

## Evidence

- #66608 — full-path fetch of deadline-callback context (comms message, workload
  plumbing, read-only route, bundle-hash verification, bundle init off the loop):
  reverted wholesale in #68909.
- #53071 — template-field rendering for start-from-trigger: merged, reverted in
  #55037, re-landed in #55068 — three passes over the same seam.
- #64715 — the merged fix establishing operator `template_fields` and trigger
  attributes are distinct sets.
- #66496 — closed by its author: rendering on stored simple context does not
  address that context should not be stored in the DB at all.
- #64984, #68408 — two further closed attempts at rendering deadline-callback
  kwargs, the latter folded into the context-fetch change.
- #55241 — the merged stored-context step the later work set out to replace.
