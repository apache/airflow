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

A trigger does not run where it was defined. The operator is constructed in a Dag
file, rendered and executed on a worker, then serialised to a classpath and kwargs
(`adr/0002`) and reconstructed inside the triggerer — a different process, on a
different host, with no access to the metadata database (`adr/0003`). Anything the
trigger or an async callback is to *receive* must therefore be either serialised
into those kwargs, fetched at execution time through the Execution API, or
persisted somewhere the triggerer can read.

Each of those three routes has a cost, and the history of this area is a sequence
of attempts to enlarge the received context that were merged and then reverted.
Template-field rendering for `start_from_trigger` landed, was reverted, and only
re-landed on a third attempt. Fetching deadline-callback context from the
Execution API at runtime landed as a substantial change — new comms messages,
workload plumbing, read-only route restrictions, bundle-hash verification, bundle
init moved off the event loop — and was then reverted wholesale. Separately, a
defect had to be fixed where an operator's `template_fields` and its trigger's
attributes are simply different sets, so rendering one against the other failed.

The rejected attempts tell the same story from the other side. Three successive
proposals to add Jinja rendering to deadline callback kwargs were withdrawn by
their own author, the decisive one on the reasoning that building rendering on top
of a context already stored in the database does not address the architectural
problem that the context should not be stored there at all — the right shape being
to fetch it at callback execution time and revert the stored simple context.

What makes this seam so revert-prone is that the change is never local. Adding one
field to what a callback receives touches the defer-time capture, the serialised
payload, the triggerer's reconstruction, the Execution API surface it may call,
and the security scope of the token it holds — and a gap in any one of them
produces a failure that appears only in a real deployment, under a real bundle,
with a real deadline. A diff confined to the trigger class looks complete and is
not.

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

- Changes here are large by necessity, which conflicts with the general preference
  for small pull requests. This area is an explicit exception: a partial change to
  this path is more dangerous than a big one.
- The revert history means reviewers are entitled to ask for more evidence than
  elsewhere, and contributors should expect that a working local demonstration is
  not sufficient.
- Keeping context out of the database costs a network round-trip per callback and
  makes the callback path dependent on Execution API availability, with a failure
  mode — fail the callback for retry rather than run it degraded — that must be
  chosen deliberately.
- Because the supported route has itself been reverted, a contributor should
  confirm the current state of this path before building on it rather than
  assuming the most recent merged design is settled.

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

- #66608 — "Fetch deadline callback context via Execution API at runtime": the
  full-path implementation — comms message, workload plumbing, read-only route
  restriction, bundle-hash verification, bundle init moved off the event loop —
  reverted wholesale in #68909.
- #53071 — "Allow rendering of template fields with start from trigger": merged,
  reverted in #55037, and re-landed in #55068 — three passes over the same seam.
- #64715 — "Fix trigger template rendering failure when operator
  `template_fields` differ from trigger attributes": the merged fix establishing
  that the two field sets are distinct.
- #66496 — "Add Jinja template rendering for async deadline callbacks": closed by
  its author on the reasoning that building rendering on stored simple context does
  not address the concern that context should not be stored in the database at all.
- #64984 and #68408 — two further closed attempts at rendering deadline callback
  kwargs, the latter folded into the context-fetch change rather than reviewed as a
  separate surface.
- #55241 — "Include simple context in triggerer async callback": the merged stored-
  context step that the later work set out to replace.
