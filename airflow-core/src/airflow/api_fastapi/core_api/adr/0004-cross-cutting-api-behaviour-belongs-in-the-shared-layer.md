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

# 4. Cross-cutting API behaviour belongs in the shared layer, not in individual routes

Date: 2026-07-20

## Status

Accepted

## Context

The API has well over a hundred route handlers. Any behaviour that is *not*
specific to one resource — translating a database constraint violation into a
status code, rejecting an over-large request body, deriving a display name from a
model, stamping or de-duplicating a response header — has exactly two possible
homes: once in the shared layer (the FastAPI exception handlers in
`api_fastapi/common/exceptions.py`, a middleware, a shared datamodel, a shared
query helper), or N times in N handlers.

Contributors reaching for a concrete bug consistently choose the second. The
resulting PR is locally correct and globally wrong: it fixes the one endpoint the
reporter hit, leaves the same defect in every sibling endpoint, and adds a
divergence that the next refactor has to reconcile. Reviewers here close those PRs
and land the shared version instead — a single exception handler that covers every
route for free, a single hybrid property both models read, one middleware rather
than a per-response fix-up.

The cost of getting this wrong is not a bug, it is drift. Two copies of a
derivation stay in sync only as long as nobody edits one of them, and the API
surface is large enough that nobody notices when they diverge. A per-route
validation knob also enlarges the public configuration surface for what is
fundamentally a generic failure the shared layer already knows how to report.

## Decision

Behaviour that is not specific to a single resource is implemented once, in the
shared layer:

- **Generic failure modes are translated by the shared exception handlers**, not
  by `try`/`except` repeated in each handler. A constraint violation, an
  over-large or DB-rejected payload, or a deserialization failure gets a handler
  in `api_fastapi/common/exceptions.py` so every route reports it identically. A
  route-local `except` is for an error only that route can produce.
- **Do not add a config knob and a bespoke exception class for a failure the
  shared layer can report generically.** Prefer the handler that covers every
  endpoint over per-route validation plus new configuration.
- **A derivation lives in one place.** A field computed from a model (a display
  name, a fallback, a status roll-up) is a shared hybrid property / datamodel
  field that both the public and UI surfaces read — never copied into a second
  response model.
- **Response-envelope concerns (headers, content negotiation, compression,
  correlation ids) belong in middleware / app wiring**, not in individual
  handlers.
- **A new reusable filter, sort, or pagination behaviour goes into
  `api_fastapi/common/`** (see that area's ADRs), not inline in the route that
  needed it first.
- **Moving a fix into the shared layer does not waive the Execution API's
  versioning duty.** `api_fastapi/common/` is shared with the Execution API, so a
  handler or middleware change here can reshape Execution-API responses too.
  `execution_api/adr/0005` requires a Cadwyn `VersionChange` for any wire-visible
  response or error shape change on those routes. This decision says *where* the
  fix belongs; that one says what the fix additionally owes. A shared-layer error-
  handling change that alters an Execution-API wire shape satisfies both only by
  doing both — shared implementation **and** the migration wired through the SDK
  chain. Neither ADR is satisfied by citing the other.

## Consequences

- One fix covers every endpoint, including the ones nobody filed a bug against.
- The public configuration surface stays small: generic failures do not each
  acquire a knob.
- Review of a shared-layer change is more expensive — it re-behaves every route at
  once, so it needs the broader test and the broader thought. That is the trade we
  are making deliberately: a wider blast radius reviewed once, rather than a
  narrow fix reviewed N times and applied to one.
- A contributor who arrives with a single-endpoint reproduction has more work to
  do than the bug appeared to require, and may need a maintainer to point at the
  shared home first.

A change **violates** this decision when it:

- adds a `try`/`except` in a route handler for a failure class that any route
  could raise, instead of registering or extending a shared exception handler;
- introduces a new config option plus a new exception class to validate something
  the database or the shared layer already rejects;
- copies a model derivation (hybrid property, display-name fallback, computed
  status) into a second datamodel rather than sharing the existing one;
- fixes a response header, envelope, or content-negotiation defect inside one
  handler instead of in middleware;
- adds a filter/sort/pagination behaviour inline in a route when it belongs in
  `api_fastapi/common/`.

A reviewer should ask, on any fix here: *does this defect exist on the sibling
endpoints too?* If yes, the fix is in the wrong place.

## Evidence

- #66787 — Dag run `conf` payload-size validation: closed by its author in favour
  of #66888, on the reasoning that a config knob, a new exception class, and
  per-route validation were the wrong shape for a "the database rejected the
  payload" failure that one exception handler translates for every endpoint.
- #62861 — `POST /pools` returning `409` on an existing pool: closed; reviewers
  noted the case is already handled by the global handler, and that route-local
  versions of these exceptions were deliberately simplified into it.
- #64251 — Dag-stats failure when the display name is `NULL`: closed in favour of
  #64256, because duplicating the hybrid-property logic into a second place risks
  the two copies diverging when one is updated.
- #63406 and #64688 — two independent PRs fixing duplicate `Date` headers in
  responses: the workable form of the fix is at the ASGI/middleware layer, not in
  the handlers producing the responses.
- #66402, #66410, #66445 — a chain of route/listener/supervisor PRs for one
  feature, closed together once the underlying state addition was dropped:
  behaviour spread across layers has to be withdrawn from all of them at once.
