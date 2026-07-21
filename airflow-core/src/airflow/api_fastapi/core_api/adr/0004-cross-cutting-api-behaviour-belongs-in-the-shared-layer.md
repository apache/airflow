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

The API has well over a hundred route handlers. Any behaviour that is *not* specific
to one resource — translating a constraint violation into a status code, rejecting an
over-large body, deriving a display name from a model, de-duplicating a response header
— has two possible homes: once in the shared layer (the FastAPI exception handlers in
`api_fastapi/common/exceptions.py`, a middleware, a shared datamodel, a shared query
helper), or N times in N handlers.

Contributors reaching for a concrete bug consistently choose the second. The result is
locally correct and globally wrong: it fixes the one endpoint the reporter hit, leaves
the defect in every sibling, and adds a divergence the next refactor must reconcile.
The real cost is drift — two copies of a derivation stay in sync only until someone
edits one, and the surface is large enough that nobody notices. A per-route validation
knob also enlarges the public configuration surface for a generic failure the shared
layer already knows how to report. Reviewers here close those PRs and land the shared
version: one exception handler covering every route, one hybrid property both models
read, one middleware.

## Decision

Behaviour that is not specific to a single resource is implemented once, in the shared
layer:

- **Generic failure modes are translated by the shared exception handlers**, not by
  `try`/`except` repeated in each handler. A constraint violation, an over-large or
  DB-rejected payload, or a deserialization failure gets a handler in
  `api_fastapi/common/exceptions.py` so every route reports it identically. A
  route-local `except` is for an error only that route can produce.
- **Do not add a config knob and a bespoke exception class for a failure the shared
  layer can report generically.** Prefer the handler that covers every endpoint over
  per-route validation plus new configuration.
- **A derivation lives in one place.** A field computed from a model (a display name, a
  fallback, a status roll-up) is a shared hybrid property / datamodel field that both
  the public and UI surfaces read — never copied into a second response model.
- **Response-envelope concerns (headers, content negotiation, compression, correlation
  ids) belong in middleware / app wiring**, not in individual handlers.
- **A new reusable filter, sort, or pagination behaviour goes into
  `api_fastapi/common/`** (see that area's ADRs), not inline in the route that needed
  it first.
- **Moving a fix into the shared layer does not waive the Execution API's versioning
  duty.** `api_fastapi/common/` is shared with the Execution API, so a handler or
  middleware change here can reshape Execution-API responses too. `execution_api/adr/0005`
  requires a Cadwyn `VersionChange` for any wire-visible response or error shape change
  on those routes. This decision says *where* the fix belongs; that one says what it
  additionally owes. A shared-layer error-handling change that alters an Execution-API
  wire shape satisfies both only by doing both — shared implementation **and** the
  migration wired through the SDK chain. Neither ADR is satisfied by citing the other.

## Consequences

- One fix covers every endpoint, including the ones nobody filed a bug against, and the
  public configuration surface stays small.
- Review of a shared-layer change is more expensive — it re-behaves every route at once,
  so it needs the broader test and thought. That is the deliberate trade: a wider blast
  radius reviewed once, rather than a narrow fix reviewed N times and applied to one.
- A contributor arriving with a single-endpoint reproduction has more work than the bug
  appeared to require, and may need a maintainer to point at the shared home first.

A change **violates** this decision when it:

- adds a `try`/`except` in a route handler for a failure class that any route could
  raise, instead of registering or extending a shared exception handler;
- introduces a new config option plus a new exception class to validate something the
  database or the shared layer already rejects;
- copies a model derivation (hybrid property, display-name fallback, computed status)
  into a second datamodel rather than sharing the existing one;
- fixes a response header, envelope, or content-negotiation defect inside one handler
  instead of in middleware;
- adds a filter/sort/pagination behaviour inline in a route when it belongs in
  `api_fastapi/common/`.

A reviewer should ask, on any fix here: *does this defect exist on the sibling
endpoints too?* If yes, the fix is in the wrong place.

## Evidence

- #66787 — Dag run `conf` size validation; closed for #66888 — a config knob + new
  exception + per-route validation is the wrong shape for a "database rejected the
  payload" failure one handler translates for every endpoint.
- #62861 — `POST /pools` `409` on existing pool; closed — already handled by the global
  handler, into which route-local versions were deliberately simplified.
- #64251 — Dag-stats `NULL` display name; closed for #64256 — duplicating the
  hybrid-property logic risks the two copies diverging.
- #63406, #64688 — duplicate `Date` headers; the workable fix is at the ASGI/middleware
  layer, not the handlers.
- #66402, #66410, #66445 — a route/listener/supervisor chain closed together once the
  underlying state addition dropped: cross-layer behaviour withdraws from all at once.
