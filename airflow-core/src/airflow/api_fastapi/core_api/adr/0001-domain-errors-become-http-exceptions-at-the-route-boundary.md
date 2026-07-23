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

# 1. Domain-layer errors are translated to HTTPException at the route boundary

Date: 2026-07-19

## Status

Accepted

## Context

Route handlers in `routes/public/` and `routes/ui/` sit on top of service and domain
code that raises domain-shaped errors (`ValueError` for bad input, an empty lookup
for a missing row, a deep validation failure). Those layers know *what* went wrong;
they do not, and should not, know the HTTP status a client should see.

FastAPI's default for an unhandled exception is a bare `500`, which is doubly bad for
a public API: it misleads the client (a bad filter or unknown `dag_id` is a `400`/`404`,
not a server fault) and it leaks internals through the error channel. The failure is
invisible in a quick read — the error path only fires on awkward input (a `~` sentinel
`dag_id`, a `NULL` timestamp, a malformed asset expression) — and it is the single most
frequent class of fix in this area.

## Decision

Route handlers translate domain-layer errors into an explicit `HTTPException` with
the correct status at the FastAPI route boundary.

**The translation rule itself is the repo-wide one** — see the "Translate
domain-layer exceptions to `HTTPException` at FastAPI route boundaries" entry in the
root `AGENTS.md` (missing resource → `404`, invalid input → `400`, catch the specific
exception, never let a domain error escape as `500`). This ADR does not restate it;
it adds the one requirement local to `core_api`:

- The status codes a handler can return are **declared in the OpenAPI docs** via
  `create_openapi_http_exception_doc`, so the generated contract matches real
  behaviour rather than a hand-written guess. This matters here because
  [ADR-2](0002-public-rest-api-v2-is-a-generated-stable-contract.md) makes the
  generated spec a stable published artefact: an undeclared status is a contract
  defect, not just a documentation gap.

This keeps the domain layer HTTP-agnostic and concentrates the status-code decision
at the one boundary that owns the client contract.

## Consequences

- Clients and the UI receive honest, actionable status codes; a `500` once again
  means a genuine server fault and is alerting-worthy.
- Internal exception detail stays inside the server.
- Every handler carries the small, explicit cost of catching and re-raising —
  intentional friction that keeps the contract correct and the OpenAPI error docs
  truthful.

A change **violates** this decision when, in a `core_api` route handler, it:

- lets a domain error (`ValueError`, a not-found, a validation failure) propagate
  unhandled so the client receives a `500` for what is really a `400`/`404`;
- returns the wrong status — e.g. `404` for invalid input, or a success for an
  operation that actually failed — instead of the one that describes the cause;
- swallows an exception into a misleading success response, or catches it with a
  broad `except` that hides the real failure class;
- adds or changes the statuses a handler returns without keeping the OpenAPI
  response docs (`create_openapi_http_exception_doc`) in sync.

A reviewer should reject any handler that can surface a domain error as a raw
`500`, or that reports the wrong status for a client-side mistake.

## Evidence

- #67489 — malformed `asset_expression` → `400` not `500` at the boundary.
- #67445 — same pattern on the `materialize_asset` route.
- #67363 — sentinel `dag_id="~"` must be `400`, not a server fault.
- #68338 — event logs with `NULL` dttm surfacing as an unhandled `500`.
- #69110 — Dag detail page `500` for Dags without a fileloc, corrected.
- #64130 — inverse failure: an error hidden behind a misleading success, surfaced.
- #62624 — moved status-code docs to `create_openapi_http_exception_doc` so declared
  errors match handler behaviour.
