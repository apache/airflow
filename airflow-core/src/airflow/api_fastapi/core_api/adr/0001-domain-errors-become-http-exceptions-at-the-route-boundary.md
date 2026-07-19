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

The API server is a layered application. Route handlers in `routes/public/` and
`routes/ui/` sit on top of service and domain code — SQLAlchemy queries, the
serialization layer, asset/backfill/state-store services — that raises
domain-shaped errors: a `ValueError` for invalid input, a lookup that returns
nothing for a missing row, a validation failure deep inside a builder. Those
layers know *what went wrong*; they do not know, and should not know, the HTTP
status that a client at the edge should see.

FastAPI's default behaviour for an unhandled exception is a bare
`500 Internal Server Error`. That outcome is doubly bad for a public API:

- **It misleads the client.** A malformed filter, an unknown `dag_id`, or an
  out-of-range parameter is the *caller's* mistake — a `400` or `404` — not a
  server fault. Reporting it as `500` tells integrators and the UI that Airflow
  broke, and makes retries and error handling on the client side wrong.
- **It leaks internals.** An unhandled exception surfaces a stack shape, an ORM
  error string, or an internal field name to an external caller, exposing
  implementation detail through the error channel.

The recurring pressure is that the happy path is easy and the error path is
invisible in a quick read: a handler calls a service, the service raises on a
bad row or bad input, and — with no translation — it escapes as `500`. The
symptom only appears when a client sends the awkward input (a `~` sentinel
`dag_id`, a `NULL` timestamp, a malformed asset expression). This is the single
most frequent class of fix in this area's history.

## Decision

Route handlers translate domain-layer errors into an explicit `HTTPException`
with the correct status at the FastAPI route boundary. Concretely:

- A **missing resource** becomes `404`; **invalid or malformed input** becomes
  `400`; other conditions get the status that honestly describes them (e.g. a
  transient DB lock as `503`). A domain error must never escape a handler as an
  unhandled `500`.
- Handlers **catch the specific domain exception** the service layer raises
  (e.g. `ValueError` for a missing row / invalid input) and re-raise as
  `HTTPException` — they do not broaden to a bare `except` that hides the real
  failure, and they do not swallow the error into a misleading success.
- The status codes a handler can return are **declared in the OpenAPI docs**
  via `create_openapi_http_exception_doc`, so the generated contract matches the
  handler's real behaviour rather than a hand-written guess.

This keeps the domain layer HTTP-agnostic and concentrates the
status-code decision at the one boundary that owns the client contract.

## Consequences

- Clients and the UI receive honest, actionable status codes; genuine server
  faults (`500`) once again mean an actual server fault, so they are alerting-
  worthy again.
- Internal exception detail stays inside the server instead of leaking through
  the error channel.
- Every handler carries the small, explicit cost of catching and re-raising —
  intentional friction that keeps the contract correct.
- The declared OpenAPI error responses stay truthful, because they are declared
  where the errors are actually raised.

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

- #67489 — "Return 400 instead of 500 from `structure_data` on malformed
  asset_expression": translated a malformed-input domain error into the correct
  `400` at the boundary instead of letting it escape as `500`.
- #67445 — "Return 400 instead of 500 from `materialize_asset` for invalid
  validation input": same pattern on the asset-materialization route.
- #67363 — "Fix `/dags/{dag_id}/latest_run` returning 500 instead of 400 for
  `dag_id=\"~\"`": a sentinel/invalid path parameter must be a `400`, not a
  server fault.
- #68338 — "Fix 500 error for event logs with NULL dttm": a data condition that
  was surfacing as an unhandled `500`.
- #69110 — "Fix Dag detail page 500 for Dags without a fileloc": a missing-field
  case corrected so it no longer 500s.
- #64130 — "Fix DAG run trigger to surface errors instead of swallowing them":
  the inverse failure — an error hidden behind a misleading success — corrected
  to surface at the boundary.
- #62624 — "Replaced manual response descriptions with
  `create_openapi_http_exception_doc`": moved status-code documentation to the
  shared generator so declared errors match handler behaviour.
