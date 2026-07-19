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

# 2. This layer is high-fan-in shared infrastructure and its parameter API is a contract

Date: 2026-07-19

## Status

Accepted

## Context

The helpers in this directory have the highest fan-in in the API layer. A single
`FilterParam` / `SortParam` / `RangeFilter` class, a `*_factory` function, or
`paginated_select` is mounted by *many* routes at once, and the query parameters
those helpers declare are not an internal detail — they are a published contract.
Each `Query(alias=..., default=..., description=...)` a factory builds flows into
the generated OpenAPI spec (`openapi/`), and from there into the code-generated
Python/Go SDKs and the UI's TypeScript client. A reviewer looking at a diff to
`parameters.py` sees one edited helper; what actually changes is every endpoint
that mounts it and every consumer generated from the spec.

This produces two properties a change here must respect:

- **Broad blast radius.** A bare change to a `to_orm` body or to `paginated_select`
  re-behaves every caller. A regression in the pipe-OR (`|`) split, in the
  match-all (`~`) alias, in datetime-range predicate generation, or in a default
  value is not a one-endpoint bug — it is an API-wide one, and the small-fixture
  test on one route will not catch it on the others.
- **Backward-compatibility sensitivity.** Renaming a query param, retyping it,
  changing a default, or editing its description changes the published contract that
  external integrators and the UI depend on. The same reusable class is the natural
  place to add a *new* filter — which is desirable — but it is also the place where
  a careless rename silently breaks downstream clients.

Because the layer is shared, the discipline that keeps it healthy is the opposite of
per-route freedom: additions are made *once* as reusable, additive building blocks,
and existing signatures/param metadata are treated as a stable interface.

## Decision

Changes to these shared helpers are made as backward-compatible, single-source
additions, treating the parameter surface as a public contract:

- **A query param's name, alias, default, type, or description is a contract
  change.** Renaming/retyping/re-defaulting a param that a `routes/public/` endpoint
  mounts is backward-compatibility-sensitive across every route and every generated
  SDK/UI client; it is justified and version/flag-gated, not reshaped
  opportunistically.
- **Existing helper signatures and semantics are preserved for current callers.** A
  new capability is added as a keyword-only argument with a safe default or as a new
  class/factory; a behaviour change to an existing `to_orm` / `paginated_select` is
  opt-in, not a silent re-behaviour of every endpoint.
- **A reusable filter/param lives here exactly once.** A new filter that more than
  one route needs is added as a shared class or `*_factory`, not copied inline into
  a route — three near-duplicate copies drift.
- **Dialect coverage is part of the contract.** A change to SQL generation holds for
  SQLite, MySQL, and PostgreSQL, because the same helper serves all deployments.

## Consequences

- One correctly-built shared filter gives every route the same, tested behaviour;
  new query capabilities reach the whole API by extension rather than duplication.
- The generated OpenAPI spec and the downstream SDKs/UI stay in step, because param
  metadata is treated as the published interface it is.
- The cost of a change here is deliberately front-loaded onto proving it is additive
  and backward-compatible across all callers — that scrutiny is the price of the
  fan-in.

A change **violates** this decision when it:

- renames, retypes, re-defaults, or re-describes a query param mounted by a public
  route without treating it as a backward-incompatible contract change (regenerated
  spec, justification, version/flag gate);
- alters an existing shared `to_orm` / `paginated_select` / factory in a way that
  silently re-behaves current callers instead of adding an opt-in, safely-defaulted
  extension;
- copies a filter/param inline into a route instead of adding it once as a shared
  class/factory, creating a near-duplicate that will drift;
- changes SQL generation in a way that only holds for one backend, breaking the
  other supported dialects the shared helper serves.

A reviewer should weigh any edit here against *all* of its callers and the generated
contract, not just the endpoint that motivated it.

## Evidence

- #60008 — "support OR operator in search parameters": added the pipe-`|` OR
  capability to the shared search params, reaching every search endpoint at once.
- #65190 — "Fix run_id_pattern pipe OR operator dropping single-term edge cases":
  an API-wide regression in the shared OR split, fixed for all callers.
- #68459 — "Fix Dag run partition key filter breaking on composite keys with `|`":
  another shared-split edge case that surfaced across endpoints, not just one.
- #66696 — "Improve DB performance of datetime range filters in API queries":
  reworked the shared range-filter SQL, so every range-filtered endpoint benefited.
- #66979 — "Enable ruff B008 (function-call-in-default-argument) and fix
  violations": a cross-cutting fix to how the shared `Depends`/`Query` defaults are
  built across the parameter surface.
- #68657 — "Add support for filtering Dags by any DagRun state": added a new
  reusable filter as a shared class rather than an inline per-route query.
- #64611 — "Add extra field filtering for asset events": extended the shared
  JSON-key-value filter capability additively for its callers.
- #69913 — "Remove redundant ORM result uniquing in core queries": a single
  shared-query change whose correctness had to hold across the endpoints that share
  the path.
