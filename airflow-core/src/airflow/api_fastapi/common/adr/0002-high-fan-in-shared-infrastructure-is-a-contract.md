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

The helpers here have the highest fan-in in the API layer. A single `FilterParam` /
`SortParam` / `RangeFilter` class, a `*_factory`, or `paginated_select` is mounted by
*many* routes at once, and the query parameters they declare are a published contract:
each `Query(alias=..., default=..., description=...)` flows into the generated OpenAPI
spec (`openapi/`) and from there into the code-generated Python/Go SDKs and the UI's
TypeScript client. A diff to `parameters.py` looks like one edited helper; what changes
is every endpoint mounting it and every consumer generated from the spec.

Two properties follow. **Broad blast radius:** a bare change to a `to_orm` body or to
`paginated_select` re-behaves every caller — a regression in the pipe-OR (`|`) split,
the match-all (`~`) alias, datetime-range predicate generation, or a default value is
an API-wide bug the small-fixture test on one route will not catch. **Backward-
compatibility sensitivity:** renaming/retyping/re-defaulting a param, or editing its
description, changes the published contract external integrators and the UI depend on.
The reusable class is the natural place to add a *new* filter, but also where a careless
rename silently breaks downstream clients. So additions are made *once* as reusable,
additive building blocks, and existing signatures/param metadata are a stable interface.

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
- **The converse is equally true: a filter only one route needs stays in that
  route.** This layer is expensive precisely because it is high-fan-in; every class
  added to it is mounted, generated into the SDKs, and maintained for everyone.
  A genuinely single-route filter promoted here buys nothing and costs contract
  surface. The trigger for promotion is a *second* route needing it, or an
  identical inline copy already existing elsewhere — not the possibility that one
  might.
- **Dialect coverage is part of the contract.** A change to SQL generation holds for
  SQLite, MySQL, and PostgreSQL, because the same helper serves all deployments.

## Consequences

- One correctly-built shared filter gives every route the same tested behaviour; new
  capabilities reach the whole API by extension, not duplication.
- The generated OpenAPI spec and downstream SDKs/UI stay in step, because param
  metadata is treated as the published interface it is.
- Cost is front-loaded onto proving a change is additive and backward-compatible across
  all callers — the price of the fan-in.

A change **violates** this decision when it:

- renames, retypes, re-defaults, or re-describes a query param mounted by a public
  route without treating it as a backward-incompatible contract change (regenerated
  spec, justification, version/flag gate);
- alters an existing shared `to_orm` / `paginated_select` / factory in a way that
  silently re-behaves current callers instead of adding an opt-in, safely-defaulted
  extension;
- writes a filter/param inline in a route when an equivalent one already exists
  here, or when another route in the same PR needs the same thing — creating a
  near-duplicate that will drift (a filter used by exactly one route is correctly
  left in that route);
- changes SQL generation in a way that only holds for one backend, breaking the
  other supported dialects the shared helper serves.

A reviewer should weigh any edit here against *all* of its callers and the generated
contract, not just the endpoint that motivated it.

## Evidence

- #60008 — added the pipe-`|` OR capability to shared search params, reaching every
  search endpoint at once.
- #65190, #68459 — API-wide regressions in the shared OR split (single-term edge cases;
  composite keys with `|`), fixed for all callers.
- #66696 — reworked the shared range-filter SQL, benefiting every range-filtered
  endpoint.
- #66979 — cross-cutting fix (ruff B008) to how shared `Depends`/`Query` defaults are
  built.
- #68657 — added a new reusable filter (any DagRun state) as a shared class, not an
  inline per-route query.
- #64611 — extended the shared JSON-key-value filter additively for its callers.
- #69913 — a single shared-query change whose correctness had to hold across the
  endpoints sharing the path.
