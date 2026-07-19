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

# 1. Bounded, paginated querying is centralized here

Date: 2026-07-19

## Status

Accepted

## Context

Every list endpoint in the API serves a user-driven query against tables shared by
the whole deployment. If each route hand-rolled its own `.limit()`, `.offset()`,
count, and `order_by`, three failure modes would recur across the surface: an
unbounded `SELECT` that passes a small-fixture test and then returns — or scans —
the whole table at real data volume; an ordering built from a client-supplied
column name; and an offset-based page that silently drops or duplicates rows when
rows shift underneath it.

This directory concentrates that logic in one place so it is correct *once*
instead of re-derived per route:

- `paginated_select` (and its async twin) in `db/common.py` is the single path a
  list query takes to its count, filters, ordering, offset, and limit. It is
  `@provide_session` with a keyword-only `session`.
- `parameters.py` supplies the reusable `LimitFilter` / `OffsetFilter` (which clamp
  to `[api] maximum_page_limit` / `fallback_page_limit`) and `SortParam`, which
  resolves ordering only against an **allow-list** of model attributes — anything
  else is a `400` — and always appends the primary key as a deterministic
  tiebreaker.
- `cursors.py` provides keyset (cursor) pagination. Its predicate must select rows
  strictly after the cursor in `ORDER BY` order *without* dropping or duplicating
  rows when the sort column is nullable, and must keep the `ORDER BY` a bare column
  so a composite index still applies. NULL placement differs by backend
  (PostgreSQL sorts NULLs last for ASC; MySQL and SQLite treat NULL as lowest), and
  the predicate encodes that per dialect.

Because these helpers are shared, getting them right protects every endpoint at
once — and getting them wrong mis-answers or degrades every endpoint at once.

## Decision

List queries are bounded and ordered through the shared helpers, not per route:

- **Route list queries go through `paginated_select`** for count, filter
  application, ordering, offset, and limit. A route does not hand-roll an unbounded
  scan or a parallel pagination path around the helper.
- **Page size is clamped to the configured maximum.** `LimitFilter` bounds the
  limit to `[api] maximum_page_limit`, falling back to `fallback_page_limit`; new
  pagination does not reintroduce an unbounded default.
- **Ordering is allow-listed and stable.** `SortParam` orders only attributes on the
  model's allowed set (rejecting a client-controlled field with a `400`) and always
  carries a primary-key tiebreaker so the order is total.
- **Cursor pagination is stable across nullable columns and backends.** The keyset
  predicate must not drop or duplicate rows on a nullable sort column, must match
  the backend's native NULL placement, and must keep the `ORDER BY` index-usable.

## Consequences

- List endpoints stay responsive at deployment scale because boundedness and
  index-friendly ordering are structural, not per-route diligence.
- A pagination fix (a nullable-column keyset bug, a dropped edge case) is made once
  in the shared helper and every endpoint benefits.
- Cursor pagination gives stable pages under concurrent writes where offset paging
  would skip or repeat rows.
- Routes carry the cost of adapting to the shared helpers' shape rather than writing
  a bespoke query — intentional, because that uniformity is the guarantee.

A change **violates** this decision when it:

- issues an unbounded list query, or hand-rolls `.limit()` / `.offset()` / count /
  `order_by` in a route instead of going through `paginated_select`;
- adds a second pagination path around the shared helper, or a per-endpoint page cap
  that bypasses `maximum_page_limit` / `fallback_page_limit`;
- builds `order_by` from a client-supplied column name outside `SortParam`'s
  allow-list, or drops the primary-key tiebreaker so the order is non-deterministic;
- writes or edits a keyset predicate that drops or duplicates rows on a nullable
  sort column, mismatches a backend's NULL placement, or function-wraps the sort
  column so the `ORDER BY` can no longer use an index.

A reviewer should reject a list endpoint that reaches the database without going
through the bounded, allow-listed, tiebroken shared pagination path.

## Evidence

- #65604 — "Add cursor based pagination for get_dag_runs endpoint": moved a list
  endpoint onto the shared keyset path instead of an unbounded/offset scan.
- #64845 — "Add cursor based pagination for get_task_instances endpoint": same,
  for task instances.
- #68869 — "Fix cursor pagination dropping rows when sorting by a nullable column":
  the keyset-stability failure this ADR guards against, fixed once for every caller.
- #67973 — "Fix cursor encoding for column-form SortParam to_replace": kept the
  cursor token correct for column-form sort mappings rather than emitting a broken
  token.
- #68842 — "Fix race condition on sort param when returning dagRuns": corrected
  shared sort-param state that affected the returned ordering.
- #60989 — "Respect maximum page limit in API": enforced the configured page-limit
  clamp so no endpoint returns an unbounded page.
- #61067 — "Deprecate api.page_size config in favor of api.fallback_page_limit":
  consolidated the page-limit configuration the shared limit filter reads.
- #64963 — "Update search parameters to better leverage DB indexes": kept
  user-driven filtering/ordering on indexed columns so the bounded query stays fast
  at scale.
