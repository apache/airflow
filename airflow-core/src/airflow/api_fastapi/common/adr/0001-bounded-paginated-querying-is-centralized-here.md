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

Every list endpoint serves a user-driven query against tables shared by the whole
deployment. If each route hand-rolled its `.limit()`, `.offset()`, count, and
`order_by`, three failure modes would recur: an unbounded `SELECT` that passes a
small-fixture test then scans the whole table at real volume; an ordering built from a
client-supplied column name; and an offset page that drops or duplicates rows when they
shift underneath it.

This directory concentrates that logic so it is correct *once*:

- `paginated_select` (and its async twin) in `db/common.py` is the single path a list
  query takes to count, filters, ordering, offset, and limit. It is `@provide_session`
  with a keyword-only `session`.
- `parameters.py` supplies `LimitFilter` / `OffsetFilter` (clamping to `[api]
  maximum_page_limit` / `fallback_page_limit`) and `SortParam`, which resolves ordering
  only against an **allow-list** of model attributes — anything else is a `400` — and
  always appends the primary key as a deterministic tiebreaker.
- `cursors.py` provides keyset pagination. Its predicate must select rows strictly
  after the cursor in `ORDER BY` order *without* dropping or duplicating rows on a
  nullable sort column, and must keep the `ORDER BY` a bare column so a composite index
  still applies. NULL placement differs by backend (PostgreSQL sorts NULLs last for
  ASC; MySQL and SQLite treat NULL as lowest), and the predicate encodes that per
  dialect.

Because these helpers are shared, getting them right protects every endpoint at once —
getting them wrong mis-answers or degrades every endpoint at once.

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

- List endpoints stay responsive at scale because boundedness and index-friendly
  ordering are structural, not per-route diligence.
- A pagination fix is made once in the shared helper and every endpoint benefits.
- Cursor pagination gives stable pages under concurrent writes where offset paging
  would skip or repeat rows.
- Routes adapt to the shared helpers' shape rather than writing a bespoke query —
  intentional, because that uniformity is the guarantee.

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

- #65604, #64845 — moved get_dag_runs / get_task_instances onto the shared keyset
  path instead of an unbounded/offset scan.
- #68869 — fixed cursor pagination dropping rows on a nullable column: the
  keyset-stability failure this ADR guards, fixed once for every caller.
- #67973 — kept the cursor token correct for column-form SortParam mappings.
- #68842 — corrected shared sort-param state affecting the returned ordering.
- #60989 — enforced the configured page-limit clamp so no endpoint returns unbounded.
- #61067 — consolidated page-limit config (deprecated `api.page_size` for
  `api.fallback_page_limit`).
- #64963 — kept user-driven filtering/ordering on indexed columns so the bounded query
  stays fast at scale.
