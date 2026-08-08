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

# 3. DB/session discipline, safe filter composition, and error translation live here

Date: 2026-07-19

## Status

Accepted

## Context

These helpers sit between untrusted client input and SQL run against the metadata
database. Three responsibilities are concentrated here so they hold for every endpoint:

- **Session discipline.** `paginated_select` is `@provide_session` with a keyword-only
  `session`; the repo-wide rule that an `airflow-core` function taking a `session` must
  not call `session.commit()` applies to every helper here. A helper that opened its
  own session mid-operation, or committed a caller's in-progress transaction, would
  split one logical unit of work across two transactions and break rollback.
- **Safe filter composition.** User query-strings become SQL through `parameters.py`,
  and every path is vetted. `SortParam` resolves ordering only against an allow-list —
  a field not on the list is a `400`, never interpolated into `ORDER BY`. Literal-match
  filters escape `LIKE`/`ILIKE` metacharacters (`_escape_like_pattern`) so a user's `%`
  or `_` cannot widen the match; only explicit `*_pattern` params expose wildcards.
  Datetimes are parsed defensively — a bad value is a `400`, not a crash deep in the
  query.
- **Error translation.** `ERROR_HANDLERS` in `exceptions.py` turn raw SQLAlchemy errors
  into the right status instead of a leaking `500`: unique-constraint `IntegrityError`
  → `409`, `DataError` (a value the DB rejects after Pydantic passes it) → `422`, Dag
  `DeserializationError` → a redacted `500`. Raw SQL and stack traces are gated behind
  `[api] expose_stacktrace` and tied to a correlation id, not echoed by default.

Getting these right in the shared layer lets each route boundary raise the correct
`HTTPException` (see the sibling `core_api` error-translation ADR) instead of
re-deriving the mapping — or leaking a `500` — per handler.

## Decision

The shared helpers enforce session discipline, compose filters safely from
untrusted input, and translate DB errors centrally:

- **Session is threaded, never re-opened or committed here.** Helpers take a
  keyword-only `session` and use the one they are given; no helper in this directory
  calls `session.commit()` — the caller owns the transaction boundary.
- **User input never reaches SQL as an un-vetted column or metacharacter.** Ordering
  fields are validated against `SortParam`'s allow-list; literal-match filters escape
  `LIKE`/`ILIKE` metacharacters; datetimes/typed values are parsed defensively and a
  bad value is a client error (`400`/`422`), not an unhandled exception.
- **DB errors are translated to the right status centrally.** New DB/domain error
  mappings extend the `exceptions.py` handlers (constraint → `409`, rejected value →
  `422`, missing/invalid → `404`/`400`) rather than being caught ad hoc per route or
  left to surface as a `500`.
- **Internal detail is redacted by default.** SQL statements and stack traces are
  exposed only under `[api] expose_stacktrace`, with a correlation id linking the
  redacted client message to the server log.

## Consequences

- Transactions stay atomic and each route keeps control of commit/rollback, so a
  partial write does not leak out on error.
- User-driven filtering and ordering cannot smuggle a column name or an unescaped
  wildcard into SQL, and a malformed value fails as a clean client error.
- Clients receive accurate, non-leaking status codes because the error mapping is
  defined once and shared, not re-derived per handler.

A change **violates** this decision when, in this shared layer, it:

- opens a new session inside a helper that already received one, or adds a
  `session.commit()` to a function that takes a `session`;
- interpolates a client-supplied string into `order_by` outside `SortParam`'s
  allow-list, or drops the `LIKE`/`ILIKE` escaping on a literal-match filter so a
  user wildcard widens the match;
- lets a malformed datetime / typed value raise an unhandled error instead of a
  `400`/`422`;
- lets a DB error (constraint violation, rejected value) surface as a raw `500`
  instead of extending the shared handlers to the right status;
- echoes SQL statements or stack traces to the client without gating them behind
  `[api] expose_stacktrace`.

A reviewer should reject any helper here that reaches SQL with un-vetted user input,
mishandles the session, or lets a database error escape as an internal-leaking `500`.

## Evidence

- #67496 — escaped `LIKE` wildcards in non-search filters so a user `%`/`_` cannot
  widen the match.
- #66888 — added the `DataError` → `422` translation so a DB-rejected value is a client
  error, not a `500`.
- #68512 — consolidated the SQLAlchemy-error-to-HTTP translation into one reusable base.
- #63028 — redacted raw SQL from the client response on constraint failure, gated behind
  config.
- #68388 — gated raw Dag deserialization detail behind `api.expose_stacktrace`, tied to
  a server-side correlation id.
- #64963 — kept vetted user-driven filtering on indexed columns rather than an
  unsafe/un-indexed predicate.
