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

# 1. Session lifecycle is defined here and is binding on every caller

Date: 2026-07-20

## Status

Accepted

## Context

`airflow-core/src/airflow/utils/session.py` is where airflow-core's database
transaction model is actually implemented. `create_session()` is the only
sanctioned place a session is opened, committed, rolled back, and closed;
`provide_session` injects one *only when the caller did not already supply one*;
and `NEW_SESSION` is the typing sentinel that lets a decorated function annotate
`session: Session` rather than `Session | None`.

The behaviour that matters is the pass-through: `provide_session` inspects the
call and, if a `session` was given, calls straight through without wrapping.
That is what makes a nested call join the caller's transaction rather than start
a second one. Every property the rest of core depends on — atomicity of a
scheduler critical section, the ability of an API handler to roll back cleanly,
the HA row locks the scheduler holds across a loop iteration — rests on that
single behaviour being respected by callers.

Two things break it, and both are recurring review findings across the whole
codebase, not just this directory:

- **A positional `session` parameter.** `provide_session` decides whether the
  caller passed a session by checking `"session" in kwargs` or by position
  index. When `session` is positional, an unrelated refactor that reorders or
  adds arguments silently changes whether the caller's session is reused or a
  fresh one is opened. The failure is invisible at the call site and shows up
  later as a split transaction.
- **A `session.commit()` inside a function that received a session.** That
  commits the caller's in-progress transaction out from under it. The caller's
  subsequent rollback then has nothing to roll back, and in the scheduler it
  releases HA row locks mid-iteration — which is why `prohibit_commit` /
  `CommitProhibitorGuard` in `sqlalchemy.py` raises the deliberately shouty
  `"UNEXPECTED COMMIT - THIS WILL BREAK HA LOCKS!"` when it happens inside a
  guarded block.

Because these primitives live here, this directory is not merely a consumer of
the rule — it is the rule's definition site. Two prek hooks make it mechanical:
`check-no-new-provide-session-positional` freezes the set of functions still
declaring `session` positionally, and a second hook forbids importing
`NEW_SESSION` / `provide_session` / `create_session` from `airflow.utils.db`
instead of `airflow.utils.session`, which would reintroduce an import cycle.

The adjacent `airflow/models/adr/0003-session-discipline-and-set-based-db-access.md`
applies these rules to ORM/model code and pairs them with set-based access and
lock ordering. This ADR is about the *mechanism itself*: what `session.py`
guarantees, and what a change to it — or to its callers — may not break.

## Decision

- **`create_session()` is the only place a session's lifecycle is managed.**
  Code does not construct `settings.Session()` directly, and does not
  hand-roll commit/rollback/close around one.
- **`session` is a keyword-only parameter** on every `@provide_session`
  function: `*, session: Session = NEW_SESSION`. New positional declarations are
  rejected by `check-no-new-provide-session-positional`; the allowlist next to
  that hook records existing debt and is not a place to add entries.
- **A function that receives a `session` uses that session.** It does not open
  `create_session()` — or any other new session — part-way through. One logical
  unit of work is one transaction.
- **The caller owns the transaction boundary.** A function in `airflow-core`
  that takes a `session` parameter does not call `session.commit()`. Commit
  happens where the session was created: in `create_session()`'s own contextual
  exit, at the `provide_session` boundary, or in an explicit batching driver
  that documents that it owns commit cadence (see ADR 2).
- **Where the transaction boundary is safety-critical, it is enforced at
  runtime,** not merely by convention: the scheduler wraps its critical section
  in `prohibit_commit(session)` and commits only via `guard.commit()`.
- **These symbols are imported from `airflow.utils.session`.** Re-exporting or
  importing them via `airflow.utils.db` is not allowed — it creates an import
  cycle between the session primitives and the migration machinery.

## Consequences

- A nested helper joins its caller's transaction, so an error anywhere in the
  unit of work rolls the whole thing back; no partial write escapes.
- The scheduler can hold row locks across a loop iteration with confidence that
  no helper it calls will quietly release them.
- Refactors that reorder arguments cannot silently change transaction behaviour,
  because `session` is never positional in new code.
- The cost is that a function cannot make its own writes durable in isolation.
  Code that genuinely needs its own transaction must open one at a level where
  that is the explicit design — and say why — rather than calling `commit()`
  from inside a borrowed session.
- Changing `provide_session`'s injection logic is a change to every DB-touching
  function in airflow-core at once, and is treated as such in review.

A change **violates** this decision when it:

- declares `session` positionally on a `@provide_session` function, or adds an
  entry to `known_provide_session_positional.txt` to let a new one through;
- calls `session.commit()` inside a function that takes a `session` parameter,
  outside an explicitly documented batching driver;
- opens `create_session()` (or instantiates `settings.Session()`) inside a
  function that already received a session;
- constructs and manages a session by hand instead of using `create_session()`,
  so commit/rollback/close is no longer guaranteed on the error path;
- weakens, bypasses, or silences `prohibit_commit` / `CommitProhibitorGuard`
  rather than fixing the unexpected commit it caught;
- imports `NEW_SESSION` / `provide_session` / `create_session` from
  `airflow.utils.db`;
- changes `provide_session`'s pass-through behaviour — for example always
  wrapping in a new session, or committing at the decorator boundary when the
  caller supplied the session — as an incidental part of a larger change.

## Evidence

- #67150 — "Add prek hook to enforce keyword-only `session` on
  `@provide_session`": makes the keyword-only rule mechanical rather than a
  review convention, with a frozen allowlist for existing debt.
- #67777 — "Remove findings from positional session check in Core Utils": the
  follow-up sweep that paid down the positional-`session` debt inside this
  directory itself.
- #61036 — "Fix connection resolution in CLI by setting server process context
  in decorators": shows how much behaviour rides on the session/context
  decorators in this area — a decorator-level detail changing what a caller
  actually reaches.
