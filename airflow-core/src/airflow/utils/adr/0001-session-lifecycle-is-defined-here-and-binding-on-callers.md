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

`airflow-core/src/airflow/utils/session.py` implements airflow-core's database
transaction model. `create_session()` is the only sanctioned place a session is
opened, committed, rolled back, and closed; `provide_session` injects one *only
when the caller did not already supply one*; `NEW_SESSION` is the typing sentinel
letting a decorated function annotate `session: Session`. The behaviour that
matters is the pass-through: if a `session` was given, `provide_session` calls
straight through without wrapping, so a nested call joins the caller's transaction
rather than starting a second. Scheduler critical-section atomicity, clean API
rollback, and the HA row locks held across a loop iteration all rest on that.

Two things break it, both recurring review findings codebase-wide:

- **A positional `session` parameter.** `provide_session` detects a passed
  session by `"session" in kwargs` or position index; positional `session` lets
  an unrelated refactor that reorders arguments silently switch reuse for a fresh
  session — invisible at the call site, surfacing later as a split transaction.
- **A `session.commit()` inside a function that received a session.** It commits
  the caller's in-progress transaction out from under it; the caller's rollback
  then has nothing to roll back, and in the scheduler it releases HA row locks
  mid-iteration — why `prohibit_commit` / `CommitProhibitorGuard` in
  `sqlalchemy.py` raises the shouty `"UNEXPECTED COMMIT - THIS WILL BREAK HA
  LOCKS!"` inside a guarded block.

This directory is the rule's definition site, not merely a consumer. Two prek
hooks make it mechanical: `check-no-new-provide-session-positional` freezes the
set of functions still declaring `session` positionally, and a second forbids
importing `NEW_SESSION` / `provide_session` / `create_session` from
`airflow.utils.db` (which would reintroduce an import cycle). The adjacent
`airflow/models/adr/0003-session-discipline-and-set-based-db-access.md` applies
these rules to ORM/model code; this ADR is about the *mechanism itself*.

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

- A nested helper joins its caller's transaction, so an error anywhere rolls the
  whole unit of work back; no partial write escapes.
- The scheduler holds row locks across a loop iteration knowing no helper will
  quietly release them.
- Argument-reordering refactors cannot silently change transaction behaviour,
  because `session` is never positional in new code.
- The cost: a function cannot make its own writes durable in isolation. Code that
  genuinely needs its own transaction opens one where that is the explicit design
  — and says why — rather than calling `commit()` on a borrowed session.
- Changing `provide_session`'s injection logic changes every DB-touching function
  in airflow-core at once, and is treated as such in review.

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

- #67150 — prek hook enforcing keyword-only `session` on `@provide_session`,
  with a frozen allowlist for existing debt.
- #67777 — the follow-up sweep paying down positional-`session` debt in this
  directory.
- #61036 — connection resolution fixed via server process context in decorators:
  how much behaviour rides on the session/context decorators here.
