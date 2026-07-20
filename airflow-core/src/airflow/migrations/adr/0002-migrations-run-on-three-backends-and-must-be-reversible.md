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

# 2. Migrations run on Postgres, MySQL and SQLite, must be reversible, and keep one linear head

Date: 2026-07-19

## Status

Accepted

## Context

Airflow supports three metadata backends — PostgreSQL, MySQL, and SQLite — and a
single migration revision must run correctly on *all* of them, in *both*
directions. This is where most of the post-merge pain in this area comes from,
because the backends disagree in exactly the places migrations touch:

- **SQLite** cannot `ALTER` a column in place; structural changes go through
  `op.batch_alter_table(...)`, which recreates the table — and SQLite *enforces
  foreign keys during that recreation*, so DML and constraint work must be
  sequenced around FK handling (`disable_sqlite_fkeys(op)`), not before it.
- **MySQL** reserves keywords (a column literally named `interval` must be
  quoted), imposes row and sort-buffer limits, and generates UUIDs/defaults
  differently.
- **PostgreSQL** has sequences and autoincrement objects that must be restored on
  downgrade, and JSONB conversions that fail on invalid input.

On top of the cross-backend requirement, every `upgrade()` needs a `downgrade()`
that genuinely restores the prior state, because operators *do* downgrade between
releases — a broken, lossy, or stubbed downgrade is a real production failure, not
a theoretical one. And the chain itself must stay **linear**: exactly one Alembic
head, each revision chained to its predecessor via `down_revision`. Two authors
branching independently create a second head, which then needs an `alembic merge`
and confuses the replay order.

CI encodes these lessons — AST anti-pattern checks, a round-trip / FK-cascade
check, and the migration-reference tooling that flags multiple heads — but they
catch known shapes, not every new mistake. The steady stream of "fix SQLite
downgrade", "fix MySQL migration", "restore sequence on downgrade" fixes shows how
easily a revision that is green on one backend breaks on another.

## Decision

A migration revision must be **backend-agnostic, reversible, and part of a single
linear chain**:

- **Runs on Postgres, MySQL, and SQLite alike.** Use `op.batch_alter_table(...)`
  for alter/drop/constraint changes; route column types through `db_types.py`;
  sequence DML inside `disable_sqlite_fkeys(op)` where SQLite FK enforcement
  applies; quote reserved words; respect backend limits.
- **Provides a real, tested `downgrade()`** that restores the prior schema *and*
  data shape (including sequences/autoincrement and nullability), verified by a
  round-trip test, not a stub. Some upgrades are irreversible by construction —
  filling a `NULL` with a safe default destroys the information that the value was
  ever `NULL`, and no downgrade can recover it. That is acceptable, but it must be
  *declared*: the revision's docstring states which data the downgrade does not
  restore and why it cannot.
- **Keeps one linear head.** The new revision's `down_revision` is the current
  head; `alembic heads` returns exactly one. Rebase onto the latest head instead
  of creating a second head that requires a merge.

## Consequences

- In-place upgrades and downgrades stay reliable on every supported backend, and
  operators can move a release in either direction without stranding data.
- Authoring costs more: a change must be reasoned about (and ideally tested) on
  all three backends, and downgrade is first-class work, not an afterthought.
- The linear chain keeps replay order unambiguous and makes the
  migration-reference graph meaningful.

A change **violates** this decision when it:

- emits engine-specific DDL that breaks another backend (a bare `op.alter_column`
  outside `batch_alter_table` that fails on SQLite; an unquoted reserved MySQL
  keyword; a Postgres-only construct with no fallback);
- runs DML before/around SQLite FK handling incorrectly, or omits
  `disable_sqlite_fkeys` where batch recreation needs it;
- ships an `upgrade()` with a missing or stubbed `downgrade()`, one that fails to
  restore sequences/nullability, or one that silently loses data — an intentionally
  one-way data change is acceptable when the revision's docstring states why the
  original values cannot be recovered;
- introduces a second Alembic head instead of chaining onto the current head.

A reviewer should reject any revision that has not been shown to round-trip on all
three backends or that forks the head.

## Evidence

- #64972 — "Add static analysis tests for DB migration anti-patterns": AST checks
  that detect cross-backend anti-patterns (e.g. DML before `disable_sqlite_fkeys`)
  across all migration files.
- #63437 — "Fix SQLite downgrade failures caused by FK constraints during batch
  table recreation": SQLite FK enforcement during `batch_alter_table` recreation.
- #61976 — "Fix: Restore task_instance_history sequence on downgrade": a downgrade
  that failed to restore a Postgres sequence.
- #63625 — "Improve 0102 make_external_executor_id_text downgrade migration": a
  downgrade path that needed to actually restore the prior column shape.
- #63494 — "Quote reserved MySQL keyword 'interval' in deadline_alert queries": a
  MySQL-specific breakage from an unquoted reserved word.
- `0108_3_2_0_fix_migration_file_ORM_inconsistencies.py` — the declared-one-way
  case: its `downgrade()` docstring records that the `NULL`-to-default backfill in
  `upgrade()` is deliberately not reversed, because a filled default "cannot be
  distinguished from legitimately-populated values after the fact". It shipped in
  3.2.0 on that basis.
