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

# 2. Bulk cleanup and maintenance writes are batched and bounded

Date: 2026-07-20

## Status

Accepted

## Context

`db_cleanup.py` is the retention and archival engine behind `airflow db clean`.
It is also the reference implementation the repo `CLAUDE.md` points at for
*every* bulk `DELETE`/`UPDATE` in airflow-core — the scheduler loop, interval
callbacks, and any new garbage-collection feature are all expected to follow the
shape defined here. That makes this module's pattern an architectural decision
for the whole codebase, not a local implementation detail.

The tables it operates on are user-driven and unbounded. A deployment with many
Dags and a long retention window accumulates millions of `task_instance`,
`dag_run`, log, and history rows. Written naively, retention is one sweeping
statement — which is exactly the anti-pattern, for three reasons:

- **Lock duration.** An unbounded `DELETE` takes row (and on some engines range,
  gap, or page) locks on every matching row and holds them for the whole
  transaction. Every concurrent writer touching those rows — other schedulers,
  the API server, workers via the Execution API — blocks for the duration.
- **Memory.** Reading or exporting a whole table in one pass to archive it is an
  out-of-memory failure at production row counts.
- **Foreign keys and dialect behaviour.** Rows still referenced by an
  `ON DELETE RESTRICT` foreign key (for instance `task_instance.dag_version_id`
  pointing at `dag_version`) make the delete fail outright. On MySQL a failed
  delete leaves the transaction holding metadata locks, and the subsequent
  archive-table `DROP` — if bound to the engine rather than to this session's own
  connection — checks out a *second* pooled connection and blocks on those locks
  forever. That is a hang, not an error.

`_do_delete` encodes the answers. It loops: bound the working query with
`query.limit(batch_size)`, stop when the bounded query selects nothing, archive
the batch into a timestamped `_airflow_deleted__*` table, delete it from the
source, and **commit after each batch** so locks are released and other writers
make progress. `_TableConfig` carries the per-table metadata this needs — the
indexed recency column to filter on, the dependent tables that must be archived
first so cascading deletes stay consistent, and `skip_if_referenced` pairs that
exclude rows a `RESTRICT` FK still points at.

The batching driver is the one place in `airflow-core` that legitimately calls
`session.commit()` while holding a session it was passed: commit cadence *is* its
contract. That is the narrow, documented exception to ADR 1, not a loophole.

## Decision

Any bulk `DELETE` or `UPDATE` against a user-driven table follows the
`db_cleanup._do_delete` pattern:

1. **Bound each statement with a `LIMIT`** (batch size) rather than issuing one
   unbounded statement.
2. **Commit between batches** so locks are released and concurrent writers
   proceed; loop until the bounded query selects no further rows.
3. **Filter on indexed columns**, so each batch is a cheap indexed range rather
   than a full table scan repeated once per batch.
4. **Archive before deleting**, and archive dependent tables before their
   parents, so a cascading delete does not orphan rows that were meant to be
   recoverable.
5. **Exclude rows protected by a `RESTRICT` foreign key** (`skip_if_referenced`)
   rather than issuing a delete that is guaranteed to fail.
6. **Bind cleanup DDL to the session's own connection** (`session.connection()`),
   never to `session.get_bind()`, when the session may hold an open transaction
   — a second pooled connection deadlocks on MySQL metadata locks.
7. **Roll back a failed batch before the `finally` cleanup runs**, and never let
   an error raised during cleanup replace the original exception.
8. **Read and export in batches too** — never materialize an entire table to
   archive or dump it.

Adding a table to `airflow db clean` means adding a `_TableConfig` with an
indexed recency column, its `dependent_tables`, and any `skip_if_referenced`
pairs — not adding a bespoke delete path.

## Consequences

- Cleanup of an arbitrarily large table runs with bounded lock hold time and
  bounded memory; the deployment stays writable throughout.
- Total wall time and statement count go up. That trade is deliberate: many
  short, index-backed transactions beat one long one that blocks the cluster.
- Every new retention or garbage-collection feature has to design its batch
  size, commit cadence, and supporting index up front — and ship the index in
  the same change, not later.
- The archive tables are a durable artifact operators must manage; deleting
  without archiving is opt-in (`skip_archive`), not the default.

A change **violates** this decision when it:

- issues an unbounded `DELETE`/`UPDATE` — one whose row set is bounded neither by
  a `LIMIT` nor by an explicit, already-enumerated set of primary keys — against a
  user-driven table, in cleanup, in the scheduler loop, or in an interval
  callback;
- batches but does not commit between batches, so locks accumulate across the
  whole run;
- selects the batch on an unindexed column, turning every batch into a full
  scan;
- adds a table to `db clean` without an indexed recency column, without its
  `dependent_tables`, or without a `skip_if_referenced` entry for a `RESTRICT`
  FK that points at it;
- deletes a parent table's rows before archiving the dependent tables, so
  cascading deletes lose rows that should have been archived;
- binds a cleanup `DROP`/DDL to `session.get_bind()` while the session holds an
  open transaction, reintroducing the MySQL metadata-lock hang;
- lets an exception raised in a `finally` cleanup block mask the original
  failure, so the real cause never reaches the operator;
- loads a whole table into memory to archive, export, or dump it;
- calls `session.commit()` from a helper that is *not* the batching driver (see
  ADR 1).

A reviewer should reject any bulk mutation whose row set is unbounded (no `LIMIT`
and no enumerated key set), that lacks an inter-batch commit, or that filters on
an unindexed column.

## Evidence

- #51510 — "CLI: add `--batch-size` option to `airflow db clean`": introduces
  the bounded-batch loop and makes the batch size operator-tunable.
- #51268 — "Adjusted `_dump_table_to_file` to read rows in batches to prevent
  memory issues": the same bounding argument applied to the export path.
- #51952 — "Fix archival for cascading deletes by archiving dependent tables
  first": establishes the dependent-tables-before-parent ordering.
- #66296 — "Fix `airflow db clean` hang on MySQL when delete fails": the
  metadata-lock hang caused by dropping the archive table on a second pooled
  connection, and the rollback-before-cleanup fix.
- #68339 — "Skip FK-referenced `dag_version` rows during db clean": adds
  `skip_if_referenced` so a `RESTRICT` FK excludes rows instead of failing the
  delete.
- #68218 — "Add `task_store` table to `airflow db clean` mechanism": the
  canonical shape of adding a new table via `_TableConfig`.
- #64818 — "Add indexes on `dag_run.created_dag_version_id` and
  `task_instance.dag_version_id`": the indexed-filter-column requirement shipped
  as its own change.
- #65239 — "Add `--error-on-cleanup-failure` flag to `airflow db clean`":
  surfacing cleanup failures to the operator instead of swallowing them.
