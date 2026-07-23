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

`db_cleanup.py` is the retention and archival engine behind `airflow db clean`,
and the reference implementation the repo `CLAUDE.md` points at for *every* bulk
`DELETE`/`UPDATE` in airflow-core — the scheduler loop, interval callbacks, and
any new garbage-collection feature. Its pattern is an architectural decision for
the whole codebase, not a local detail.

The tables are user-driven and unbounded (millions of `task_instance`, `dag_run`,
log, and history rows). Written as one sweeping statement — the anti-pattern —
three things break: **lock duration** (an unbounded `DELETE` holds row/range/page
locks for the whole transaction, blocking every concurrent writer); **memory**
(reading a whole table in one pass to archive is OOM at production counts); and
**foreign keys / dialect behaviour** (a row under an `ON DELETE RESTRICT` FK, e.g.
`task_instance.dag_version_id` → `dag_version`, fails the delete outright, and on
MySQL the failed delete holds metadata locks while the archive-table `DROP` —
bound to the engine rather than this session's connection — checks out a *second*
pooled connection and hangs forever).

`_do_delete` encodes the answers: bound the query with `query.limit(batch_size)`,
stop when it selects nothing, archive each batch into a timestamped
`_airflow_deleted__*` table, delete from source, and **commit after each batch**.
`_TableConfig` carries the per-table metadata — indexed recency column, dependent
tables archived first, and `skip_if_referenced` pairs excluding `RESTRICT`-FK
rows. The batching driver is the one place in `airflow-core` that legitimately
calls `session.commit()` on a session it was passed: commit cadence *is* its
contract — the narrow, documented exception to ADR 1, not a loophole.

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
  memory; the deployment stays writable throughout.
- Total wall time and statement count go up — a deliberate trade: many short
  index-backed transactions beat one long one that blocks the cluster.
- Every new retention/GC feature designs its batch size, commit cadence, and
  supporting index up front, shipping the index in the same change.
- Archive tables are a durable artifact operators must manage; deleting without
  archiving is opt-in (`skip_archive`), not the default.

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

- #51510 — `--batch-size` introduces the bounded-batch loop, operator-tunable.
- #51268 — `_dump_table_to_file` reads in batches: same bounding on the export
  path.
- #51952 — archives dependent tables before parents for cascading deletes.
- #66296 — the MySQL hang from dropping the archive table on a second pooled
  connection, and the rollback-before-cleanup fix.
- #68339 — `skip_if_referenced` excludes `RESTRICT`-FK `dag_version` rows instead
  of failing the delete.
- #68218 — adding `task_store` via `_TableConfig`: canonical new-table shape.
- #64818 — indexes on `dag_run.created_dag_version_id` /
  `task_instance.dag_version_id`: the indexed-filter requirement as its own change.
- #65239 — `--error-on-cleanup-failure` surfaces cleanup failures instead of
  swallowing them.
