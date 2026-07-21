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

# 3. Large data migrations must be batched and stay in sync with the models and serialization

Date: 2026-07-19

## Status

Accepted

## Context

Many revisions *move data* — backfilling a column, re-encoding a value, converting
a type, reformatting payloads. Unlike DDL, this scales with row count, and in
production those tables (`task_instance`, `xcom`, `dag_run`) hold *millions* of
rows. A step that is instant on a test database can, on a real one: run for hours
holding one giant transaction if it is an **unbounded one-shot `UPDATE`**; exhaust
memory or crawl if it **deserializes every row in Python** instead of set-based
SQL; or trip backend limits and fail partway with no safe restart point if there
is no batching. The recurring fixes move in this ADR's direction: replace Python
row-by-row passes with SQL, split one savepoint-per-DAG monster into bounded
per-unit transactions, batch UUID/type backfills.

A schema change is also never *only* a schema change. A column a model reads must
exist and match the ORM definition, or physical schema and code drift; and for
objects persisted through serialization (serialized Dags and similar), a new field
must be added to `get_serialized_fields()` or the value is silently dropped at the
serialization boundary even though the column exists. Migrations are therefore
authored as one coordinated change across *schema + data + model + serialization*.
(Migration scripts must **not import live ORM models** — a frozen revision pins the
schema as it was, so coordination is at authoring time, not by importing changing
model code.)

## Decision

A revision that migrates data at scale, or that adds a persisted field, is
authored as a coordinated, bounded change:

- **Batch bulk data changes.** No unbounded one-shot `UPDATE`/`DELETE` on a large
  user table. Bound the work (e.g. by primary-key ranges), commit between
  batches, and keep transactions per-unit rather than one transaction across the
  whole table.
- **Prefer set-based SQL over per-row Python.** Express the transformation in SQL
  where possible instead of deserializing every row into Python and writing it
  back.
- **Backfill so existing rows stay valid** — a new required column needs a
  default/backfill, with a matching downgrade.
- **Keep the schema in sync with the ORM model**, and add any new persisted field
  to `get_serialized_fields()` (and custom serializer/deserializer if needed) so
  it round-trips end to end.

## Consequences

- Data migrations complete in bounded time, don't hold table-wide locks for their
  full duration, and can resume after an interruption.
- A field added by a revision is actually usable end to end — column, ORM model,
  and serialize/deserialize all agree. The cost of batching and set-based SQL is
  deliberate given the scale these run at.

A change **violates** this decision when it:

- issues an unbounded one-shot bulk `UPDATE`/`DELETE` against a large user table,
  or holds one transaction across the entire table;
- reshapes data by deserializing every row in Python where set-based SQL would do;
- adds a required column with no backfill/default so upgraded rows are invalid;
- adds or alters a column that does not match the ORM model, or adds a persisted
  model field that is absent from `get_serialized_fields()` so it never survives
  serialization.

A reviewer should reject a data migration that is unbounded, row-by-row where SQL
would serve, or that leaves the schema, model, and serialization out of step.

## Evidence

- #49015 — "batch processing for updating TI UUIDs": a large backfill converted to bounded batches.
- #63591 — "replace savepoint-per-DAG with per-DAG transaction in migration": bounding transaction scope.
- #63628 — "optimize migration 0094 upgrade to use SQL instead of Python deserialization": set-based SQL replacing a per-row deserialize pass.
- #66016 — "migrate existing deadline rows in migration 0080 upgrade and downgrade": a data backfill handled in both directions.
- #62234 — "fix inconsistences between ORM/migration files": the schema-vs-model drift this decision guards against.
