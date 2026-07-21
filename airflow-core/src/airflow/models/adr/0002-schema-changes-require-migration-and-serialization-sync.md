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

# 2. Schema changes require a migration in an unreleased version and stay in sync with serialization

Date: 2026-07-18

## Status

Accepted

## Context

A SQLAlchemy column on a model in `airflow-core/src/airflow/models/` is not a
self-contained edit. It ripples three directions at once: **the physical schema**
(installations upgrade in place, so every add/drop/alter needs an Alembic
migration that runs on SQLite, MySQL, and PostgreSQL); **existing rows** (an
upgrade must leave old rows valid, so a new non-nullable or required column needs a
backfill/default); and **serialization** (serialized Dags and objects must
round-trip the new field, or a value written by one component is silently lost when
read by another). Released migrations are immutable history — a user who already
ran revision *X* never re-runs it, so rewriting *X* diverges the two database
populations permanently.

## Decision

A new or changed column ships with all of:

- **A migration in the latest UNRELEASED version's migration file** — never edit
  or rewrite a migration that has shipped in a released version. Add a new
  revision on top instead.
- **A backfill / server default** so existing rows are valid after upgrade, and
  a matching downgrade.
- **SQLite-safe operations** — use `with op.batch_alter_table(...)` for
  alter/drop/constraint changes, since SQLite cannot `ALTER` in place.
- **The field added to the model's `get_serialized_fields()`** (and the
  serializer/deserializer if it needs custom handling) so it round-trips.

## Consequences

- In-place upgrades stay reliable across all three backends and never strand old
  rows or diverge database populations.
- A field added to the model is actually persisted end to end — written,
  serialized, and read back — rather than dropping out at the serialization
  boundary.

A **violating** change looks like: adding or altering a column with no migration
(the ORM model and the physical schema drift apart); editing a migration that
has already been released instead of adding a new revision; a bare `op.add_column`
/ `op.alter_column` outside `batch_alter_table` that breaks on SQLite; a
required column with no backfill so upgraded rows are invalid; or a new model
field absent from `get_serialized_fields()` so its value never survives a
serialize/deserialize cycle.

## Evidence

- #61550 — Add the option to select bundle version on dag run trigger endpoint: a persisted field that must migrate and serialize together.
- #64522 — Add a way to mark a return value XCom as dag result: new state that has to round-trip through serialization.
- #69311 — Fix asset event ingestion crash for Dags using FixedKeyMapper: a serialization-round-trip defect at the model boundary.
