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

# 4. Metadata-database indexes are a deployment choice, not a schema change

Date: 2026-07-20

## Status

Accepted — a pointer to
[`models/adr/0004`](../../models/adr/0004-deployment-specific-indexes-are-documented-not-shipped.md),
which is authoritative for this decision.

## Context

"Add an index" looks like the cheapest possible migration, and is the single most
frequently *rejected* class of change in this directory — so the decision needs to
be visible from here. But it is a decision about the *schema*, and was previously
written out twice (here and under `models/`) with the same six citations; the two
copies drifted. Rather than keep two near-identical documents in sync, the argument
lives in one place.

**[`models/adr/0004`](../../models/adr/0004-deployment-specific-indexes-are-documented-not-shipped.md)
is authoritative** for what justifies an index on a core table, what evidence a
proposal must carry, and why deployment-specific indexes are documented rather
than shipped. Read it before reviewing an index migration.

## Decision

Apply `models/adr/0004` to any revision under `versions/` that creates an index.
Two consequences are specific to migrations and are recorded here:

- **The index build happens inside the operator's upgrade window.** A revision
  that adds an index to a large, hot table lengthens `airflow db migrate` for every
  installation. State that cost in the PR.
- **An index is far easier to add than to remove.** Removing one is itself a
  released revision, subject to ADR 0001 — which is why the bar for adding one
  through `versions/` is higher than the bar for a code change.
- **The same bar applies to widening or retyping a column for one deployment's
  data** — see ADR 0005; both are "make the schema absorb a local problem".

## Consequences

- The core schema stays lean, and upgrade time does not creep upward one
  well-intentioned index at a time.
- There is one document to change when this decision evolves, and one set of
  citations to keep accurate.

A change **violates** this decision when it does anything in the violates list of
`models/adr/0004`, in a file under `versions/`; plus, specifically here:

- ships an index on a large, hot table without stating the upgrade-time cost.

## Evidence

See `models/adr/0004`. The migration-specific instances are
`0086_3_1_8_add_index_to_task_reschedule_ti_id.py` (#61983) and
`0018_2_10_0_add_indexes_on_dag_id_column_in_referencing_tables.py` — both
index-only revisions that were correct because each names an Airflow call site.
