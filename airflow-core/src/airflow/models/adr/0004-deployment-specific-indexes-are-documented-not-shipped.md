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

# 4. Deployment-specific metadata-DB indexes are documented, not shipped

Date: 2026-07-20

## Status

Accepted — authoritative for this decision.
[`migrations/adr/0004`](../../migrations/adr/0004-metadata-database-indexes-are-a-deployment-choice-not-a-schema-change.md)
points here and adds only the migration-specific consequences (upgrade window,
irreversibility under migrations ADR 0001).

## Context

A recurring class of PR arrives here shaped like an obvious win: a query is slow
on the author's cluster, `EXPLAIN` shows a sequential scan, so the PR adds an
Alembic migration creating an index on `dag_run.start_date`, `job.hostname`,
`(dag_id, logical_date, run_type)`, or whatever the local hot query needs. The
change is small, the local benefit is real, and the PR is nonetheless closed.

The reason is that an index is not free and its cost is paid by everyone.

- **Write amplification is borne by every deployment.** `task_instance`,
  `dag_run`, and `job` are the highest-churn tables in the metadata database.
  Every additional index is maintained on every insert and update in the
  scheduler's hot path, in exchange for speeding up one query shape that only
  some installations run.
- **The index lands during `airflow db migrate`.** Creating an index on a table
  with hundreds of millions of rows extends the upgrade window on a live
  production database — a cost operators cannot opt out of and often cannot
  afford.
- **The winning query shape is deployment-specific.** Cardinality decides
  whether an index helps: on one cluster `job.hostname` has thousands of
  distinct values, on another a handful. Airflow's own queries already filter on
  indexed columns; the scans being reported are usually driven by a particular
  UI filter, a custom dashboard, or an installation's row distribution.
- **Indexes are hard to take back.** Once shipped in a migration, an index is in
  every user's schema; removing it later is another migration and another
  upgrade window.

The project considered shipping tooling for this and deliberately declined. A PR
adding configurable metadata-database indexes was built, reviewed, and closed in
favour of documentation only: the conclusion was that Airflow should not
facilitate arbitrary metadata-schema mutation, but should tell power users how to
do it themselves. What shipped is the *custom metadata indexes* section of the
performance documentation, including the drop-index-before-upgrade guidance.

## Decision

- **Do not add an index to a core table for a query shape that is not Airflow's
  own hot path.** The remedy for a deployment-specific slow query is the
  documented custom-metadata-index procedure, applied by the operator.
- **An index PR must carry evidence, not a plan.** Nobody expects a contributor
  to have a production cluster, so the floor is one that anyone can reach: seed
  the affected table to a stated row count using a Breeze-generated dataset
  (`breeze testing` against a generated Dag set, or a direct bulk insert),
  then attach `EXPLAIN ANALYZE` for the query before and after the index, the row
  count used, and the resulting index size. "`EXPLAIN` shows a seq scan" is an
  observation, not a justification. Production timings are welcome when the author
  has them; their absence is not what sinks a proposal.
- **Prefer making the query sargable or set-based over adding an index.** A
  filter that cannot use an existing index because it wraps a column in a
  function, or a loop issuing one query per row, is a code defect — fix that
  first and re-measure.
- **If Airflow's own code genuinely needs the index, ship it with the query that
  needs it**, in the same PR, so the reviewer can weigh the two together.
- **Airflow does not ship tooling to add, drop, or configure metadata indexes.**
  Proposals to make the index set configurable are a settled question; reopening
  it needs a devlist discussion, not a PR.

## Consequences

- Operators with an unusual query profile carry their own indexes and must
  re-apply them across upgrades. That burden is accepted deliberately, and the
  documentation exists to support it.
- Airflow's schema stays small enough that upgrade windows remain predictable
  and the write path stays cheap for the median deployment.
- Genuine core-query index needs still land — but as part of the change that
  introduces or fixes the query, with measurements attached.
- Contributors who hit a real slowdown get told "no" on a change that helped
  them. The compensating path is documented and supported, and reviewers should
  point at it rather than simply closing.

The decision is about indexes that serve **a deployment's own** queries. An index
that serves a query **Airflow itself issues** is a different thing and is in scope
for core: migration `0086_3_1_8_add_index_to_task_reschedule_ti_id.py` shipped in
3.1.8 via #61983 ("Fix scheduler heartbeat misses caused by slow reschedule
dependency check") — index-only, with a named call site and no formal benchmark —
and `0018_2_10_0_add_indexes_on_dag_id_column_in_referencing_tables.py` is an
earlier instance. Neither would survive the bullets below read literally, and both
were correct. Name the Airflow call site and the bullets do not apply.

A change **violates** this decision when it:

- adds an Alembic migration whose only content is `create_index` on a core table
  (`dag_run`, `task_instance`, `job`, `log`, `xcom`, …) **to serve a query Airflow
  does not itself issue** — an index backing a named Airflow call site is in scope
  for core and is not a violation;
- justifies an index for a query Airflow does not itself issue with a query plan,
  a screenshot, or a description of one installation's symptoms but no
  before/after `EXPLAIN ANALYZE` at a stated row count (an index backing a named
  Airflow call site needs the call site, not a benchmark — production index-usage
  statistics or an observed stall are stronger evidence than a synthetic run, not
  weaker);
- adds an index for a query Airflow does not itself issue without accounting for
  the write-path and upgrade-window cost, or without stating the affected table's
  size;
- reintroduces configurable / user-supplied metadata indexes as an Airflow
  feature;
- adds an index to work around a non-sargable filter or an N+1 loop instead of
  fixing the query;
- ships an index "for a future query" that no code in the PR issues.

A reviewer should ask two questions before anything else: *which Airflow query
uses this index*, and *what did it cost to maintain it*. If the first answer is
"none — it is for my dashboard", the answer is the custom-index documentation.

## Evidence

- #58814 — "Add configurable metadata db indexes": built and reviewed, then
  closed with the explicit conclusion that Airflow will not provide tooling to
  mutate the metadata schema; the documentation-only part landed separately.
- #57828 — index on `dag_run.start_date` for query optimization; closed in
  favour of the configurable-index track, which itself then became docs only.
- #62520 — "Perf: add hostname index on job table": reviewers asked for query-time
  benchmarks and table-size impact, noted the query already filters on indexed
  columns, and pointed at the custom-metadata-indexes documentation. Closed
  without the measurements.
- #62139 — composite index on `dag_run(dag_id, logical_date, run_type)`: sat long
  enough that every newly-merged migration re-conflicted it, and closed without
  landing.
- #63166, #62158 — "optimize `historical_metrics_data` with indexes and sargable
  queries": the sargability half was the substantive part; both were drafted back
  and closed.
