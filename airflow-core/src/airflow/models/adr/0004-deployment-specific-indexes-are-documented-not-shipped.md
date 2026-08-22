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

A recurring PR looks like an obvious win: a query is slow, `EXPLAIN` shows a seq
scan, so the PR adds a migration creating an index on `dag_run.start_date`,
`job.hostname`, or whatever the local hot query needs. The benefit is real, and the
PR is still closed — an index is not free and its cost is paid by everyone. The
highest-churn tables (`task_instance`, `dag_run`, `job`) maintain every extra index
on every insert in the scheduler's hot path; the index lands during `airflow db
migrate`, extending the upgrade window on huge tables; and it is hard to take back.
The winning query shape is deployment-specific, and Airflow's own queries already
filter on indexed columns. The project built a configurable-metadata-index PR and
closed it (#58814) for documentation only: the *custom metadata indexes* section of
the performance docs, with drop-index-before-upgrade guidance.

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

- Operators with an unusual query profile carry their own indexes across upgrades —
  a burden accepted deliberately, and documented.
- Airflow's schema stays small, so upgrade windows stay predictable and the write
  path stays cheap for the median deployment.
- Genuine core-query index needs still land — with the query that needs them and
  measurements attached; reviewers point at the documented path rather than closing.

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

- #58814 — "Add configurable metadata db indexes": built, reviewed, closed; docs
  landed separately, no schema-mutation tooling.
- #57828 — index on `dag_run.start_date`; closed in favour of the docs-only track.
- #62520 — hostname index on `job`; closed for want of benchmarks and table-size
  impact, query already filtered on indexed columns.
- #62139 — composite index on `dag_run(dag_id, logical_date, run_type)`; sat until
  migrations re-conflicted it, closed unmerged.
- #63166, #62158 — sargable `historical_metrics_data` queries; the sargability half
  was the substance; both closed.
