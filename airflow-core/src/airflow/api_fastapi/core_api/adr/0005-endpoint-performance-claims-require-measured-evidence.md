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

# 5. Endpoint performance changes require measured evidence, not a plausible story

Date: 2026-07-20

## Status

Accepted

## Context

The list and summary endpoints here — Dag runs, task instances, XComs, grid data,
historical metrics — are the most frequently hit code in a running deployment, so
"this endpoint is slow" attracts a steady stream of performance-claim PRs: eager-load
these relationships, remove this join, make this predicate sargable, swap the
serializer, cache the authorization lookup, add an index. A large share do not merge.
The asymmetry is the problem: a plausible optimisation is cheap to write and expensive
to disprove, and disproving it is work a maintainer does on their own hardware because
the author did not. Two failure shapes recur — the unmeasured claim (a query/serialization
reduction asserted with nothing behind it, sometimes asking the reviewer to run the
benchmark) and the measured-but-tiny win (a few percent on a laptop with SQLite that
does not survive a production-shaped dataset).

**But a benchmark is not what separates merged from closed, and it is important not to
overstate this rule.** Roughly twenty performance changes have merged in `core_api` and
`common` since mid-2025, several justified purely by structural arithmetic in the diff.
PR #57270 removed a Dag-list tags N+1 on a query count going 101 → 2 with no latency
figures; #62910 eliminated duplicate JOINs in `get_task_instances` by showing 7 JOINs
down to 5, again with no benchmark; #62482 narrowed eager loading with `load_only()` on
the same basis. The decisive comparison is #62910 against #62108: the *same* optimisation of the
*same* endpoint, one merged and one closed, and **neither carried a benchmark**. What
separated them was that #62910 showed its mechanism concretely in the diff while #62108
asked a reviewer to establish its premise.

The distinction the evidence supports is about *what is being traded*, not whether a
number is present. A **structural** fix — removing a duplicate join, collapsing an N+1,
narrowing a column list — changes the query's shape without changing what it means; its
mechanism is legible in the diff and a query-count regression test pins it. A change that
trades **semantics or schema** for speed — a sargable-predicate rewrite, an index, a
cached authorization lookup, a swapped serializer — has both correctness and payoff that
inspection cannot settle, so only a measurement can.

This ADR composes with
[ADR-3](0003-api-server-is-the-sole-mediator-of-client-database-access.md), which lists
*introducing* an N+1 as a violation. If *removing* one required a production-grade
benchmark first, the two ADRs would trap a contributor between them.

## Decision

**A performance change that trades semantics or schema for speed carries a measurement.
A structural query-count fix carries a test.** Which one a change is determines what it
owes:

- **Structural fixes owe a regression test, not a benchmark.** Removing a duplicate join,
  collapsing an N+1, replacing a per-row lookup with a batched one, narrowing a
  `load_only()` column list — these change the query's shape without changing which rows
  it returns or what they mean. Show the mechanism in the PR (before/after SQL, before/after
  query count, or the `joinedload`/`contains_eager` that replaced the loop) and pin it with
  a query-count assertion so the fix cannot silently regress. That is the bar #57270,
  #62910 and #62482 met, and it is sufficient.
- **Semantic and schema trades owe before/after numbers.** A sargable-predicate rewrite,
  an index, a cached lookup, or a swapped serializer changes something other than query
  shape — result semantics at the edges, every deployment's schema, or staleness. State
  which endpoint, which query, what row counts, which backend, and how it was measured.
- **Measure on a realistic dataset and backend.** A few thousand rows on SQLite is not
  evidence for an endpoint whose problem appears at deployment data volume on PostgreSQL
  or MySQL.
- **The author establishes the PR's own premise.** Asking a reviewer to run the benchmark,
  or to work out whether the change does anything, is what sank #62108 while the equivalent
  #62910 merged. Whether by a number or by legible arithmetic, the author supplies it.
- **Show that behaviour is unchanged**, in particular at the edges the rewrite moves:
  `NULL` handling, ordering stability, pagination boundaries, and the rows the
  permitted-filter scope allows. This applies to both kinds — a structural fix that quietly
  drops rows is not structural.
- **Never cache an authorization decision as a performance fix.** A cached allow is a
  security change, and it goes through the auth-manager and security process, not an
  endpoint-optimisation PR — no benchmark makes this one acceptable.
- **Schema-level fixes go through the general mechanism**, not this endpoint. A new
  metadata-database index is a schema decision for every deployment, not a side effect of
  tuning one route.

## Consequences

- Optimisations that land are known to work, and the reviewer's time goes to the change
  rather than to establishing whether it does anything.
- Real wins take longer: a contributor without a Breeze environment and a realistic
  dataset cannot land one here, and that is the intended bar. Some genuine improvements are
  refused for want of evidence and have to be re-submitted with it.
- The project keeps the option of solving these problems structurally — a
  configurable-index mechanism, a shared batching helper — instead of accumulating
  per-endpoint micro-tuning every deployment pays for.

A change **violates** this decision when it:

- rewrites a predicate, adds an index, caches a lookup, or swaps a serializer for speed
  with no before/after measurement in the PR;
- lands a structural query-shape fix (duplicate join, N+1, per-row lookup) without either
  the before/after SQL or query count in the PR **and** a query-count assertion in the
  tests;
- benchmarks only on SQLite / a toy dataset for an endpoint whose problem is data volume;
- asks a reviewer or maintainer to establish the PR's own premise — by running the
  benchmark, or by deriving the mechanism the PR did not state;
- changes selection, join, ordering, or serialization behaviour without showing the result
  set and its `NULL`/ordering/pagination edges are unchanged;
- caches or memoises an authorization lookup for speed;
- adds a metadata-database index as part of an endpoint-tuning change.

## Evidence

**Merged, on structural evidence alone — the accepted path:**

- #57270 — Dag-list tags N+1; merged on a query count 101 → 2, no latency numbers.
- #62910 — duplicate JOINs in `get_task_instances`; merged on before/after SQL (7 → 5 JOINs)
  with the `joinedload`-vs-`contains_eager` mechanism spelled out. No benchmark.
- #62482 — `load_only()` in `get_dag_runs` eager loading; merged on the same basis.

**Closed — semantic or serializer trades without numbers:**

- #59149 — `orjson` serialization; a reviewer measured ~5% at most, sometimes none; closed.
- #62108 — the **same optimisation of the same endpoint as #62910** (which merged); neither
  had a benchmark, so measurement is not what separated them — the author asked a reviewer
  to run the benchmark instead of showing the duplicate JOINs in the diff.
- #62152, #62158, #63166 — three sargable/index rewrites of the historical-metrics endpoint;
  a reviewer saw no measurable improvement; none landed.
- #62160 — eager loading against a Dag-run-versions N+1; went stale without measurement.
- #64928 — task-instance eager loading plus grid batching; closed without demonstrated benefit.
- #61569, #61572 — caching the Dag team lookup in `requires_access_dag`; closed — caching an
  authorization decision is not an endpoint optimisation.
- #57828 — index on `dag_run.start_date`; closed for a general configurable-index mechanism.
