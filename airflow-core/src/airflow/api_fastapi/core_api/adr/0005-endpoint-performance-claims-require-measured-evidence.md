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
the dashboard's historical metrics — are the most frequently hit code in a running
deployment, so "this endpoint is slow" is one of the most common issues filed
against this area. It attracts a steady stream of PRs whose entire justification
is a performance claim: eager-load these relationships, remove this duplicate
join, make this predicate sargable, swap the JSON serializer, cache the
authorization lookup, add an index.

A large share of them do not merge. Reviewers who take the trouble to benchmark
repeatedly find the claimed win is absent or in the noise, while the change itself
has real costs: an eager load that materialises rows nobody reads, a rewritten
predicate that changes result semantics at the `NULL` edges, a cached
authorization decision that can serve a stale allow, an index that every
deployment then carries. The asymmetry is the problem — a plausible-sounding
optimisation is cheap to write and expensive to disprove, and disproving it is
work a maintainer has to do, on their own hardware, because the author did not.

Two failure shapes recur. The first is the unmeasured claim: a PR description
asserting a reduction in queries or serialization time with nothing behind it —
sometimes with the author explicitly asking a reviewer to run the benchmark for
them because they cannot run Breeze locally. The second is the measured-but-tiny
win: a real benchmark showing a few percent on a laptop with SQLite, which does
not survive contact with a production-shaped dataset and does not pay for the
added complexity or the semantic risk.

**But a benchmark is not what separates the merged from the closed here, and it is
important not to overstate this rule.** Roughly twenty performance changes have
merged in `core_api` and `common` since mid-2025, several justified purely by
structural arithmetic visible in the diff. #57270 removed an N+1 on the Dag-list
tags query on the strength of a query count going from 101 to 2, with no latency
figures at all. #62910 eliminated duplicate JOINs in `get_task_instances` by
showing the before and after SQL — 7 JOINs down to 5 — again with no benchmark,
and #62482 narrowed eager loading with `load_only()` on the same basis. The
decisive comparison is #62910 against #62108: the *same* optimisation of the
*same* endpoint, one merged and one closed, and **neither carried a benchmark**.
What separated them was that #62910 showed its mechanism concretely in the diff
while #62108 asked a reviewer to establish its premise.

The distinction the evidence actually supports is therefore about *what is being
traded*, not about whether a number is present. A structural fix — removing a
duplicate join, collapsing an N+1, narrowing a column list — changes the shape of
the query without changing what it means; its mechanism is legible in the diff and
a query-count regression test pins it. A change that trades **semantics or schema**
for speed — rewriting a predicate to be sargable, adding an index, caching an
authorization lookup, swapping a serializer — is a change whose correctness and
whose payoff are both unproven by inspection, and only a measurement can settle
whether it was worth taking on that risk.

Note also that this ADR must compose with
[ADR-3](0003-api-server-is-the-sole-mediator-of-client-database-access.md),
which lists *introducing* an N+1 as a violation. If removing one required a
production-grade benchmark first, the two ADRs would trap a contributor between
them.

## Decision

**A performance change that trades semantics or schema for speed carries a
measurement. A structural query-count fix carries a test.**

Which one a change is determines what it owes:

- **Structural fixes owe a regression test, not a benchmark.** Removing a
  duplicate join, collapsing an N+1, replacing a per-row lookup with a batched one,
  narrowing a `load_only()` column list — these change the shape of the query
  without changing which rows it returns or what they mean. Show the mechanism in
  the PR: the before/after SQL, the before/after query count, or the
  `joinedload`/`contains_eager` that replaced the loop. Pin it with a query-count
  assertion so the fix cannot silently regress. That is the bar #57270, #62910 and
  #62482 met, and it is sufficient.
- **Semantic and schema trades owe before/after numbers.** Rewriting a predicate
  for sargability, adding an index, caching a lookup, or swapping the serializer
  changes something other than query shape — result semantics at the edges,
  every deployment's schema, or staleness. State which endpoint, which query, what
  row counts, which backend, and how it was measured.
- **Measure on a realistic dataset and a realistic backend.** A few thousand rows
  on SQLite is not evidence for an endpoint whose problem appears at a
  deployment's data volume on PostgreSQL or MySQL.
- **The author establishes the PR's own premise.** Asking a reviewer to run the
  benchmark, or to work out from scratch whether the change does anything, is what
  sank #62108 while the equivalent #62910 merged. Whether the premise is
  established by a number or by legible arithmetic, the author supplies it.
- **Show that behaviour is unchanged**, in particular at the edges the rewrite
  moves: `NULL` handling, ordering stability, pagination boundaries, and the rows
  the permitted-filter scope allows. This applies to both kinds — a structural fix
  that quietly drops rows is not structural.
- **Never cache an authorization decision as a performance fix.** A cached allow
  is a security change, and it goes through the auth-manager and security process,
  not an endpoint-optimisation PR — no benchmark makes this one acceptable.
- **Schema-level fixes go through the general mechanism**, not this endpoint. A
  new metadata-database index is a schema decision for every deployment, not a
  side effect of tuning one route.

## Consequences

- Optimisations that land are known to work, and the reviewer's time is spent on
  the change rather than on establishing whether it does anything.
- Real wins take longer to contribute: a contributor without a Breeze environment
  and a realistic dataset cannot land one here, and that is the intended bar.
- Some genuine improvements are refused for want of evidence, and have to be
  re-submitted with it.
- The project keeps the option of solving these problems structurally — a
  configurable-index mechanism, a shared batching helper — instead of accumulating
  per-endpoint micro-tuning that each deployment pays for.

A change **violates** this decision when it:

- rewrites a predicate, adds an index, caches a lookup, or swaps a serializer for
  speed with no before/after measurement in the PR;
- lands a structural query-shape fix (duplicate join, N+1, per-row lookup) without
  either the before/after SQL or query count in the PR **and** a query-count
  assertion in the tests;
- benchmarks only on SQLite / a toy dataset for an endpoint whose problem is data
  volume;
- asks a reviewer or maintainer to establish the PR's own premise — by running the
  benchmark, or by deriving the mechanism the PR did not state;
- changes selection, join, ordering, or serialization behaviour without showing
  the result set and its `NULL`/ordering/pagination edges are unchanged;
- caches or memoises an authorization lookup for speed;
- adds a metadata-database index as part of an endpoint-tuning change.

## Evidence

**Merged, on structural evidence alone — the accepted path:**

- #57270 — "Fix n+1 query to fetch tags in the dags list page": merged on a query
  count going from 101 to 2. No latency numbers anywhere in the PR.
- #62910 — "eliminate duplicate JOINs in `get_task_instances`": merged on
  before/after SQL showing 7 JOINs reduced to 5, with the `joinedload`-versus-
  `contains_eager` mechanism spelled out. No benchmark.
- #62482 — `load_only()` in `get_dag_runs` eager loading: merged on the same basis.

**Closed — semantic or serializer trades without numbers:**

- #59149 — `orjson` response serialization: a reviewer benchmarked it with a load
  tool against a warmed-up API server and measured at most ~5% on average latency,
  sometimes none, with no change in requests per second; closed.
- #62108 — "Optimize TaskInstance list query by eliminating duplicate joins": the
  **same optimisation of the same endpoint as #62910**, which merged. Neither
  carried a benchmark, so measurement is demonstrably not what separated them: the
  author, on a machine that could not run Breeze, asked a reviewer to run the
  benchmark that would have established the PR's premise, rather than showing the
  duplicate JOINs in the diff as #62910 did.
- #62152, #62158 and #63166 — three separate sargable/index rewrites of the
  historical-metrics endpoint: a reviewer reported seeing no measurable improvement
  from any of them and asked for benchmarking guidance; none landed.
- #62160 — eager loading to avoid an N+1 on Dag-run versions: went stale without
  the measurement that would have justified it.
- #64928 — eager loading of task-instance relationships plus grid response
  batching: closed without demonstrated benefit.
- #61569 and #61572 — caching the Dag team lookup inside `requires_access_dag`:
  closed; caching an authorization decision for speed is not an endpoint
  optimisation.
- #57828 — an index on `dag_run.start_date`: closed in favour of a general
  configurable-metadata-database-index mechanism rather than a one-off schema
  addition driven by a single query.
