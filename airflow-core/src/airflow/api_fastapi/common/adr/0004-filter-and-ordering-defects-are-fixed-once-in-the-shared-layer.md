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

# 4. A filter or ordering defect is fixed once, here, by one PR

Date: 2026-07-20

## Status

Accepted

## Context

A defect in this layer presents to users as a defect in an *endpoint*. Sorting a
task list by a column that contains `NULL`s crashes `/dags/{dag_id}/tasks`; an
owners filter that matches substrings returns the wrong Dags on the Dag list. So
the bug gets filed against the endpoint, and contributors fix it where they found
it — in the route, or in a copy of the predicate — while the actual cause is a
shared param class that every other endpoint also mounts.

The second, sharper effect is duplication. Because these bugs are user-visible on
popular endpoints and look small, several contributors start on the same one
within days of each other. One `order_by`-with-`NULL`s defect produced four
concurrent PRs; a reviewer closed one explicitly in favour of the three that were
going in the right direction, and only one of those could land. That is three
authors' work discarded and four reviews' worth of maintainer attention spent on
one fix — the most expensive way this area consumes review capacity, and it is
entirely avoidable by looking first.

Both effects have the same root: the fix belongs to the shared layer, but the
symptom belongs to a route, and nothing in the symptom points the contributor at
the shared home or at the work already in flight there.

## Decision

Filter, sort, and pagination defects are handled as shared-layer changes:

- **Search for in-flight work before writing code.** These defects attract
  concurrent PRs; check open PRs and the issue's cross-references first, and
  prefer reviewing or building on the existing one to opening a near-identical
  parallel PR.
- **Fix the shared class, not the route.** If the symptom would also occur on a
  sibling endpoint that mounts the same param, the change belongs in
  `parameters.py` / `cursors.py` / `db/common.py`, not in the handler that
  reported it.
- **State the matching semantics the change establishes.** Exact, prefix,
  substring, case sensitivity, and wildcard handling are the contract of a filter;
  a fix that changes which rows match is a behaviour change for every endpoint
  mounting it, and must say so.
- **Cover the `NULL` and empty cases explicitly.** A `NULL` in a sort or filter
  column must have a defined position and must not raise; the test carries the
  `NULL` row and the empty result.
- **A new filter is a shared class**, added once with its param name, alias, and
  description, rather than a route-local variant of an existing one.

## Consequences

- One fix covers every endpoint mounting the param, including those with no bug
  report.
- The behaviour that changes is stated, so a reviewer can weigh it as the
  contract change it is instead of reading it as a bug fix.
- Contributors face a higher bar than the symptom suggested: understanding the
  shared class and every endpoint that mounts it.
- Some parallel work is still unavoidable when two people start within hours; the
  rule reduces it rather than eliminating it, and the later PR is expected to
  yield gracefully.

A change **violates** this decision when it:

- fixes a filter/sort/pagination defect inside a route handler when the same
  defect exists on sibling endpoints mounting the same shared param;
- duplicates an open PR fixing the same shared defect, with no reference to it
  and no reason for a different approach;
- changes which rows a filter matches without stating the new matching semantics;
- changes the *behaviour* of sort or filter handling — which rows match, what
  order they come back in, how `NULL`s sort — without a test covering the `NULL`
  and empty-result cases. A docstring fix, type annotation, parameter rename, or
  any other diff here that cannot change a result set does not trigger this;
- adds a route-local variant of a filter that already exists as a shared class.

## Evidence

- #64248 — `NULL` values in a task-list `order_by`: closed in favour of #64163,
  #64075 and #63991, three further concurrent PRs for the same defect; four
  authors converged on one shared-layer bug.
- #66775 — Dag-owners filter matching unintended substrings: the defect is in the
  matching semantics of the shared filter rather than in the endpoint that
  surfaced it.
- #68785 — a Dag-tag filter added from the endpoint side, which stalled on not
  being able to exercise the shared pagination/count path in tests.
- #63801 — normalising error response formats across a whole API surface: the
  author closed it because it had accumulated unrelated changes, and reviewers
  noted a format change on a shared surface needs a version migration rather than
  a direct edit.
