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

# 1. Selective-checks logic, documentation and tests change together

Date: 2026-07-20

## Status

Accepted

## Context

`dev/breeze/src/airflow_breeze/utils/selective_checks.py` decides, for every pull
request, which CI jobs run: test types, Python and backend versions, provider matrices,
which prek hooks are skipped, and whether the full matrix is forced. Its failure mode is
unlike ordinary code: when a rule is wrong in the *restrictive* direction, nothing fails
— a suite simply does not run, the PR goes green, and the regression lands with no stack
trace, surfacing days later on another branch or at release time.

That makes the *diff* an inadequate review surface. A one-line condition is only
reviewable against a stated model of what CI should do. Two artifacts carry that model:
`dev/breeze/doc/ci/04_selective_checks.md` (decision rules, file groups, outputs, worked
examples) and `dev/breeze/tests/test_selective_checks.py` (pins the classification of
concrete file sets). When either drifts from the code, a reviewer can no longer
distinguish an intended optimisation from an accidental hole in coverage. The
repository's `CLAUDE.md` states this as a coding standard; this ADR records it as a
binding architectural decision, together with the conservatism rule: heuristics that
cannot tell whether a change is risky must run *more*, not less.

## Decision

- Any change to the selective-checks rules updates
  `dev/breeze/doc/ci/04_selective_checks.md` in the **same pull request** — the
  decision-rules list, the diagrams, the outputs table, and the worked examples
  as applicable.
- Any change to file groups, to what forces `full_tests_needed` /
  `all_versions`, to provider or test-type selection, or to which prek hooks are
  skipped, adds or adjusts cases in
  `dev/breeze/tests/test_selective_checks.py`.
- A change that makes CI run **less** states, in the pull request, what is no
  longer covered and why that is safe. Reducing coverage is a legitimate and
  frequent goal here, but it is an argument to be made, not a cleanup.
- When a heuristic cannot determine whether a change is risky, it escalates to
  running more. A heuristic that has proven unreliable is removed rather than
  tuned in place.
- Release-branch (`v3-X-test`) behaviour is a separate decision from `main`
  behaviour; branch defaults live in
  `dev/breeze/src/airflow_breeze/branch_defaults.py` and are not altered as a
  side effect of a `main`-targeted rule.

## Consequences

CI behaviour stays explainable: a reviewer reads the documented rules, checks them against
the test cases, and judges whether the code implements them, and skips accumulate
deliberately rather than by accretion. The cost is that every selective-checks PR is a
three-file change at minimum — friction that is the point, since the alternative is a
silent loss of coverage nobody detects.

A change *violates* this decision when it:

- modifies the selective-checks rules without touching
  `dev/breeze/doc/ci/04_selective_checks.md`, or leaves the documented rules,
  outputs table or worked examples describing the previous behaviour;
- adds, renames or removes a file group, or changes test-type / provider
  selection or prek-hook skipping, without a corresponding case in
  `dev/breeze/tests/test_selective_checks.py`;
- narrows what CI runs without naming, in the pull request, the coverage that is
  being given up;
- resolves an ambiguous classification by running fewer jobs, or adds a
  heuristic that guesses at risk from proxies such as diff size;
- changes release-branch matrix behaviour as an incidental effect of a rule
  aimed at `main`.

## Evidence

- #68116 — documented the selective-checks algorithm, establishing
  `04_selective_checks.md` as the human-readable model of the rules.
- #69861 — fixed the documented breeze selective-checks command after the doc drifted.
- #68109 — removed the large-PR heuristic rather than keep tuning a proxy that guessed
  wrong about risk.
- #68120 — reverted #68057 after a release-branch full-matrix change misbehaved on
  `v3-X-test`.
- #68802 — narrowed forced full CI for non-test workflow and prek-only changes, landing
  logic, doc and tests together.
- #69519 — made `area:kubernetes-tests` force the Kubernetes job, with logic, doc and
  tests in one change.
- #69674, #70021 — doc-only skips for Java SDK and Go SDK jobs, each with matching test
  cases.
