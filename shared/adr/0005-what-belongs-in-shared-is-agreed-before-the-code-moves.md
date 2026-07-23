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

# 5. What belongs in a shared library is agreed before the code moves

Date: 2026-07-20

## Status

Accepted

## Context

Creating a shared library is close to irreversible. The moment
`apache-airflow-shared-<name>` exists it acquires a distribution, a
`pyproject.toml`, a dependency set every consumer inherits, symlinks, prek hooks,
and — once released — consumers that may pin different versions. Undoing that is a
migration across distributions, not a revert. The expensive part of such a PR is
therefore not the (mostly mechanical) code motion but the boundary: what exactly is
"shared", what stays behind, and how the library interacts with providers, which
have their own config and cadence. That must be answered first, not from a diff
that already moved two thousand lines.

The configuration library (#54943) shows the cost: proposed directly as a PR,
review turned to scope, and it was closed for a successor (#57744) built against
the agreed scope — a large diff reviewed twice and thrown away once. The smooth
extractions went the other way: the secrets-backend move landed as narrow steps,
and `module_loading` was one unambiguous utility. The same discipline applies to
*adding surface* — a new hook, class, or metric is inherited by every consumer and
is hard to withdraw, so reviewers ask what the existing surface cannot express
first (a listener hook closed once shown reachable through an existing hook; a
scheduler gauge withdrawn after comparison with prior art).

## Decision

The boundary is agreed before the code moves:

- **Creating a new shared library, or moving a substantial body of code into an
  existing one, starts with an issue or dev-list discussion** that states what
  belongs in the library, what stays behind, and how providers interact with it.
  The PR follows the agreement.
- **Extractions land incrementally.** Move the surface, then break the
  dependencies it dragged along, then clean up the consumers — in separate,
  individually reviewable steps rather than one large diff.
- **New public surface in a shared library must be justified against the
  existing surface.** State what the current API cannot express; a solution that
  adds no new surface is preferred to one that adds a little.
- **The dependency cost is part of the proposal.** A new third-party dependency
  in a shared library is paid by every consumer that links it, including those
  that never use the new code.

## Consequences

The libraries stay small and their boundaries defensible, and the
cross-distribution surface grows slowly enough that cross-version compatibility
stays tractable. The cost falls on anyone with a genuinely good extraction to
propose: they must argue for it before writing it, and the incremental-landing
rule turns one conceptual change into four or five PRs — a long road for a
contributor without standing in the area.

A change **violates** this decision when it:

- creates a new `shared/<name>` distribution without a prior issue or dev-list
  thread agreeing its scope;
- moves a large body of code into `shared/` in one diff, with the boundary
  question unresolved in the PR description;
- adds a public class, hook, or metric to a shared library *where an equivalent
  surface demonstrably already exists* — the reviewer must be able to name the
  existing class, hook, or metric it duplicates. Whether the PR body happens to
  contain a sentence arguing the gap is not the test: the shared tree carries a
  steady stream of well-formed metric and hook additions, and firing on the ones
  whose authors did not write that sentence penalises prose, not design. When the
  boundary is genuinely unclear, ask the author what the existing surface cannot
  do — that is a review question, not a violation;
- introduces a new third-party dependency into a shared library without
  accounting for the consumers that will inherit it.

## Evidence

- #54943 — shared configuration library proposed directly as a PR; closed on scope
  in favour of #57744.
- #57744 — the successor, built against the agreed scope.
- #58621, #61523, #59597 — secrets-backend extraction as separate narrow steps:
  move the base class, remove the `Connection` dependency, remove core references
  on the SDK side.
- #59139 — `module_loading` extracted as a single, unambiguous utility.
- #66410 — a listener hook closed once shown reachable through an existing hook
  with no new surface.
- #69086 — a scheduler gauge withdrawn after comparison with prior art deriving the
  signal from existing state.
