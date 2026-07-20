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
`pyproject.toml`, a dependency set every consumer inherits, symlinks into each
consumer, prek hooks that police its structure, and — once released — consumers
that may pin different versions of it. Undoing that is not a revert; it is a
migration across several distributions.

The expensive part of such a PR is therefore not the code motion, which is
mostly mechanical, but the boundary: what exactly is "shared" here, what stays
behind, and how the new library interacts with code that is neither core nor
Task SDK — providers in particular, which have their own configuration and their
own release cadence. That question cannot be answered by reading a diff that has
already moved two thousand lines. It has to be answered first.

This is what happened to the configuration library. The extraction was proposed
as a PR, review immediately turned to scope — what counts as shared, and how the
provider side of configuration fits — and the PR was closed in favour of a
successor built against the agreed scope. The work was not wasted, but a large
diff was reviewed twice and thrown away once, which is the specific outcome this
decision exists to avoid.

The extractions that went smoothly went the other way round. The secrets-backend
move landed as a sequence: move the base class out to the shared library, then
remove the `Connection` dependency, then remove the remaining core references on
the SDK side — each step small, each with a clear boundary, and the client/server
separation they were serving was already agreed. The `module_loading` extraction
was similarly narrow: one well-understood utility, no ambiguity about what
belonged in it.

The same discipline applies at smaller scale to adding surface. A new hook, a
new class, a new metric in a shared library is inherited by every consumer and
is hard to withdraw. Reviewers ask what the existing surface cannot express
before considering an addition — a proposed listener hook was closed once its
author established the case was reachable through an existing hook with zero new
surface, and a proposed scheduler gauge was withdrawn after comparison with how
comparable systems derive the same signal from state they already have.

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

The libraries stay small and their boundaries stay defensible, and the
cross-distribution surface grows slowly enough that cross-version compatibility
remains tractable.

The cost falls on anyone with a genuinely good extraction to propose: they must
argue for it before writing it, and the work arrives later than it would have.
Contributors reasonably experience this as friction, and the incremental-landing
rule makes a single conceptual change into four or five PRs, each needing its
own review round. For a contributor without standing in the area, that is a long
road for a refactor with no user-visible payoff.

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

- #54943 — shared configuration library proposed directly as a PR; review turned
  on the scope of "shared" and its interaction with provider configuration, and
  the PR was closed in favour of #57744.
- #57744 — the successor, built against the agreed scope.
- #58621, #61523, #59597 — the secrets-backend extraction landed as separate,
  narrow steps: move the base class to the shared library, remove the
  `Connection` dependency, then remove core references on the SDK side.
- #59139 — `module_loading` extracted as a single, unambiguous utility.
- #66410 — a proposed listener hook closed once the case was shown to be
  reachable through the existing hook with no new surface.
- #69086 — a proposed scheduler gauge withdrawn after comparison with prior art
  that derives the same signal from existing state.
