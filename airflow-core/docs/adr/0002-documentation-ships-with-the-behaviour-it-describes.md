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

# 2. Documentation ships with the behaviour it describes, and is judged on accuracy before coverage

Date: 2026-07-20

## Status

Accepted

## Context

In the merged history of this tree, most documentation lands in the same commit as
the change it describes, authored by whoever made the change; the rest is
corrections. Standalone documentation *campaigns*, where someone who did not build a
subsystem explains it, are rare and heavily over-represented in the closed-PR
record. The reason is that Airflow's user-visible behaviour is conditional almost
everywhere — Dag versioning depends on bundle support and whether versioning is
enabled and interacts with clearing, backfill, and pinning; masking of connection
`extra` depends on key names; template rendering coerces types under some settings.
A contributor working from the source alone produces prose that is *locally
plausible and globally wrong*: the path they read, in documentation's confident
register, with none of the conditions attached. That is worse than an absent page,
which sends a reader to the source; a confidently wrong page sends them into
production. The closed-PR record shows the shape repeatedly: a short "documentation
for X" PR, a maintainer who owns X saying it is not correct in the general case or
duplicates the release notes or is really an engineering task, sometimes superseded
by the maintainer's own PR.

One adjacent habit makes it worse: **generated prose that was not read** —
duplicated paragraphs, sections restating each other, code samples longer than the
explanation. This one is mechanisable and stays a rule. Inline code samples are
*not* a violation, though an earlier draft treated them as one: the tree uses inline
`code-block:: python` 405 times against 69 `exampleinclude` directives, a ~6:1
sanctioned majority, and whether an `exampleinclude` "was available" is a fact about
`example_dags/`, not visible in the diff. None of this makes documentation-only
contributions unwelcome — corrections, clarifications of something the contributor
hit, missing links, and tried examples are among the most valuable changes here and
merge readily. The distinction is not doc-only versus code-plus-doc; it is *grounded
in something the author verified* versus *assembled from reading*.

## Decision

Documentation is written from verified behaviour:

- **Behaviour changes carry their documentation in the same PR**, written by
  whoever changed the behaviour. Do not defer the page to a follow-up.
- **Write what you have run.** A documentation-only PR describes behaviour the
  author reproduced, hit in practice, or otherwise verified — not behaviour
  inferred from reading the source.
- **Before documenting a subsystem you do not work on, check with the area that
  owns it** — on the issue or the dev list — that a page is wanted and that the
  intended shape is right. For conditional subsystems, expect that the honest
  page is much longer than the one you planned.
- **Accuracy outranks coverage.** A partial page that is correct about the cases
  it covers, and explicit about the ones it does not, is preferred to a complete
  one that omits the conditions. When in doubt, narrow the claim.
- **Read what you or your tools wrote, end to end, before opening the PR.**
  Duplicated paragraphs are the tell reviewers act on.

Two further preferences are *guidance, not violations*, and live in the area's
`AGENTS.md`: prefer `exampleinclude` from `example_dags/` over an inline
`code-block` for a complete runnable Dag, and prefer linking to the release notes
or an adjacent section over restating them.

## Consequences

- The failure mode becomes "incomplete" rather than "confidently wrong" — the trade
  the project prefers: readers can tell something is missing, not that something is
  untrue.
- Gaps persist where documentation is hardest and most wanted, because the people
  who could write those pages correctly are the small group building the features.
- Willing contributors are turned away from the largest-looking tasks, which are
  exactly the ones needing subsystem knowledge; the area's `AGENTS.md` should keep
  pointing them at corrections and at documenting what they used.
- Examples cost more to add — a real Dag the suite runs — and stop being a source
  of silent breakage.

A change **violates** this decision when it:

- changes user-visible behaviour, a config default, a CLI flag, or an API
  response **to something the existing page does not already describe**, and
  leaves that page untouched. A bugfix that restores behaviour the docs already
  document correctly needs no doc change and does not trip this bullet;
- documents a conditional subsystem in unconditional terms, or describes only
  the code path the author happened to read;
- introduces a new page carrying more than roughly 150 lines of original prose
  about a subsystem, with no linked issue and no dev-list agreement referenced in
  the PR. Whether the author "works on" the subsystem is not observable in a
  diff, so the test is the artefact: a linked issue *satisfies* the agreement
  requirement, as does a dev-list thread. A short page — or one that mainly wires
  in an existing, test-suite-run example via `exampleinclude` — is a small enough
  bet that the review itself is the agreement, and does not trip this bullet;
- contains duplicated or self-restating paragraphs.

Deliberately *not* violations, though an earlier draft listed them: inline
`code-block:: python` where an `exampleinclude` existed (the tree does this 405
times to 69, and "was available" is not visible in a diff), and "prose the author
cannot explain" (unfalsifiable from a diff — the observable version is the
duplicated-paragraph bullet above).

## Evidence

- #61605 (closed) — "Add documentation for Dag versioning behavior", closed because
  the description was not correct in the general case: versioning depends on bundle
  support and whether it is enabled, with interactions the page flattened away.
- #58948 (closed) — a dependencies-documentation update closed as "even more
  inaccurate", superseded by a maintainer's own PR.
- #57325 (closed) — an Airflow-2-migration page closed as too much code, too little
  prose, duplicated with the release notes; the task needs testing and design.
- #61596 (closed) — "fix misleading timetable variable example": the correction was
  right, stalled only on author availability.
- #58539 (closed) — a masking-clarification PR closed for duplicated lines.
- #60494, #62279 (closed) — unreviewed generated changes across many areas at once.
- #58556 (closed) — a connection-masking clarification closed as already covered.
- #55817 (closed) — two contributors updating the same core-concepts pages,
  resolved by coordinating on the task list.
- #53708 (closed) — an incorrect `bash_command` example withdrawn once its author
  saw the snippet needed imports: the case for examples from Dag files the suite runs.
- #60270 (closed) — a bundle-file-permissions config change closed in favour of
  documenting the deployment pattern: the same judgement in the other direction.
- Merged, for the shape this describes: #68373, #69152, #68461, #68887, #68886,
  #69179 — behaviour documented or corrected alongside the feature.
