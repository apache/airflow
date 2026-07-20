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

Read the merged history of this tree and one pattern dominates: most
documentation lands in the same commit as the change it describes, authored by
whoever made the change. The rest is corrections — a method name that was
renamed, a callback count that went from six to five, a config option removed
from the reference, a timetable example that said the wrong thing about
`last_automated_data_interval`. Standalone documentation *campaigns*, where
someone who did not build a subsystem sits down to explain it, are rare, and in
the closed-PR record they are heavily over-represented.

There is a reason for that asymmetry. Airflow's user-visible behaviour is
conditional almost everywhere. Dag versioning behaves differently depending on
whether the bundle supports versioning and whether versioning is enabled at all,
and interacts with clearing, backfill, and bundle version pinning. Masking of
connection `extra` depends on key names. Template rendering coerces types under
some settings and not others. A contributor working from the source alone
produces prose that is *locally plausible and globally wrong*: it describes the
path they read, in the confident register documentation uses, with none of the
conditions attached. That is worse than an absent page. An absent page sends a
reader to the source or to Slack; a confidently wrong page sends them into
production.

The closed-PR record shows the failure repeatedly and in a recognisable shape.
A short "documentation for X" PR is opened; a maintainer who owns X reads it and
says the description is not correct, or not correct in the general case, or
duplicates the release notes, or is really an engineering task that needs
testing and design work before anything can be written down. Sometimes the
successor is a maintainer's own PR. The pattern holds regardless of how much
effort went into the original.

One adjacent habit makes it worse and appears in the same PRs: **generated prose
that was not read** — whole paragraphs duplicated within a page, sections that
restate each other, code samples longer than the explanation around them.
Reviewers spot duplicated lines immediately, and the response is consistently that
a contributor who cannot say why their own text says something twice should not be
asking maintainers to debug it. This one is mechanisable and stays a rule.

A note on inline code samples, which an earlier draft of this ADR treated as a
violation. Python pasted into a `code-block` is never executed and can rot
silently, and where a sample is a working Dag the tutorials and core-concepts
pages rightly pull it from `example_dags/` through `exampleinclude`. But the tree
uses inline `code-block:: python` 405 times against 69 `exampleinclude`
directives — a roughly 6:1 sanctioned majority — so inline Python is plainly the
house norm, not a defect. It is also not reviewable as a rule: whether an
`exampleinclude` "was available" is a fact about `example_dags/`, not something
visible in the diff under review. Prefer `exampleinclude` for anything that is a
complete, runnable Dag; that preference belongs in the area's `AGENTS.md` as
guidance, and it is not a violation here.

None of this means documentation-only contributions are unwelcome. Corrections,
clarifications of something the contributor genuinely hit, missing links, and
examples that were tried are among the most valuable changes this tree receives,
and they merge readily. The distinction is not doc-only versus code-plus-doc; it
is *grounded in something the author verified* versus *assembled from reading*.

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

- The documentation's failure mode becomes "incomplete" rather than "confidently
  wrong", which is the trade the project prefers: readers can tell that
  something is missing, and cannot tell that something is untrue.
- Gaps persist in exactly the areas where documentation is hardest and most
  wanted, because the people who could write those pages correctly are the same
  small group building the features.
- Willing contributors are turned away from the largest-looking documentation
  tasks, which are precisely the ones that need subsystem knowledge. The area's
  `AGENTS.md` should keep pointing them at corrections and at documenting what
  they actually used.
- Examples cost more to add — a real Dag, in the right place, that the suite
  runs — and in exchange stop being a source of silent breakage.

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

- #61605 (closed) — "Add documentation for Dag versioning behavior", closed
  because the description was not correct: versioning behaviour depends on
  whether the bundle supports it and whether versioning is enabled, and the
  feature interactions have many nuances the page flattened away.
- #58948 (closed) — a dependencies-documentation update closed as "even more
  inaccurate" than what it replaced, superseded by a maintainer's own PR.
- #57325 (closed) — a migration-path page for Airflow 2 users with database
  access, closed as too much code with too little prose and too much duplicated
  between it and the release notes; the maintainer response was that the task
  needs background, motivation, and testing, and is not a documentation task.
- #61596 (closed) — "fix misleading timetable variable example": the correction
  itself was right, and stalled only on author availability.
- #58539 (closed) — a masking-clarification PR closed for duplicated lines, with
  the maintainer position that a contributor who cannot explain their own diff
  should not expect reviewers to debug generated text.
- #60494 and #62279 (closed) — documentation PRs from contributors submitting
  unreviewed generated changes across many areas at once.
- #58556 (closed) — a connection-masking clarification closed because the same
  ground had already been covered by other PRs.
- #55817 (closed) — two contributors independently updating the same
  core-concepts pages, resolved by coordinating on the shared task list.
- #53708 (closed) — an incorrect `bash_command` example withdrawn by its author
  after realising the snippet needed imports to work at all: the case for
  including examples from Dag files the suite runs.
- #60270 (closed) — a configuration change for bundle file permissions, closed
  in favour of documenting the deployment pattern instead: the same judgement
  running in the other direction.
- Merged, for the shape this decision describes: #68373 (documenting that bundle
  kwargs reference a Connection rather than inline credentials), #69152
  (native template rendering type coercion), #68461 (when
  `last_automated_data_interval` is `None`), #68887 and #68886 (state-store
  method names and behaviour corrected alongside the feature), #69179
  (documentation misusing previous/next for task relationships).
