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

# 4. One open pull request per UI defect, iterated in place

Date: 2026-07-20

## Status

Accepted

## Context

UI issues are visible and mostly small, so they attract several contributors at
once. The closed-PR record is full of three or four branches fixing the same
clipped dropdown or calendar colour; only one merges, and the rest are closed
"in favour of #NNNNN" — often after a reviewer has already read them, because two
PRs cannot be told apart without reading both. The cost lands entirely on the
scarce side: the few people who review this directory. It is worse when duplicates
arrive after a maintainer's own fix, or after review comments already exist on the
first PR — the feedback and reproduction discussion live on the branch about to be
discarded. The **PR-per-revision** habit is the self-inflicted version: instead of
pushing to the branch, a contributor opens a new PR carrying the addressed feedback,
and the review threads do not travel. Force-pushing is always available, and a
closed PR can be reopened.

Scope has the same shape. A PR carrying a bugfix *and* a UX improvement, or the
change *and* drive-by edits from an agent's working tree, cannot be partially
approved, so it gets closed rather than split after review. **A component and its
English catalogue keys are not such a pair, and must not be split.** An earlier form
of this decision asked to separate component logic from the translation keys it
introduces, which cannot be satisfied either way:
[ADR-3](0003-user-facing-strings-go-through-the-locale-catalogues.md) makes adding a
string without its English key its own violation, and a component shipping
`translate("foo.bar")` with no `en` entry renders the raw key. So the split is not
asked for. #64689 was closed for an *incomplete component* plus unrelated agent
scratch files — not the co-location of a component with its own strings.

## Decision

Work converges on one branch per defect:

- **Search for existing work before writing any code.** Check the issue for
  linked PRs, and search open PRs for the component and the symptom. If a PR
  already exists, review it, test it, or offer to take it over — do not open a
  parallel one.
- **Iterate in place.** Address review feedback by pushing (or force-pushing) to
  the same branch. Do not open a successor PR to carry the fixes; reopen a
  closed PR rather than replacing it.
- **One concern per PR.** Separate a bugfix from an improvement to the same
  component; separate a refactor from the behaviour change it enables. A component
  and the English catalogue keys it introduces are *one* concern — ship them
  together.
- **No drive-by changes.** Everything in the diff must be explained by the PR's
  stated purpose. Files an editor, formatter, or coding agent touched
  incidentally are removed before pushing.
- **Keep the branch current.** Rebase onto `main` before requesting review;
  a branch far enough behind that its diff no longer describes a change against
  today's code is restarted, not resolved.
- **When two PRs do exist, the one that merges is the one that is complete** —
  reproduced, evidenced, tested, and rebased — not the one that was opened
  first. The other author is credited by review, not by a competing branch.

## Consequences

- Review time is spent once per defect rather than once per contributor, which is
  what makes the area's review capacity go far enough.
- The required search is genuinely awkward — GitHub's PR search matches a symptom to
  a component poorly — so some duplication survives it; the check is still cheaper
  than the duplicate review.
- Taking over a stalled branch becomes the normal path, coordinated on the issue.
- The first PR on an issue holds no reservation: a stalled branch is legitimately
  superseded, and this decision does not protect it.

A change **violates** this decision when it:

- fixes a defect that an open PR, linked from the issue or discoverable by the
  component name, already addresses;
- is a new PR opened to carry review feedback that belongs on an existing
  branch, or to replace a branch whose history the author tangled;
- combines a bugfix with an unrelated improvement in one diff;
- contains files unrelated to its stated purpose — agent scratch files, editor
  configuration, unrelated formatting, or commits belonging to another change;
- has been left far enough behind `main` that it no longer applies, and is
  offered for review in that state.

## Evidence

- #67796, #67049, #66851, #66621, #65095, #64987, #64989, #67308, #67539, #67905,
  #55861, #56452, #53836, #59663, #60181, #60584, #51281 — each closed "in favour
  of" a parallel PR fixing the same thing; several after a full review.
- #58102 — closed for a competing PR closer to mergeable.
- #56854, #55650 — contributors and a maintainer converging on one feature,
  consolidated onto one branch.
- #64281 — a new PR opened to carry feedback from the author's own previous PR;
  the reviewer noted this loses the review comments.
- #57706 — "Why close and open a new one? All the context is here … you should keep
  iterating on this PR."
- #67539 — closed for drive-by changes alongside the intended timezone fix.
- #64689 — closed for an incomplete component carrying unrelated agent scratch
  files. Read narrowly: the incompleteness and the scratch files are what this
  decision catches. A finished component landing with its own English keys is not
  the same shape and is not refused.
- #66219 — closed for carrying both a bugfix and a UX improvement.
- #64268, #61202 — closed for containing changes belonging to other PRs.
- #59847, #59848 — unrelated provider files and cross-PR commits, closed as a batch.
- #58881, #55811, #56347 — branches far behind `main`, closed with the
  recommendation to start fresh.
