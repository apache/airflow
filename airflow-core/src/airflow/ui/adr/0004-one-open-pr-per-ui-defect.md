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

UI issues are highly visible and mostly small, so they attract several
contributors at once. The result, seen over and over in this area's closed-PR
record, is three or four branches fixing the same clipped dropdown, the same
calendar colour, the same connection form. Only one can merge. The others are
closed with "closing in favour of #NNNNN" — after a reviewer has already read
some or all of them, because a reviewer cannot tell that two PRs are the same
fix without reading both.

The cost lands entirely on the scarce side of the exchange. Every duplicate is a
full read by one of the few people who review this directory, and the
contributors whose PRs are closed have done real work for nothing. It is worse
when the duplicates arrive *after* a maintainer has opened their own fix, or
after review comments on the first PR have already been given: the feedback,
the reproduction discussion, and the reviewer's context all live on the branch
that is about to be discarded.

A related and entirely self-inflicted version of the same problem is the
**PR-per-revision** habit: a contributor gets review feedback, and instead of
pushing to the branch, opens a new PR containing the addressed feedback. The
review threads, the reasons behind them, and the record of what was already
tried do not travel. Maintainers correct this explicitly and repeatedly —
force-pushing the existing branch is always available, and a closed PR can be
reopened. The same applies to a branch whose history got tangled during a
rebase: the fix is to clean up the branch, not to abandon the thread.

Scope has the same shape as duplication. When one PR carries a bugfix *and* a
UX improvement, or the intended change *and* a handful of drive-by edits picked up
from an agent's working tree, the reviewer cannot approve the part that is ready
without also approving the part that is not. These get closed rather than
partially merged, and the split that should have happened up front happens anyway
— after the review.

**A component and its English catalogue keys are not such a pair, and must not be
split.** An earlier form of this decision asked for component logic to be
separated from the translation keys it introduces, which cannot be satisfied in
either direction:
[ADR-3](0003-user-facing-strings-go-through-the-locale-catalogues.md) treats
adding a user-facing string without adding its key to the English catalogue as its
own violation, and a component shipping `translate("foo.bar")` with no `en` entry
renders the raw key to users. The split is only available where it costs
correctness, so it is not asked for. What #64689 was actually closed for was an
*incomplete component* plus unrelated agent scratch files — the incompleteness and
the scratch files, not the co-location of a component with its own strings.

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

- Review time is spent once per defect rather than once per contributor, which
  is what makes the area's review capacity go far enough.
- Contributors are asked to do a search that is genuinely awkward — GitHub's PR
  search is poor at matching a symptom to a component — and some duplication
  will survive it. The check is still cheaper than the duplicate review.
- Taking over someone else's stalled branch becomes the normal path, and it
  requires coordination on the issue rather than a fresh PR.
- The first PR on an issue holds no reservation. A stalled branch is
  legitimately superseded, and this decision does not protect it.

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

- #67796, #67049, #66851, #66621, #65095, #64987, #64989, #67308, #67539,
  #67905, #55861, #56452, #53836, #59663, #60181, #60584, #51281 — each closed
  with "closing in favour of" a parallel PR fixing the same thing; several after
  a full review had already been written.
- #58102 — closed in favour of a competing PR that was closer to mergeable,
  with the reviewer noting it was not worth continuing the effort.
- #56854 and #55650 — two contributors and a maintainer converging on the same
  feature, resolved by consolidating onto one branch.
- #64281 — a new PR opened to address the review feedback left on the author's
  own previous PR; the reviewer's response was that this loses the review
  comments and the branch should have been updated instead.
- #57706 — "Why close and open a new one? All the context is here and unless
  you have a very good reason you should keep iterating on this PR."
- #67539 — closed for carrying too many drive-by changes alongside the intended
  timezone fix.
- #64689 — closed for an incomplete component carrying unrelated agent scratch
  files. Read narrowly: the incompleteness and the scratch files are what this
  decision catches. A finished component landing with its own English keys is not
  the same shape and is not refused.
- #66219 — closed because it carried both a bugfix and a UX improvement; a
  focused fix with a test was opened instead.
- #64268 and #61202 — closed for containing changes belonging to other PRs.
- #59847 and #59848 — unrelated provider files and cross-PR commits from a
  contributor's git workflow, closed as a batch.
- #58881, #55811, #56347 — branches hundreds to thousands of commits behind
  `main`, closed with the recommendation to start from a fresh branch.
