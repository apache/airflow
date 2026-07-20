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

# 5. UI dependency changes are deliberate, browser-verified, and land on main first

Date: 2026-07-20

## Status

Accepted

## Context

This directory carries a large npm dependency tree — React 19 and the React
compiler, Chakra, TanStack Table and Query, Vite, Playwright, Vitest, Monaco,
Chart.js, ESLint and its plugin set. Dependabot is configured to group minor and
patch updates per branch (`core-ui-package-updates` on `main`, and a
`3-N-core-ui-package-updates` group per active release branch) with a cooldown,
and it regenerates the group PR every time any member moves.

The consequence is a firehose. Closed-unmerged group bumps against this
directory outnumber every other kind of closed PR here combined: the group PR is
superseded within days, and the superseded ones are closed rather than rebased.
That is the process working, not failing — exactly one regeneration of each
group merges per cycle, and the others carry no information. It is also why
dependency traffic cannot be treated as ordinary review work: a reviewer who
starts reading a group bump is usually reading a diff that no longer exists.

Where real damage occurs is at the edges of that flow.

**Bulk bumps applied directly to a release branch.** A grouped upgrade of
several dozen packages is not verifiable by CI alone: the tests pass and the
rendered UI is subtly broken. On `main` that is recoverable, and the project
accepts it as the price of staying current. On a release branch it is not, and
when it happened the response was to revert the offending bumps rather than to
chase the breakage — a fix-forward on a release branch means shipping an
untested dependency tree to users.

**Reviving a bump that has been overtaken.** A security-motivated upgrade sitting
open for weeks is often already satisfied on `main` by a later bump or a
`pnpm.overrides` entry. Rebasing it produces an empty or misleading change; the
correct action is to confirm where `main` actually stands and close it.

**Adding or pinning a dependency to route around a problem.** A new package
introduced to work around a browser bug commits the project to maintaining a
divergent user experience for one browser, permanently, for a defect that
belongs upstream. A version pin added to dodge a regression is a workaround that
outlives everyone's memory of it unless it is tracked. And building on a
dependency the project has already decided to move away from spends effort that
is discarded when the removal happens — reviewers say so explicitly rather than
merging work into a component that is on its way out.

## Decision

Dependency changes in this directory are treated as behaviour changes:

- **Grouped bumps land on `main` first**, where a regression is recoverable, and
  reach a release branch only after the same change has been exercised there.
  A bulk bump that breaks a release branch is reverted, not patched forward.
- **Verify a bump in a running UI**, not only in CI. Grid, Graph, the Dag list,
  and any component owned by the upgraded package are opened in a browser; a
  green test suite is not evidence for a rendering library.
- **Check where `main` stands before reviving a stale dependency PR.** If the
  version range is already satisfied — by a later bump, a transitive upgrade, or
  a `pnpm.overrides` entry — close it rather than rebasing it into a no-op.
- **A new dependency needs agreement before the code.** Adding a package to
  work around a browser defect, or to reimplement something the existing stack
  already provides, is discussed in an issue or on the dev list first.
- **Do not build on a dependency that is slated for removal.** When a library is
  under active discussion for replacement, changes that deepen the project's
  investment in it wait for that decision.
- **A version pin is a workaround, not a fix.** It carries a tracking issue and
  a comment at the pin site naming the issue URL, so it can be removed.

## Consequences

- The dependency tree stays current without the project absorbing an
  unreviewable bulk change on a branch it is about to release from.
- Reviewers can ignore superseded group PRs without reading them, which is the
  only reason the volume is survivable.
- Browser verification makes dependency bumps meaningfully more expensive than
  merging what the bot proposes, and some upgrades therefore lag.
- Real browser incompatibilities go unaddressed in Airflow when the fix belongs
  upstream. That is a deliberate trade: a per-browser divergence in this UI
  costs more over time than the defect does.

A change **violates** this decision when it:

- applies a grouped or bulk dependency upgrade to a release branch without the
  equivalent change having been exercised on `main`;
- fixes the fallout of a broken bulk bump on a release branch instead of
  reverting the bump;
- bumps a dependency with no evidence that the affected views were opened in a
  browser;
- rebases a superseded dependency PR whose version range `main` already
  satisfies;
- adds an npm package to work around a browser bug, or to duplicate capability
  the existing stack already has, without prior agreement;
- extends or refactors code around a dependency whose removal is under active
  discussion;
- pins or caps a version without a tracking issue and a comment at the pin site
  linking to it.

## Evidence

- 156 grouped dependency PRs against this directory closed unmerged as
  superseded regenerations — roughly a third of every closed PR touching it —
  against the small number of group bumps that actually merge per cycle. The
  closure is mechanical and carries no review signal.
- #64466 — a release-branch UI static-check failure caused by two bulk
  dependency upgrades applied to that branch; the maintainer position was that
  bulk UI dependency updates are done on `main`, where breakage is expected and
  recoverable, and that reverting the two bumps was the safer path than fixing
  forward.
- #66797 and #66798 — security-motivated bumps of `happy-dom` and `vite` closed
  by their own author after rebasing revealed `main` had already moved well past
  the target versions, with a `pnpm.overrides` entry covering the vulnerable
  range.
- #58183 — a Firefox backfill-form fix closed by its author after review pushed
  back on adding an npm package: reusing the existing date-range calendar was
  not possible because of the browser limitation, and diverging the UX for one
  browser was judged worse than leaving the defect upstream.
- #55994 — a custom Chakra tooltip replacing the Chart.js default, closed with
  the decision to make the change as part of removing Chart.js from the Gantt
  chart rather than layering a workaround on a library on its way out.
- #68638 — removal of an unused Monaco-related dependency deferred pending the
  open discussion about whether the Monaco editor stays at all.
- #67796 and #67648 — competing PRs pinning and unpinning `@chakra-ui/react`
  around a dialog-mounting regression, illustrating the churn a pin generates
  when it is not tracked to a resolution.
