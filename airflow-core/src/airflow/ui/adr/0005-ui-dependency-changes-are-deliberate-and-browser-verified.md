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

This directory carries a large npm tree — React 19 and the React compiler, Chakra,
TanStack Table and Query, Vite, Playwright, Vitest, Monaco, Chart.js, ESLint.
Dependabot groups minor and patch updates per branch (`core-ui-package-updates` on
`main`, a `3-N-` group per release branch) and regenerates the group PR every time
any member moves. The result is a firehose: grouped bumps closed unmerged as
superseded regenerations outnumber every other kind of closed PR here combined.
That is the process working — one regeneration merges per cycle, the others carry no
information — and it is why a reviewer who starts reading a group bump is usually
reading a diff that no longer exists.

Real damage occurs at the edges. **Bulk bumps applied directly to a release
branch** are not verifiable by CI alone — tests pass and the UI is subtly broken;
on `main` that is recoverable, on a release branch the response is to revert, not
fix forward. **Reviving a bump already overtaken** on `main` by a later bump or a
`pnpm.overrides` entry produces an empty or misleading change; confirm where `main`
stands and close it. **Adding or pinning a dependency to route around a problem**
commits the project to a divergent per-browser UX permanently for a defect that
belongs upstream; a pin added to dodge a regression outlives everyone's memory
unless tracked; and building on a dependency slated for removal spends effort that
is discarded when the removal happens.

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

- The dependency tree stays current without the project absorbing an unreviewable
  bulk change on a branch it is about to release from.
- Reviewers can ignore superseded group PRs without reading them — the only reason
  the volume is survivable.
- Browser verification makes bumps more expensive than merging what the bot
  proposes, so some upgrades lag.
- Real browser incompatibilities go unaddressed in Airflow when the fix belongs
  upstream — a deliberate trade: a per-browser divergence costs more over time than
  the defect does.

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

- 156 grouped dependency PRs against this directory closed unmerged as superseded
  regenerations — roughly a third of every closed PR touching it — versus the few
  group bumps that merge per cycle. The closure carries no review signal.
- #64466 — a release-branch static-check failure from two bulk upgrades applied to
  that branch; the maintainer position: bulk UI dependency updates happen on `main`
  where breakage is recoverable, and reverting was safer than fixing forward.
- #66797, #66798 — security bumps of `happy-dom` and `vite` closed by their author
  after rebasing showed `main` had moved past the targets, with a `pnpm.overrides`
  entry covering the vulnerable range.
- #58183 — a Firefox backfill-form fix closed after review pushed back on adding an
  npm package: diverging the UX for one browser was judged worse than leaving the
  defect upstream.
- #55994 — a custom Chakra tooltip closed to be done as part of removing Chart.js
  from the Gantt chart rather than layering a workaround on a library on its way out.
- #68638 — removal of an unused Monaco dependency deferred pending the open
  discussion about whether the Monaco editor stays at all.
- #67796, #67648 — competing PRs pinning and unpinning `@chakra-ui/react` around a
  dialog-mounting regression: the churn a pin generates when untracked.
