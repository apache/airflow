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

# 2. Dev tooling never becomes a release-blocking single point of failure

Date: 2026-07-20

## Status

Accepted

## Context

Nothing under `dev/` ships to users at runtime, which makes it tempting to treat
changes here as low-risk. The opposite is true operationally. Breeze is the only
supported way to run Airflow's tests and build its images, so every contributor
depends on it; the `release-management` breeze commands and the
`README_RELEASE_*.md` runbooks are what release managers execute, under ASF vote
deadlines, to produce the artifacts the PMC votes on.

Two properties make defects here disproportionately expensive.

First, the *installed base* is invisible from the diff. Breeze runs from the
current git worktree's sources via `uvx`, on macOS and Linux, on AMD and ARM,
inside and outside worktrees, under rootless docker — the conditions already
recorded in `dev/breeze/doc/adr/`. A change that works in the author's checkout
can break contributors who did nothing at all, and the breakage arrives as "I
can't run any tests today" rather than as a failing job.

Second, a release defect is discovered *late*. A tarball that is subtly wrong, a
step that half-completed, or a metadata command that hangs is found when a vote
is already in flight; the recovery is a re-roll of the whole release candidate,
not a hotfix. Airflow has hit each of these: uvx path assumptions broke the
provider release tooling, a pool choice hung `generate-providers-metadata`, and
an artifact-completeness gate had to be added before the vote because
incompleteness was previously found after it.

The common thread in the recoveries is that the tooling should have *stopped*
rather than continued. Silent partial success is what turns a small tooling bug
into a release-blocking one.

## Decision

- Changes to breeze and to `dev/` scripts stay backward-compatible for existing
  worktrees and workflows. Where a change genuinely requires contributor action
  (re-running setup, rebuilding an image, clearing a cache), the tooling detects
  the stale state and says so, and the pull request states it explicitly.
- Tooling fails loudly. A missing token, absent dependency, unavailable path,
  unsupported platform, or unmet precondition produces a clear message and a
  non-zero exit — never a partial run that reports success or emits an
  incomplete artifact.
- Release steps are re-runnable. A step that failed partway can be repeated
  without producing duplicate, partial or corrupt artifacts.
- Verification gates run *before* the artifacts are put to a vote, not after.
- Tooling and runbook move together: a change to a `release-management` command
  updates the matching step in `README_RELEASE_*.md`, and a change to a runbook
  step that the tooling implements updates the command.
- Scripts and docs use the `upstream` / `origin` remote convention, and no
  script pushes to `upstream`.
- Reproducibility of the artifacts the ASF votes on is preserved; changes near
  source-tarball generation, export rules or `.dockerignore` are treated as
  release-critical.

## Consequences

Contributors can update their checkout without a tooling break stopping their
day, and release managers can trust that a command either completed or told them
it did not. Release candidates fail at the gate rather than at the vote. The
cost is that dev-tooling pull requests carry obligations that feel heavy for
their size — compatibility reasoning, explicit error paths, a paired runbook
edit — and that convenient assumptions about the author's own environment are
not available.

A change *violates* this decision when it:

- assumes a fresh checkout, a specific install layout, a single platform, or a
  newly-added option being present, so that existing worktrees stop working
  without warning;
- swallows a missing precondition — token, dependency, path, platform — and
  continues, producing a partial or empty artifact that looks like success;
- introduces a release step that cannot be safely re-run after a partial
  failure, or that duplicates artifacts when repeated;
- moves a correctness check to after the vote, or removes a pre-vote
  completeness or reproducibility gate;
- changes a `release-management` command without updating the corresponding
  `README_RELEASE_*.md` step (or the reverse);
- hardcodes a git remote name other than the `upstream` / `origin` convention,
  or pushes to `upstream`.

## Evidence

- #67960 — fixed breeze provider release tooling after uvx path assumptions and
  issue submission broke the release manager's flow.
- #68192 — added a breeze shim fallback for use outside worktrees, so the
  installed base kept working after the uvx-from-sources move.
- #65873 — moved breeze to running via uvx from the current worktree, the change
  whose compatibility edges the two fixes above addressed.
- #69141 — gated the provider release on an artifact-completeness check before
  the vote rather than discovering incompleteness during it.
- #69763 — fixed a `generate-providers-metadata` hang by using spawn pools; a
  release command that hangs blocks the release manager with no diagnosis.
- #69417 — supported delegating the providers release process to non-PMC
  committers, removing a single-person dependency from the flow.
- #68641 — added an explicit warning for release managers to verify
  `prepare-providers-documentation` output before relying on it.
- #65629 — standardised the `upstream` / `origin` git remote naming across docs
  and tooling.
