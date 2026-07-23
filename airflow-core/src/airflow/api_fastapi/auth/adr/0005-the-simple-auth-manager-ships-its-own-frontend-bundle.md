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

# 5. The Simple auth manager ships its own frontend bundle, changed on main first

Date: 2026-07-20

## Status

Accepted

## Context

`managers/simple/ui/` is a **second, independent frontend application** inside this
Python package: its own `package.json`, lockfile, generated API client, and build
output, packaged into the distribution and served as the login surface. It is not part
of the main Airflow UI, shares neither its dependency pins nor its tests.

This shows up as a steady stream of grouped dependency-bump PRs opened, superseded,
and closed without landing. The reasons repeat: the branch has moved past the proposed
version; another group rule supersedes it; the bump targets versions far behind main;
or the bump is aimed at a maintenance branch rather than main. The last has teeth: bulk
frontend upgrades break things — CI and the rendered UI — and the breakage is found by
using it. Airflow absorbs that on `main`, where it is cheap, then flows it to
maintenance branches; landing a bulk upgrade directly on a release branch takes the
risk where it is most expensive, on a surface whose failure mode is nobody can log in.
Airflow has reverted bulk UI upgrades made that way. A supply-chain nuance: because this
bundle has its own lockfile, transitive security ranges are pinned via the package
manager's override mechanism, covering a range once instead of chasing packages
individually.

## Decision

This bundle is treated as a distinct frontend deliverable:

- **Frontend dependency changes land on `main` first**, and reach maintenance
  branches by backport — never as a bulk upgrade opened directly against a release
  branch.
- **A bulk upgrade is validated in the running UI**, not only by green CI. The
  login and required-action flows are exercised, and the evidence goes in the PR.
- **Cover a vulnerable range with the lockfile override mechanism** rather than
  chasing individual package declarations, and check first whether the range is
  already covered.
- **Rebase before reviving a stale bump.** Check whether the branch already
  carries an equal or newer version; if it does, close rather than re-target.
- **Changes to this bundle are separate from Python-side auth changes.** A PR that
  edits the manager's Python surface and its bundled UI in one diff needs
  splitting so the frontend and the auth logic get the reviewers each requires.

## Consequences

- Login-surface breakage is discovered on `main`, where it is cheap, not on a release
  branch. Release branches carry older frontend dependencies for longer, on purpose.
- Automated bump PRs churn — most closed as superseded; that is the expected steady
  state, not a backlog to drain by merging them.
- A second bundle costs a second lockfile, build, and set of upgrade decisions — an
  accepted cost of shipping a usable default auth manager in core.

A change **violates** this decision when it:

- opens a bulk frontend dependency upgrade for this bundle directly against a
  maintenance/release branch instead of `main`;
- lands a bulk upgrade on green CI alone, with no evidence the login UI was
  exercised;
- narrows a declared package version to address a vulnerable range that the
  lockfile override mechanism already covers, or that main has already moved past;
- mixes Python auth-manager changes and this bundle's frontend changes in one PR.

## Evidence

- #64466, and the reverts it drove of #64440 and #64439 — bulk UI bumps on a
  maintenance branch; reviewers noted these belong on `main` and reverting was safer
  than carrying them on a release branch without thorough UI testing.
- #66797 and #66798 — patched-release bumps closed after rebasing showed main had
  moved well past the targets, the vulnerable range already covered by a lockfile
  override.
- #68768, #68473, #68092, #65719, #65471, #65152, #65025, #64275 — grouped
  dependency-update PRs, each closed as superseded/regenerated; the volume is what
  makes the "on main first" rule load-bearing.
