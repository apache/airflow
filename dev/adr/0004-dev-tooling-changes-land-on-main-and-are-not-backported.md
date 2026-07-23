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

# 4. Dev tooling changes land on `main` and are not backported

Date: 2026-07-20

## Status

Accepted

## Context

`dev/` looks safe to backport: it ships nothing to users, so a contributor who fixes a
breeze command on `main` naturally opens the same change against `v3-X-test`, and a
release manager wants the newest tooling. That instinct is wrong here:

- **`dev/` on the release branches has already diverged far from `main`.** Cherry-picking
  into that divergence produces conflicts on every subsequent backport of *real* code,
  the traffic that actually matters on those branches.
- **A release branch's tooling only has to build and verify that branch.** A refactor, new
  command or tidier option layout is pure risk against a branch about to be voted on.
- **A branch past end of life takes security fixes only** — a routine dev-dependency
  update is noise that still costs a CI cycle and a reviewer. (#67008, `v2-11-test`.)
- **The Helm chart, providers and airflow-ctl release from `main`**, so "the fix must go
  to the release branch or users won't get it" is usually not even true.

The exception is neither rare nor cosmetic. `git log origin/v3-1-test -- dev/` carries
hundreds of legitimate merged commits — branch-local build fixes, tool pins the branch's
CI needed (`[v3-1-test] Upgrade Hatch to 1.16.5 and revert virtualenv pin (#62602)
(#62611)`), recurring CI-environment upgrades — because a release branch has to keep
building while it is supported. What separates a legitimate branch-local fix from a
forbidden backport is **not visible in the diff**: the two are frequently byte-identical,
and the difference is only the justification — *this branch's build is broken this way and
this unbreaks it* versus *`main` got better and this branch should have it too*. That is
why the rule asks for a statement in the PR rather than reading intent from the file list.

## Decision

- **An *improvement* to `dev/` is opened against `main` only.** Refactors, new
  commands, new options and doc updates are not cherry-picked onto `v3-X-test` or
  older branches just because `main` has them.
- **A `dev/` change targeting a release branch states, in the pull request, which
  failure *on that branch* it fixes** — the failing job, the broken build step,
  the pin that no longer resolves. That statement is the whole test. With it, the
  change is a branch-local build fix and is welcome, whether or not an identical
  commit exists on `main`. Without it, it is a backport of a `main` improvement
  and is closed.
- **Branches past end of life accept security-relevant changes only.** Once a
  branch no longer produces releases, routine dependency updates and convenience
  improvements to its tooling are closed. Branches still cutting patch releases
  keep taking the pin and CI-environment updates their builds need.
- **When a backport is opened by mistake, the fix is to land the change on
  `main`** and close the backport, not to fix the backport up.

## Consequences

- Release branches stay quiet, so backports of actual product fixes rebase cleanly under
  vote deadlines; the cost is that contributors there work with older tooling and re-hit
  paper cuts `main` has fixed.
- A tooling fix that genuinely blocks a release is written twice — once for `main`, once
  for the branch, the branch version deliberately smaller.
- Dependency bots open backport-shaped PRs against release branches anyway; closing them
  is routine triage, not a judgement on the bot.

A change **violates** this decision when it:

- targets a `v3-X-test` or `v2-XX-test` branch, touches `dev/`, and the pull
  request does not name the failure *on that branch* the change fixes (a
  `dev/`-confined backport is normal and expected when it does — see the Context);
- backports a refactor, rename, new command or new breeze option to a release
  branch because "it is only dev tooling", with no branch-local failure to point
  at;
- adds routine dependency or tool-version updates to a branch that is past end of
  life and no longer cutting releases;
- changes release-branch tooling in the same pull request as a `main`-targeted
  improvement, so the two cannot be evaluated separately.

## Evidence

- #59490 — a `dev/` refactor backported to `v3-1-test` and closed: refactors are not
  backported, and `dev/` has diverged enough that such backports create conflict work.
- #67008 — grouped dev-tooling dependency updates against `v2-11-test` closed: the branch
  no longer cuts releases and the updates are not release-critical.
- `[v3-1-test] Upgrade Hatch to 1.16.5 and revert virtualenv pin (#62602) (#62611)` — the
  counter-case: a `dev/`-confined, identical-to-`main` change that merged because the
  branch's own build needed it.
- #64325 — a `v3-2-test` backport of the uv resolution cooldown that accumulated extra
  fixes and was closed in favour of #64417.
- #60923 — a release-branch CI environment change made incorrectly and replaced by
  #60929: even a legitimate branch-local change is redone for that branch, not patched up.
- #58801 — a chart/Docker feature backport to `v2-11` declined, the plan being to build
  the 2.11 release from the newer branch's scripts.
