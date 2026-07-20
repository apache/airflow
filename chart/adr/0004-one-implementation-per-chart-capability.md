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

# 4. One implementation per chart capability, and the simplest one wins

Date: 2026-07-20

## Status

Accepted

## Context

Chart features are unusually prone to being implemented several times in
parallel. The requests are visible (a log-retention setting, a cleanup CronJob,
worker groups, a missing checksum annotation), each is a self-contained change
to a handful of templates plus values, and the barrier to starting is low. The
result, repeatedly, is two or three open pull requests adding the same
capability in incompatible shapes at the same time.

For the chart specifically this is more damaging than elsewhere in the
repository:

- **Reviewer capacity is the binding constraint.** The people who can evaluate a
  template change against upgrade semantics are few, and every parallel
  implementation consumes that same scarce attention to reach a conclusion that
  only one of them can benefit from.
- **The values shape is permanent.** Two implementations do not merely differ in
  code; they propose two different public configuration surfaces. Whichever
  merges first fixes the contract users write against, so the choice cannot be
  deferred or reconciled later — see ADR 1.
- **Merging both is not an option, and merging the wrong one is expensive.**
  Undoing a merged values shape means a deprecation cycle and a major version,
  so the comparison has to happen before the merge, not after.
- **A second pull request against the same files is not a way to resolve
  review.** Opening a fresh PR rather than addressing comments on the existing
  one splits the discussion, hides which version reviewers already objected to,
  and reads as working around the reviewer.

The corollary the chart maintainers apply consistently: when two working
implementations exist, the one with the smaller surface wins, even when it is
the later one. Ordering is a tiebreak, not the criterion.

## Decision

- **Search open pull requests and issues for the capability before writing
  templates.** The check is on the capability, not on the exact title.
- **When an implementation is already open, contribute to it** — review it,
  extend it, or take it over with the author's agreement — rather than opening a
  competing one.
- **When two implementations exist, the smaller values surface and the simpler
  template logic win**, regardless of which was opened first; the other is
  closed by its author or by triage.
- **Review feedback is addressed on the pull request that received it.** A
  replacement PR is opened only when the original author has stepped away or the
  branch is unrecoverable, and it links the one it replaces.
- **While a parent issue is still consolidating a direction, individual template
  pull requests against its sub-parts wait.** Fragmenting an undecided direction
  into separate PRs produces contradictory partial changes.

## Consequences

- Reviewer attention concentrates on one candidate per capability, which is the
  only way features here actually land.
- Contributors sometimes lose work they had already finished. Making the
  duplicate check the first step of the task — not the last — is what keeps that
  cost small.
- Ordering does not confer a right to merge, so a contributor who moved fast on
  a complex design can still be asked to stand down for a simpler one.
- Triage carries the ongoing job of spotting duplicates early, while both
  branches are still cheap to abandon.
- **The existence of a competing open pull request is never itself a ground for
  rejection.** Airflow allows parallel work and the better pull request wins
  (root `CLAUDE.md`; `contributing-docs/04_how_to_contribute.rst`), and this ADR
  says so itself: the smaller surface wins regardless of filing order. Treating
  "another PR does this" as a disposition would close whichever one is looked at
  second, which is a coin toss, not a decision. Surfacing a duplicate is a triage
  **comment** — "see also #NNNN, these two need to be compared" — and the
  comparison then happens on its merits.

A change **violates** this decision when it:

- adds a capability that a **recently merged** pull request already provides,
  without saying why the merged one is insufficient;
- re-implements the author's **own** change that is under review, as a fresh pull
  request, instead of addressing the comments on the original;
- lands part of a direction that a parent issue is still consolidating;
- takes over another contributor's work without a note on the original pull
  request.

## Evidence

- #58585 — a Helm DB-cleanup implementation closed because #58155 had just added
  a db-clean CronJob: the reviewer's note was that checking for an existing open
  pull request is worth doing before starting.
- #56589 — multiple Celery worker groups closed by its own author in favour of
  #58547, which arrived later but was simpler and already covered KEDA and HPA.
- #61859 and #61853 — two parallel minute-level log-retention pull requests,
  both obsoleted when #61855 merged.
- #61044 — per-bundle Dag processor deployments closed as a duplicate of the
  in-flight #61039.
- #60783 — a `skipKubernetesEnvVars` option superseded by the earlier #60750,
  which removed the variables outright rather than adding a knob to hide them.
- #63027 — a second pull request opened against the same deployments instead of
  correcting #62178; both were closed, with the reviewer noting that the
  original's review comments were being bypassed.
- #64058 and #64128 — webserver configuration deprecation PRs closed so the
  parent issue could consolidate the direction rather than land it piecemeal.
- #54493 — a stalled pull request handed over explicitly, with the original
  closed once another contributor picked the work up.
