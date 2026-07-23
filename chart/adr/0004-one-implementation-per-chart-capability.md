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

Chart features are unusually prone to parallel implementation — requests are visible,
each is a small self-contained change to a few templates plus values, and the result is
repeatedly two or three open PRs adding the same capability in incompatible shapes at
once. This is more damaging here than elsewhere: **reviewer capacity is the binding
constraint** and every parallel implementation consumes the same scarce attention; **the
values shape is permanent** — whichever merges first fixes the public contract (ADR 1),
so the choice cannot be deferred, and undoing it costs a deprecation cycle and a major
version; and **a second PR against the same files** splits the discussion and hides which
version reviewers objected to. The corollary maintainers apply: when two working
implementations exist, the smaller surface wins, even when it is the later one; ordering
is a tiebreak, not the criterion.

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

- Reviewer attention concentrates on one candidate per capability — the only way features
  here land. Contributors sometimes lose finished work; making the duplicate check the
  first step keeps that cost small.
- Ordering confers no right to merge, so a fast, complex design can be asked to stand down
  for a simpler one; triage spots duplicates early, while both branches are cheap to
  abandon.
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

- #58585 — a Helm DB-cleanup implementation closed because #58155 had just added a
  db-clean CronJob; the reviewer's note: check for an existing PR before starting.
- #56589 — multiple Celery worker groups closed by its own author in favour of #58547,
  which arrived later but was simpler and already covered KEDA and HPA.
- #61859 / #61853 — two parallel log-retention PRs, both obsoleted when #61855 merged.
- #61044 — per-bundle Dag processor deployments closed as a duplicate of in-flight
  #61039.
- #60783 — a `skipKubernetesEnvVars` option superseded by the earlier #60750, which
  removed the variables outright rather than adding a knob to hide them.
- #63027 — a second PR opened against the same deployments instead of correcting #62178;
  both closed, the original's review comments being bypassed.
- #64058 / #64128 — webserver config deprecation PRs closed so the parent issue could
  consolidate the direction rather than land it piecemeal.
- #54493 — a stalled PR handed over explicitly, the original closed once another
  contributor picked the work up.
