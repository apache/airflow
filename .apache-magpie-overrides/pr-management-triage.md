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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Apache Airflow — pr-management-triage overrides](#apache-airflow--pr-management-triage-overrides)
  - [Override 1 — Skip the PR entirely when a maintainer is already involved](#override-1--skip-the-pr-entirely-when-a-maintainer-is-already-involved)
    - [What counts as "a maintainer is already involved"](#what-counts-as-a-maintainer-is-already-involved)
    - [What does NOT count](#what-does-not-count)
    - [Effect](#effect)
    - [Why](#why)
  - [Override 2 — Never chase a human for an answer](#override-2--never-chase-a-human-for-an-answer)
    - [Actions that are dropped](#actions-that-are-dropped)
    - [Actions that are still allowed](#actions-that-are-still-allowed)
    - [Why](#why-1)
  - [Interaction with the framework's own pre-filters](#interaction-with-the-frameworks-own-pre-filters)
  - [Upstreaming](#upstreaming)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Apache Airflow — pr-management-triage overrides

Adopter overrides for the
[`pr-management-triage`](../.apache-magpie/skills/pr-management-triage/SKILL.md)
skill, applied on every invocation per the framework's
[agentic-overrides contract](https://github.com/apache/magpie/blob/main/docs/setup/agentic-overrides.md).

Both overrides below narrow what triage is allowed to do on
Airflow. The shape they produce: **triage does mechanical,
deterministic work — CI state, merge conflicts, labels, workflow
approval, stale hygiene — and never conversation work.** The
moment a human maintainer is on a PR, or the only thing left to
do is ask a human to reply, triage stands down.

## Override 1 — Skip the PR entirely when a maintainer is already involved

Treat this as an additional pre-filter, **F5d**, evaluated
alongside F1–F6 in
[`classify-and-act.md#pre-filters`](../.apache-magpie/skills/pr-management-triage/classify-and-act.md)
— and, unlike F5a/F5b/F5c/F6, also honoured by **every stale
sweep (1–5)** in `stale-sweeps.md`.

### What counts as "a maintainer is already involved"

Any of the following, resolved with the framework's
[Maintainer activity](../.apache-magpie/skills/pr-management-triage/classify-and-act.md#maintainer-activity)
definition (`committers_team` membership or repo permission
`write`/`maintain`/`admin` — **not** `authorAssociation` alone;
confirm a load-bearing `COLLABORATOR` hit with the live
permission/team check):

- Any node in `comments(last:10)` authored by a maintainer.
- Any node in `reviewThreads.nodes.comments` authored by a
  maintainer — resolved or unresolved.
- Any `latestReviews` node by a maintainer in state
  `COMMENTED`, `CHANGES_REQUESTED`, or `APPROVED`.

**No qualifiers.** No 72-hour window (unlike F5a), no
"after `commits(last:1).committedDate`" test, no minimum body
length (unlike F6), no draft-only restriction (unlike F6), and
no requirement that the comment be a question or a ping (unlike
F5b/F5c). One maintainer comment from eight months ago is
enough. If a committer has spoken on the PR at all, the PR is
theirs.

### What does NOT count

- **The viewer's own triage-generated artifacts.** The
  `pr-triage-fold` body block, quality-criteria marker comments,
  `ping` / `request-author-confirmation` bodies, the
  `stale-ready-label-strip` audit marker, label changes, and
  workflow approvals are this skill's own output, not a human
  maintainer engaging. Counting them would make every
  previously-triaged PR permanently untouchable.
- **Anything authored by a bot** (`*[bot]`, `github-actions`,
  Copilot reviews) regardless of its `authorAssociation`.
- **The viewer's own hand-written comments do count.** If the
  maintainer running triage has personally written a review or a
  substantive comment on the PR (anything that is not one of the
  templated artifacts above), the PR is maintainer-involved and
  gets skipped like any other.

### Effect

The PR is classified `skip` and **no mutation of any kind is
proposed or performed** — no comment, no body fold, no label
add or strip, no draft conversion, no close, no rerun, no
rebase, no workflow approval. This overrides every signal in the
decision table and every sweep trigger: red CI, merge conflicts,
unresolved threads, stale drafts, inactivity timers, and a
rotted `ready for maintainer review` label all lose to it.

This is deliberately the **strict** reading of "skip all
actions" — it includes the otherwise-harmless mechanical rows
(`approve-workflow`, `rerun`, `rebase`, `mark-ready`). A
maintainer who is already on the PR can take those steps
themselves, and them doing so is cheaper than triage acting
inside a conversation it cannot read. To relax it later, list
the exempted actions here explicitly rather than softening the
filter.

**Report, don't act.** Still surface these PRs in the session
summary under a *"maintainer already involved — skipped"*
heading, with the PR number, which maintainer engaged, and when.
The point is to hand them to a human, not to hide them.

### Why

The framework's F5a (72h cooldown), F5b (maintainer→maintainer
ping), F5c (author question unanswered) and F6 (maintainer
co-drafted) each cover one slice of "a human is mid-conversation
here". On Airflow's traffic, the slices leak: a maintainer
comments, the cooldown expires, and the next sweep flags the PR
for CI failures the maintainer already knows about — talking
over the human and re-opening a thread they had parked. Once any
committer has engaged, the PR's queue position and next move
belong to that person. Losing a few genuinely triage-eligible
PRs to an over-broad filter is much cheaper than contradicting a
maintainer once.

## Override 2 — Never chase a human for an answer

Drop every action whose only purpose is to ask a human to
respond to an earlier, unanswered message. This applies whether
the human being chased is the PR author, a reviewer, or another
maintainer, and whether the chase is a decision-table row, a
sweep, or something the agent improvises during a session.

### Actions that are dropped

| Source | Default action | Under this override |
|---|---|---|
| Row 15 — unresolved review threads from collaborators | `ping` | `skip` |
| Row 18 — `stale_review`, author pushed after `CHANGES_REQUESTED` | `ping` | `skip` |
| Row 14c — threads likely addressed | `request-author-confirmation` | `skip` |
| [Sweep 5](../.apache-magpie/skills/pr-management-triage/stale-sweeps.md#sweep-5--stale-author-confirm-request) — stale author-confirm-request | `ping` (escalation) | do not run the sweep at all |
| [Sweep 4](../.apache-magpie/skills/pr-management-triage/stale-sweeps.md#sweep-4--stale-ready-for-review-label) author-court dispositions | `strip-ready-label` **+ `ping`** | keep the strip and its `stale-ready-label-strip` audit marker; drop the `ping`. The audit marker already records what was stripped, why, and whose move is next — that is an explanation of an action taken, not a chase. If the disposition's *only* content would have been the nudge, skip the PR instead. |
| Ad-hoc reminders invented during a session ("any update?", "friendly ping", reviewer re-review nudges) | — | never post |

Row 14b (`awaiting_author_confirmation` → `skip`) is unchanged;
it was already a skip. With row 14c dropped, no new confirmation
requests are created, so
[`viewer_confirmation_request_present`](../.apache-magpie/skills/pr-management-triage/classify-and-act.md#viewer_confirmation_request_present)
only ever matches requests posted before this override took
effect — leave those alone, do not escalate them.

### Actions that are still allowed

These are not chases and remain in force:

- First-time deterministic quality feedback via the pr-body fold
  (`draft` / `comment` / `close`) — a statement of what CI or the
  merge state says, delivered once.
- The security-language comment and the suspicious-changes
  comment — both exist to notify a human of something new.
- The `stale-ready-label-strip` audit marker accompanying a
  Sweep-4 strip.
- Every non-comment mechanical action: `rerun`, `rebase`,
  `mark-ready`, `approve-workflow`, `promote-bot-draft`,
  `strip-ready-label`.

### Why

A ping restates what GitHub's own notifications already
delivered, to someone who is not blocked on being reminded. The
cost lands on real inboxes; the benefit is a nudge the recipient
almost always already had. Unanswered questions on an Airflow PR
get resolved when a person decides to engage — automating the
reminder does not move that decision, it just adds noise to the
same thread.

## Interaction with the framework's own pre-filters

F5a, F5b, F5c and F6 stay in force. Override 1 is strictly
broader than all four, so they never change an outcome on their
own — they are kept so that nothing regresses if this override
is later narrowed, and so a framework upgrade that retunes them
needs no coordination here.

## Upstreaming

Both overrides are policy calibration, not bug fixes, and should
**not** be upstreamed as new framework defaults — the framework's
graduated F5a/F5b/F5c/F6 middle ground is the right default for a
project that wants triage to keep nudging. What *is* worth
upstreaming is making each one a config knob in
`pr-management-config.md`, so adopters pick a stance without an
agentic override:

- `maintainer_involvement_policy: cooldown | skip-always`
  (framework default `cooldown` = today's F5a/F5b/F5c/F6).
- `nudge_actions: on | off` (framework default `on`), gating
  rows 15 / 18 / 14c and Sweep 5.

Until those exist, this file is the mechanism.
