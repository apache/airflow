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

# 1. A released migration is immutable — fix forward, never rewrite

Date: 2026-07-19

## Status

Accepted

## Context

The migrations under `versions/` are not ordinary source. Each revision is a step
in an *append-only history* that every installation replays exactly once to move
its metadata database from one schema state to the next. The chain is what turns
"the database of a user still on 2.10" into "the database of a user on 3.x".

The consequence is unforgiving: **once a revision has shipped in a released
Airflow version, it has already run on real databases and will never run again on
them.** Editing that revision's `upgrade()` or `downgrade()` after the fact does
*not* retroactively change the databases that already applied it. It only changes
what *future* installations do — so the two populations diverge permanently, and
there is no schema state that is consistent for both. A "quick fix" to a shipped
migration is therefore not a fix at all; it is a second, silent bug.

This pressure is constant because the natural instinct when a released migration
turns out to be wrong is to open the file and correct it. The history above shows
this repeatedly: the right move was always a *new* corrective revision layered on
top, never an in-place edit — for example restoring nullability with a follow-up
rather than rewriting the offending revision, or moving a not-yet-released change
to a different target version while it was *still* unreleased.

Only *unreleased* revisions — those that exist on `main`/a test branch but have
not gone out in any release — are still malleable. Those may be edited, squashed,
retargeted, or dropped, because no user has a database pinned to them.

## Decision

A migration that has shipped in a released version is **immutable**. Corrections
are made **forward**, never by rewriting released history.

- To fix behaviour introduced by a released revision, add a **new revision** on
  top of the current head that corrects the schema/data going forward. Do not
  edit the released revision's `upgrade()`/`downgrade()`.
- An **unreleased** revision (not yet in any release) *may* be edited, retargeted,
  or dropped — that is the only window in which the history is still soft.
- When correcting forward, keep the chain linear (see ADR 2): the corrective
  revision's `down_revision` is the current head, and it becomes the new head.

## Consequences

- Databases that already ran a revision and databases that run it in the future
  always reach the *same* schema state from that revision — the two populations
  never diverge.
- Corrective work costs an extra revision rather than a one-line edit; that cost
  is the price of a consistent, replayable history and is intentional.
- Reviewers must know the release boundary — whether the touched revision has
  shipped — because it is not visible in the diff. When in doubt, treat it as
  released and fix forward.

A change **violates** this decision when it:

- edits the `upgrade()` or `downgrade()` of a revision that has already shipped in
  a released version, instead of adding a new corrective revision;
- reuses or repurposes a released revision's identifier for different DDL;
- "retcons" a change onto a version that is *already released* (retargeting is
  only legitimate while the change is still unreleased).

A reviewer should reject any change that mutates released migration history rather
than layering a new revision on top of the current head.

## Evidence

- #63899 — "Restore nullable ORM fields and drop unreleased corrective migration":
  the corrective migration was *unreleased*, so dropping it was legitimate — the
  fix restored the fields forward rather than rewriting released history.
- #62569 — "Retcon migration chain: move signed_url_template change to 3.1.8":
  retargeting a change to a different version, done while it was still unreleased
  and keeping the chain consistent.
- #63825 — "Do not backfill old DagRun.created_at": chose to *not* reach back and
  rewrite historical rows, forward-only behaviour over retroactive mutation.
