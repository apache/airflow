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

The revisions under `versions/` are an *append-only history* that every
installation replays exactly once to move its metadata database forward. The
consequence is unforgiving: **once a revision has shipped in a released Airflow
version, it has already run on real databases and will never run again on them.**
Editing its `upgrade()`/`downgrade()` after the fact does not change the databases
that already applied it — it only changes what *future* installations do, so the
two populations diverge permanently with no schema state consistent for both. A
"quick fix" to a shipped migration is a second, silent bug. The right move is
always a *new* corrective revision layered on top (e.g. restoring nullability with
a follow-up), never an in-place edit. Only *unreleased* revisions — on `main`/a
test branch, not in any release — are still malleable: they may be edited,
squashed, retargeted, or dropped, because no user has a database pinned to them.

There is one case where fixing forward is not merely expensive but *impossible*: a
released revision that **cannot complete**. If an `upgrade()` raises, the operator
never reaches the head where a corrective revision would live. If a `downgrade()`
errors, the broken code is precisely what runs; no revision on top of the head
changes the way down. In both, the population that "already ran it" is empty for
exactly the databases the fix targets, so the divergence argument does not apply.
This is why the project has correctly edited released revisions in place: #66016
repaired `0080`'s `upgrade()`/`downgrade()`, both raising `IntegrityError` on a
non-empty `deadline` table; #65288 added a missing `disable_sqlite_fkeys` to
`0108`'s downgrade; #65688 fixed a downgrade crashing on a unique-constraint
violation. Such an edit must be **idempotent** for databases that ran the revision
successfully — it adds the missing work, never redoing what already succeeded.

## Decision

A migration that has shipped in a released version is **immutable**. Corrections
are made **forward**, never by rewriting released history.

- To fix behaviour introduced by a released revision, add a **new revision** on
  top of the current head that corrects the schema/data going forward. Do not
  edit the released revision's `upgrade()`/`downgrade()`.
- An **unreleased** revision (not yet in any release) *may* be edited, retargeted,
  or dropped — that is the only window in which the history is still soft.
- **Exception — a released revision that cannot complete may be edited in place.**
  When the revision's `upgrade()` raises, or its `downgrade()` errors, no forward
  revision can reach the failure: the operator never gets to the corrective head,
  or the broken code is what runs on the way down. Repair it in the revision
  itself. The repair must be **idempotent** for databases that already applied the
  revision successfully — it fills in what was missed and is a no-op otherwise —
  and the PR must state which failure it fixes and why a forward revision cannot.
  This exception covers *failures*, not defects: a revision that completes but
  writes wrong data, or produces a schema the project later regrets, is fixed
  forward like anything else.
- When correcting forward, keep the chain linear (see ADR 2): the corrective
  revision's `down_revision` is the current head, and it becomes the new head.

## Consequences

- Databases that already ran a revision and those that run it in future always
  reach the *same* schema state from it — the populations never diverge.
- Corrective work costs an extra revision rather than a one-line edit — the price
  of a consistent, replayable history. Reviewers must know the release boundary
  (not visible in the diff); when in doubt, treat it as released, then ask: *does
  the revision run at all?* If it does, fix forward; if it does not, in-place
  repair is the only option.

A change **violates** this decision when it:

- edits the `upgrade()` or `downgrade()` of a revision that has already shipped in
  a released version, instead of adding a new corrective revision — **unless** that
  revision cannot complete (its `upgrade()` raises or its `downgrade()` errors), in
  which case the in-place repair is correct and the PR states the failure and,
  where any database may already have run the revision successfully, the
  idempotency argument;
- reuses or repurposes a released revision's identifier for different DDL;
- "retcons" a change onto a version that is *already released* (retargeting is
  only legitimate while the change is still unreleased).

A reviewer should reject any change that mutates released migration history rather
than layering a new revision on top of the current head.

## Evidence

- #63899 — "Restore nullable ORM fields and drop unreleased corrective migration": the corrective migration was *unreleased*, so dropping it was legitimate.
- #62569 — "Retcon migration chain: move signed_url_template change to 3.1.8": retargeting while still unreleased, keeping the chain consistent.
- #63825 — "Do not backfill old DagRun.created_at": chose forward-only behaviour over rewriting historical rows.
- #66016 — "migrate existing deadline rows in migration 0080 upgrade and downgrade": the sanctioned exception. `0080` shipped in 3.1.0 and both directions raised `IntegrityError` on a non-empty `deadline` table, so neither could be reached by a forward revision; the fix backfills in place and is a no-op on clean databases. Also cited in ADR 0003.
- #65288 — "add missing `disable_sqlite_fkeys` to 0108 migration": the same shape, downgrade-only — the broken code runs on the way down, so a forward revision could not fix it.
