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

# 5. New serialized Dag state must not duplicate an existing concept

Date: 2026-07-20

## Status

Accepted

## Context

Adding a field to the serialized Dag is close to irreversible. It has to be
optional and ignorable so older readers survive it (ADR 0001), it has to stay in
parity between core and the Task SDK (ADR 0003), it becomes part of the blob every
deployment persists and every adjacent-version component parses, and once Dag
authors write it into their Dags it cannot be withdrawn without a deprecation
cycle measured in releases. The write is cheap; the commitment is not.

That asymmetry is why the review question is not "is this field useful?" but "does
the model already express this?" — and often it does, under a name the proposer did
not recognise.

The clearest case is a user-supplied "processing date" for a Dag run. It is a real
need, it has an obvious API, and it is refused, because the data interval already
*is* that concept: a run that processes one day's data has a half-open interval
whose start and end bound that day, and where there is no interval the two are set
to the same instant. The interval is produced by the timetable, which is the
extension point for exactly this. A parallel field would have given two answers to
"what is this run processing", one of them unknown to every timetable, every
scheduler decision and every existing Dag.

The second shape is a new *layer* rather than a new field: configuration that can
be set in three places with a precedence order between them, over state that is
already serialized per Dag and per run. Each level is individually reasonable and
the combination is not — the interaction with clearing a run, with which Dag
version a task actually executes against, and with the race between two sources of
the same version, could not be settled in review. The PR was closed by its author
in favour of a simplified one, which is the outcome this decision aims at reaching
before the code is written rather than after.

The third is simple duplication: the same attribute proposed twice by two
contributors within days, because neither checked for in-flight work on a
serialized-Dag attribute that had already been designed elsewhere.

## Decision

**A new serialized attribute must be shown not to restate something the model
already carries.**

- **Name the existing concepts you considered and why each is insufficient** —
  `data_interval_start` / `data_interval_end`, `logical_date`, `run_type`, the
  timetable, `bundle_version` / `DagVersion`, `params`. Reviewers will ask; answer in
  the PR description.
- **Extend the existing extension point instead of adding a field beside it.** Where
  a timetable, a params definition, or a bundle already produces the value, that is
  where a new behaviour belongs.
- **A precedence hierarchy over Dag-version resolution must name its authoritative
  source and define the clear/re-run behaviour.** Configuration hierarchies are not
  forbidden — the merged `rerun_with_latest_version` change (#63884, cited
  approvingly in ADR 0001) is exactly such a hierarchy and is the shape that
  survived. What #61448 failed on was narrower and specific: it left unresolved
  which of `DagModel.bundle_version` and `DagBundleModel.version` wins and how they
  race, and it did not define what clearing or re-running a run does under the new
  precedence. Answer those two questions in the PR and the hierarchy is reviewable.
- **State what happens on clear, on re-run, and on a run pinned to an older Dag
  version.** A new attribute that is read at execution time must have a defined
  answer for a run resolved to a version that predates it.

## Consequences

- The serialized document stays a description of the Dag rather than an
  accumulation of near-synonyms, and readers on adjacent versions have fewer fields
  whose meaning they must agree on.
- Genuinely new concepts still land — the requirement is an argument, not a veto.
- The cost is borne by contributors solving a real problem who discover the answer
  is an existing field used differently, or a timetable rather than an attribute.
  That redirection is slower than merging the field, and cheaper than living with two
  spellings of one concept in a format that cannot be edited retroactively.

A change **violates** this decision when it:

- adds a serialized Dag or Dag-run attribute that restates the data interval, the
  logical date, the run type, or the resolved Dag version;
- introduces a second source of truth for a value a timetable, params definition, or
  bundle already produces;
- adds a precedence hierarchy over which Dag version a run executes against without
  naming the authoritative version source and defining the clear/re-run behaviour;
- leaves undefined what the new attribute means for a run pinned to a Dag version
  serialized before the attribute existed;
- duplicates an attribute already under development in another open PR. This
  bullet is **not evaluable from the PR alone** — it requires the open-PR queue as
  an input, and may only be raised after a scan of that queue has actually found
  the overlapping PR, which the finding must name.

## Evidence

This ADR is authoritative for the **Dag-version resolution** question (#61448).
The overlapping question of whether a *core model column* duplicates an existing
concept (#67329) is decided in `../../models/adr/0005`; it is summarised below for
context but judged there, so one PR does not trip two areas.

- #61448 — "Add three-level `run_on_latest_version` configuration hierarchy":
  discussion turned on the interaction with clearing a run, on `DagModel.bundle_version`
  versus `DagBundleModel.version` and the race between them, and on which serialized
  Dag code tasks actually execute; closed by the author in favour of a simplified
  proposal. Note that a configuration hierarchy for the adjacent rerun behaviour
  (#63884) did merge — the objection was to the unresolved version race, not to
  hierarchies.
- #67329 — "Add `target_date` — a user-defined processing date for Dag runs":
  declined because `data_interval_start` / `data_interval_end` already carry that
  meaning. Decided in `../../models/adr/0005`.
- #61063 — "Add configurable bundle version defaults": review required it be split
  into the configurable default and the operator change, and flagged that it depended
  on an execution-API endpoint that does not exist; did not land in that form.
- #65595 — adding a `team_name` attribute to the Dag for multi-team context:
  closed as duplicating #65617, which was already designing the same serialized
  attribute.
