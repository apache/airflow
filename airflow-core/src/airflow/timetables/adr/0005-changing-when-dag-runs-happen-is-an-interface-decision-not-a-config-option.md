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

# 5. Changing when Dag runs happen is an interface decision, not a config option

Date: 2026-07-20

## Status

Accepted

## Context

This area attracts a recognisable proposal: a contributor hits a scheduling
behaviour they consider wrong — the extra run when unpausing a `catchup=False`
Dag, no way to express several cron expressions in one schedule, the fixed
structure of built-in timetables — and arrives with a working patch that adds a
new switch (a config option, or a new value on `catchup` / `schedule`). These are
refused or left to lapse not on their code or premise, but because nobody has
agreed what the interface should be.

The clearest instance: one PR added a config option to suppress the unpause run;
another separately changed the values `catchup` accepts for the same intent.
Review held that the two are one question about the heart of Airflow and belong
in a mailing-list discussion covering both, not two independent patches; the
second was closed after a year, consensus against a config option, the underlying
problem still unsolved. The shape repeats: multiple-cron-expression support was
attempted twice years apart without converging on how the schedule is *expressed*;
a composable-timetable proposal stalled on how a deserializer reaches an arbitrary
user-supplied strategy class (arbitrary code the scheduler invokes, needing a
registration and safe-guarding story first).

The cost of getting it wrong is high: schedule semantics are the contract every
Dag is written against and cannot be withdrawn once released, and a
*deployment-level* switch makes the same Dag file mean different things on
different installations, so a Dag is no longer portable. The existing
`scheduler.catchup_by_default` marks the boundary: it supplies the *default* for a
parameter the Dag declares and can override, so the Dag file stays authoritative.
A setting that changes what a Dag-declared parameter *means*, or produces
behaviour no Dag can opt out of, is the shape this refuses.

## Decision

A change to when Dag runs happen starts as an interface proposal, and the
behaviour is expressed in the schedule the Dag declares.

- **Take the interface to the dev list or an AIP before the code.** A change to
  catchup semantics, to the first run produced after unpausing, or to how a
  schedule is expressed is proposed and agreed before it is implemented. The PR
  links the discussion.
- **Do not add a configuration option that changes when Dag runs fire.** Schedule
  behaviour belongs to the Dag, not to the deployment. Express it as a timetable,
  or as a value of the parameter the Dag already declares, so it travels with the
  Dag file. Supplying the *default* for a Dag-declared parameter — as
  `scheduler.catchup_by_default` does — is the one accepted shape, because the Dag
  can still override it.
- **Settle adjacent proposals together.** When another open PR changes a
  neighbouring part of the same semantics, the two are one interface question and
  are resolved as one, rather than merged independently.
- **A new composition point is designed with its resolution story.** Any
  user-supplied object a timetable composes is arbitrary code the scheduler
  invokes; state how it is registered, how `deserialize()` reaches the class, and
  what constrains it — before proposing the composition.
- **Own the proposal through the discussion.** Interest expressed on a thread is
  not a design; the contributor drives the interface question to a conclusion,
  rebasing and answering review, or the proposal lapses.

## Consequences

- Real, widely-felt problems stay unfixed for long periods (the unpause run was
  patched twice and left open over a year) rather than shipping two
  half-interfaces for one behaviour.
- Contributors sometimes arrive with complete code and find the conversation is
  whether the feature should exist in that shape; this document moves that
  conversation earlier.
- Refusing deployment-level switches keeps Dag files portable and the set of
  supported behaviours bounded — at the price of no quick escape hatch.
- Major-version boundaries become the natural moment to correct semantics.

A change **violates** this decision when it:

- adds a configuration option, or a new accepted value of `catchup` / `schedule`,
  that changes when Dag runs fire, without a linked prior dev-list or AIP
  discussion — a config supplying the default of a Dag-overridable parameter
  excepted;
- changes catchup behaviour or the first run produced after unpausing inside a
  pull request rather than in a prior discussion;
- lands one half of a semantics change whose other half is in another open PR;
- introduces a timetable composition point accepting a user-supplied object
  without stating how that object is registered and reconstructed on
  deserialization;
- proposes a schedule-expression feature with no owner driving the interface
  question, relying on accumulated user support in the thread.

A reviewer should ask: after this change, does the same Dag file still schedule
identically on every deployment, and where was the interface agreed?

## Evidence

- #38168 — config to avoid the unpause run with `catchup=False`: closed; review
  held it and #35392 are one interface question for a mailing-list discussion,
  not two standalone patches.
- #35392 — a `catchup` value disabling catchup for the first run only: closed
  after a year, consensus against a config option, problem acknowledged unresolved.
- #24733, #35337 — two attempts years apart at multiple cron expressions; both
  lapsed without converging on how the schedule is expressed, and review asked
  contributors to open PRs rather than post "+1" support.
- #28757 — composable timetables: lapsed on how a deserializer reaches an
  arbitrary user-supplied strategy class (arbitrary scheduler-invoked code needing
  registration and safe-guarding).
- #25434 — cron timetable starting its run at the interval start: closed by its
  author for an in-flight alternative rather than adding a second expression.
