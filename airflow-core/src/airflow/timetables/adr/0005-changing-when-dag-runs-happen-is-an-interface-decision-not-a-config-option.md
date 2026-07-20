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

This area attracts a recognisable class of proposal: the contributor has hit a
scheduling behaviour they consider wrong — the extra run produced when unpausing
a Dag with `catchup=False`, the inability to express several cron expressions in
one schedule, the fixed structure of the built-in timetables — and arrives with a
working patch. Almost always the patch introduces a new switch: a configuration
option, or a new accepted value on `catchup` or `schedule`.

These proposals are not refused on their code, and usually not on their premise
either. They are refused, or left to lapse, because nobody has agreed what the
interface should be.

The clearest instance is the unpause-with-`catchup=False` behaviour. One PR added
a configuration option to suppress the immediate run; another, opened separately,
changed the set of values `catchup` accepts to express the same intent. Review's
answer to the first was not about its implementation: this is the heart of
Airflow, the two proposals are parts of one question, and the interface should be
settled in a mailing-list discussion covering both rather than assembled from two
independent patches. The second was eventually closed after a year of inactivity
with the observation that consensus was against adding a config option for it
specifically — and with the acknowledgement, from a participant, that the
underlying problem remained unsolved. Neither PR was wrong about the problem;
both stalled on the absence of an agreed interface.

The same shape repeats. Support for multiple cron expressions in one schedule was
attempted twice, years apart, and accumulated user demand in both threads without
either converging on how the schedule should be *expressed* — accompanied by a
reminder from a maintainer that "+1" comments do not move a feature forward. A
composable-timetable proposal, which would have let users assemble a schedule from
strategy objects, ran into the question of how a deserializer reaches an arbitrary
user-supplied strategy class: like a custom timetable, it is arbitrary code the
scheduler invokes, so it needs a registration and safe-guarding story before it
can be code at all.

Two properties of this area make the cost of getting it wrong unusually high.
Schedule semantics are the contract every Dag in every deployment is written
against, and a released behaviour cannot be withdrawn. And a *deployment-level*
switch is worse than a Dag-level one: it makes the same Dag file mean different
things on different installations, so a Dag is no longer portable and support
questions can no longer be answered from the Dag alone.

The existing `scheduler.catchup_by_default` option marks the boundary rather than
contradicting it. It supplies the default for a parameter the Dag declares and
can override, so the Dag file remains the authority on its own schedule. A
setting that changes what a Dag-declared parameter *means*, or that produces
behaviour no Dag can express or opt out of, is the shape this decision refuses.

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

- Real and widely-felt problems stay unfixed for long periods. The extra run on
  unpause was reported, patched twice, and left open for over a year. The project
  accepts this cost rather than shipping two half-interfaces for one behaviour.
- Contributors sometimes arrive with complete, working code and find the
  conversation is about whether the feature should exist in that shape. Saying so
  in this document is meant to move that conversation earlier, before the
  implementation effort is spent.
- Refusing deployment-level scheduling switches keeps Dag files portable and
  keeps the number of behaviours the project must keep working bounded — at the
  price of having no quick escape hatch for a deployment that dislikes a default.
- Major-version boundaries become the natural moment to correct scheduling
  semantics, which concentrates this class of change into a few releases rather
  than spreading it across many.

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

- #38168 — "Add config to avoid one Dag run when unpausing a Dag with
  `catchup=False`": closed unmerged. Review's position was that this and #35392
  are one question about the heart of Airflow's scheduling and belong in a
  mailing-list discussion that arrives at the interface, not two standalone
  patches.
- #35392 — a proposal to add a `catchup` value that disables catchup for the
  first Dag run only: closed after a year of inactivity, with the stated
  consensus being against a new config option for this, and with the underlying
  problem explicitly acknowledged as still unresolved.
- #24733 and #35337 — two independent attempts, years apart, to support multiple
  cron expressions in a schedule. Both lapsed; the threads accumulated user
  support without converging on how the schedule should be expressed, and review
  asked contributors to open their own pull requests rather than post support.
- #28757 — "Initial draft of composable timetables": lapsed on the unresolved
  question of how a deserializer reaches an arbitrary user-supplied strategy
  class, which — like a custom timetable — is arbitrary code invoked in the
  scheduler and needs registration and safe-guarding.
- #25434 — an attempt to make a cron timetable start its Dag run at the interval
  start: closed by its author once review pointed at an in-flight alternative and
  a separate proposal addressing the same underlying need, rather than adding a
  second way to express it.
