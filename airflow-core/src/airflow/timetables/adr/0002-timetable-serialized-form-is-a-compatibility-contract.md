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

# 2. A timetable's serialized form is a backward-compatibility contract

Date: 2026-07-19

## Status

Accepted

## Context

A Dag's schedule is not stored as live Python. When a Dag is serialized, its
timetable is asked to `serialize()` itself to a JSON-serializable dict, and the
timetable class is recorded by classpath. When the serialized Dag is read back —
by the scheduler on every tick, or by any component that reconstructs the Dag —
the class is looked up and `deserialize(data)` rebuilds the timetable from that
dict. Custom (user-authored) timetables participate in exactly the same
mechanism: they are registered and resolved by import path.

This makes the serialized form an *interface*, and interfaces across a rolling
deploy are versioned by reality rather than by choice. During an upgrade,
different components run different Airflow versions at the same moment: a Dag
serialized by a *newer* processor may be deserialized by an *older* scheduler,
and vice versa. If `serialize()` drops a field the timetable needs, or a key is
silently renamed or changes type, or a timetable class is moved without an
alias, the reconstructed timetable is wrong — and the failure surfaces as
mis-scheduled runs after the deploy, not as an error at the change site.

The history here is a run of exactly these misses: a description that was not
persisted and so was lost on round-trip; a datetime serialized without an
explicit isoformat separator; and a schedule class whose serialized shape had to
be made compatible across minor releases so already-serialized Dags kept
working. Each was cheap to write and expensive to discover.

## Decision

A timetable's serialized form is a **backward-compatibility contract**, and
changes to it must preserve round-trip fidelity across versions.

- `serialize()` must emit **every field the timetable needs** to reconstruct its
  scheduling behaviour, and `deserialize()` must restore them. A field that
  affects when runs happen or what interval they cover cannot live only on the
  in-memory object.
- The serialized payload must be **JSON-serializable and stable**: explicit,
  unambiguous encodings (e.g. an explicit isoformat separator for datetimes), no
  reliance on a default `repr`, and no dependence on dict/set ordering.
- An **older reader must still deserialize a newer payload**. Evolve the shape
  *additively* — add a new optional key with a default, version-gate, or provide
  a shim — never rename, retype, or drop an existing key in place.
- A timetable class is resolved **by classpath**, so it must not be moved or
  renamed without a compatibility alias; the same applies to keeping a core
  timetable and its SDK counterpart in sync.
- The round-trip must be covered by a test that **serializes then deserializes**
  and asserts the reconstructed timetable schedules identically — a test that
  **fails without the change** when the field or compatibility path is missing.

## Consequences

- A Dag's schedule survives serialization and rolling upgrades unchanged; the
  scheduler reconstructs the same timetable the author declared.
- Contributors adding state to a timetable must extend `serialize()` /
  `deserialize()` together and provide a compatibility path for already-stored
  Dags, rather than assuming a fresh re-serialize everywhere.
- Custom timetables remain resolvable across releases because their classpath
  and serialized keys are treated as public surface.

A change **violates** this decision when it:

- adds a scheduling-relevant attribute to a timetable but does not persist and
  restore it in `serialize()` / `deserialize()`;
- renames, retypes, or removes an existing serialized key in place, so an older
  scheduler can no longer read a payload a newer one wrote (or vice versa);
- moves or renames a timetable class without a compatibility alias, breaking
  classpath lookup for already-serialized Dags;
- serializes a value through an unstable or ambiguous encoding (default `repr`,
  implicit datetime formatting, order-dependent output);
- lands such a change without a serialize → deserialize round-trip test that
  fails when the field or compat path is reverted.

## Evidence

- #51203 — "Persist EventsTimetable's description during serialization": the
  description was dropped on round-trip because `serialize()` did not emit it;
  fixed by persisting it so `deserialize()` restores the same timetable.
- #48732 — "Set explicit separator for isoformat when serializing
  EventsTimetable": pins an explicit, unambiguous datetime encoding in the
  serialized form rather than relying on an implicit default.
- #49350 — "Make `DatasetOrTimeSchedule` compatible with Airflow 2.10.x" and
  #48097 — "Add backwards compatibility for `DatasetOrTimeSchedule`": make the
  serialized schedule shape readable across releases so already-serialized Dags
  keep scheduling after an upgrade.
