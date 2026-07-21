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

A Dag's schedule is not stored as live Python. On serialization a timetable
`serialize()`s itself to a JSON dict and records its class by classpath; on read
back the class is looked up and `deserialize(data)` rebuilds it. Custom
(user-authored) timetables use exactly the same mechanism, resolved by import
path.

This makes the serialized form an *interface* versioned by reality across a
rolling deploy: a Dag serialized by a *newer* processor may be deserialized by an
*older* scheduler, and vice versa. If `serialize()` drops a needed field, a key
is renamed or retyped, or a class is moved without an alias, the reconstructed
timetable is wrong — surfacing as mis-scheduled runs after the deploy, not as an
error at the change site. The history is exactly these misses: a description lost
on round-trip, a datetime serialized without an explicit isoformat separator, a
schedule class whose shape had to be made compatible across minor releases.

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

- A Dag's schedule survives serialization and rolling upgrades unchanged.
- Adding state to a timetable means extending `serialize()` / `deserialize()`
  together with a compatibility path for already-stored Dags.
- Custom timetables stay resolvable across releases because their classpath and
  serialized keys are public surface.

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

- #51203 — `EventsTimetable` description dropped on round-trip; `serialize()`
  did not emit it.
- #48732 — pins an explicit isoformat separator rather than an implicit default.
- #49350, #48097 — make the `DatasetOrTimeSchedule` serialized shape readable
  across releases so already-serialized Dags keep scheduling after an upgrade.
