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

# 3. Core and Task-SDK field definitions stay in parity (enforced by prek)

Date: 2026-07-18

## Status

Accepted

## Context

Several serializable objects are defined **twice**: once in the Task SDK
(`airflow.sdk`, the surface Dag authors use) and once in `airflow-core` (the
side that serializes, stores, and reconstructs them). The two definitions
describe the same object and must expose the same set of fields. If they drift —
the SDK grows a field the core class does not know about, or the two disagree on
a field's name — an object authored against one side round-trips incorrectly
through the other: attributes silently vanish on serialize, or reconstruction
falls back to constructor defaults on deserialize.

To catch this mechanically, a `check-...-in-sync` prek hook compares the two
class bodies field-for-field. The hook only works if the fields are actually
*visible in the class body* — declared as class-level attributes it can read.
Definitions that hide fields (assembling them dynamically, pulling them from a
computed list, or setting them only inside `__init__`) are invisible to the
hook, which then reports parity while the objects have in fact diverged.

Separately, `serialize()` and the constructor are two ends of the same
round-trip: `serialize()` must emit **every** field the constructor consumes.
If `serialize()` omits a field that `__init__` needs, deserialization silently
reconstructs the object with that field at its default — so the value the author
set is lost after a scheduler/worker restart, with no error.

## Decision

Keep the core and Task-SDK definitions of a shared serializable object in
parity, and keep the parity machine-checkable.

- Declare fields in the **class body** as class-level attributes, so the
  `check-...-in-sync` prek hook can see them and enforce field-for-field parity
  between the SDK class and its core counterpart. Do not hollow the class body
  by computing the field set dynamically or defining fields only in `__init__`.
- `serialize()` must emit **every field the constructor consumes**, so the
  round-trip `serialize → deserialize` reconstructs the object with all its
  authored values intact.

## Consequences

- The prek hook fails the build the moment the two definitions drift, so parity
  is caught at commit time rather than as a silent runtime data-loss bug.
- Objects survive the full serialize/deserialize round-trip — and therefore a
  component restart — with every constructor-supplied value preserved.
- Contributors adding a field to one side are forced to add it to the other and
  to include it in `serialize()`.

A **violating change** looks like any of:

- **Hollowing the class body** to dodge the sync hook — moving field
  declarations out of the class body (into `__init__`, a dynamic list, or a
  computed property) so the hook can no longer see them and reports parity while
  the two sides have actually diverged.
- Adding a field to the SDK class (or the core class) without the matching
  declaration on the other side.
- A `serialize()` that **omits a field the constructor consumes**, so the object
  deserializes with that field reverted to its default and the authored value is
  lost after restart.

## Evidence

- #69311 — fixes an asset-event ingestion crash for Dags using
  `FixedKeyMapper`, a case where the serialized/reconstructed object must carry
  the same fields the constructor expects.
- #66990 — includes `dataset_id`, `table_id`, and `poll_interval` in
  `BigQueryIntervalCheckTrigger` serialization so `serialize()` emits every
  field the constructor consumes and the trigger reconstructs faithfully.
