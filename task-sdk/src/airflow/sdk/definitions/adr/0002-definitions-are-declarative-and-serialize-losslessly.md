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

# 2. Definitions are declarative and must serialize losslessly

Date: 2026-07-19

## Status

Accepted

## Context

The scheduler **never runs a Dag file**. The Dag File Processor imports the
author's module once, and from the resulting in-memory objects it produces a
*serialized* Dag; from then on the scheduler and workers act on that serialized
form. That only works if the authoring objects in this package are *declarative*
— an author's intent has to be captured as **data that survives serialization**,
not as behaviour that only exists while the Dag module is imported.

The mechanism is a curated set of serialized fields. `DAG._DAG__serialized_fields`
is derived as the class's `attrs` fields *minus* a hand-maintained exclusion list
of runtime-only attributes; `BaseOperator.get_serialized_fields()` is derived
from a throwaway instance's `vars()` *minus and plus* curated sets.
`get_serialized_fields()` is exactly the set the airflow-core serializer reads.
So a field that is not in that set is invisible to serialization: the author sets
it, it is dropped on the way out, and on the other side the object is
reconstructed with that attribute at its default — silently, with no error, on
every scheduler and worker.

There is a second half to the same contract: these SDK definitions have
serialization counterparts in airflow-core, and the two must stay field-for-field
in parity (see `serialization/adr/0003`, enforced by a `check-...-in-sync` prek
hook that reads the class body). A field declared only inside `__init__`, or
assembled dynamically, is invisible both to the hook and, in effect, to anyone
reasoning about what serializes. The recurring pressure is that adding an
attribute to `DAG` or `BaseOperator` *looks complete* the moment the constructor
accepts it — the object works in-process in a unit test — while the field that
was never added to the serialized set quietly fails to reach production.

## Decision

Author intent is captured as serializable data, and every definition field that
carries author intent must round-trip losslessly. Concretely:

- **A new field on a definition is threaded into its serialized-field set** —
  added to `DAG._DAG__serialized_fields` / `BaseOperator.get_serialized_fields()`,
  or explicitly added to the exclusion list with a reason if it is genuinely
  runtime-only.
- **The SDK definition stays in parity with its airflow-core serialization
  counterpart**, with fields declared in the *class body* so the sync hook can see
  them — not hidden in `__init__` or a computed list.
- **Behaviour that must reach the scheduler/worker is expressed as serialized
  state**, not as logic that only runs while the Dag file is imported.
- **The round-trip is proven by a test** — construct, serialize, deserialize,
  assert the authored value survived.

## Consequences

- Authored values survive the DFP → serialized Dag → scheduler/worker path and a
  component restart, instead of reverting to a default somewhere invisible.
- Adding a field is more than a constructor change: it is a constructor change
  *plus* a serialized-set change *plus* a parity check *plus* a round-trip test.
- The serialized shape is itself a compatibility surface — a released serialized
  field cannot be ret-conned, so shape changes are version-gated.

A change **violates** this decision when it:

- adds a field that carries author intent to `DAG` / `BaseOperator` (or another
  definition) without adding it to the serialized-field set — so it round-trips to
  a default and never reaches the scheduler/worker;
- hides a field from serialization or the parity hook by defining it only in
  `__init__` or a dynamically-computed list instead of the class body;
- lets the SDK definition and its airflow-core serialization counterpart diverge
  in the set of fields they carry;
- relies on import-time behaviour to convey intent the scheduler needs, instead of
  serialized state, so the intent is lost once the Dag file is no longer imported;
- changes the serialized shape of a released definition without version-gating it.

## Evidence

- #68583 — "Add `bundle_name` to serialized dag": a new definition field is only
  useful once it is carried in the serialized Dag — the whole point of the change
  is threading it into the serialized shape, not just the in-memory object.
- #51494 — "Fix serialization of `DeadlineAlert` and add unit tests to prevent
  regression": a definition object that was not round-tripping correctly, fixed
  *and* pinned with round-trip tests — exactly the failure mode this decision
  guards against.
- #58992 — "Move Serialization/Deserialization (serde) to task SDK" and #55538 —
  "Remove SDK dependency from SerializedDAG": the moves that put the serialize
  contract on the SDK side and keep the SDK/core split clean, so parity is a
  maintainable invariant rather than an accident.
- #69311 — "Fix asset event ingestion crash for Dags using `FixedKeyMapper`": a
  reconstructed object must carry the same fields its constructor expects, or it
  breaks on the consuming side.
