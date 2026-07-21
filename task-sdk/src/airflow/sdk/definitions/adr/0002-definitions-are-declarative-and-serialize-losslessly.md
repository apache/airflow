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
author's module once and produces a *serialized* Dag; from then on the scheduler
and workers act on that form. That only works if the authoring objects here are
*declarative* — intent captured as **data that survives serialization**, not
behaviour that exists only while the module is imported.

The mechanism is a curated set of serialized fields.
`DAG._DAG__serialized_fields` is the class's `attrs` fields *minus* a
hand-maintained runtime-only exclusion list; `BaseOperator.get_serialized_fields()`
is a throwaway instance's `vars()` *minus and plus* curated sets, and is exactly
the set the airflow-core serializer reads. A field not in that set is invisible to
serialization: the author sets it, it is dropped on the way out, and the object is
reconstructed with that attribute at its default — silently, on every scheduler
and worker. The second half of the contract: these SDK definitions have
airflow-core serialization counterparts that must stay field-for-field in parity
(see `serialization/adr/0003`, enforced by a `check-...-in-sync` prek hook reading
the class body). A field declared only in `__init__`, or assembled dynamically, is
invisible to the hook — while the constructor accepting it makes the change *look
complete* and pass an in-process unit test.

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
  restart, instead of reverting to a default somewhere invisible.
- Adding a field is a constructor change *plus* a serialized-set change *plus* a
  parity check *plus* a round-trip test.
- The serialized shape is itself a compatibility surface — a released field cannot
  be ret-conned, so shape changes are version-gated.

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

- #68583 — `bundle_name` on the serialized dag; the point is threading it into the
  serialized shape, not just the in-memory object.
- #51494 — `DeadlineAlert` serialization fixed *and* pinned with round-trip tests
  — exactly this failure mode.
- #58992, #55538 — moving the serialize contract to the SDK side, keeping parity a
  maintainable invariant.
- #69311 — `FixedKeyMapper` crash; a reconstructed object must carry the fields
  its constructor expects.
