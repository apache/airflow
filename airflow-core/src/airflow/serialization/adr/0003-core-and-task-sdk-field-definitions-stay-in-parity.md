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
(`airflow.sdk`, the authoring surface) and once in `airflow-core` (which serializes,
stores, and reconstructs them). The two must expose the same fields; if they drift,
an object round-trips incorrectly — attributes silently vanish on serialize, or
reconstruction falls back to constructor defaults on deserialize.

A `check-...-in-sync` prek hook compares the two class bodies field-for-field, but
only works if the fields are *visible in the class body* as class-level attributes.
Definitions that hide fields (assembled dynamically, from a computed list, or set
only in `__init__`) are invisible, so the hook reports parity while the objects have
diverged. Separately, `serialize()` must emit **every** field the constructor
consumes, or deserialization silently reconstructs with that field at its default
and the author's value is lost after a restart, with no error.

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

- The prek hook fails the build the moment the definitions drift, catching it at
  commit time rather than as a silent runtime data-loss bug.
- Objects survive the full round-trip — and a component restart — with every
  constructor-supplied value preserved.
- Contributors adding a field to one side are forced to add it to the other and to
  `serialize()`.

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

The primary evidence is the enforcement itself — three prek hooks in
`.pre-commit-config.yaml` for no other purpose, each pinned to the pair of files it
compares:

- `check-partition-mapper-defaults-in-sync` —
  `airflow-core/src/airflow/partition_mappers/{temporal,window,fixed_key}.py`
  against `task-sdk/src/airflow/sdk/definitions/partition_mappers/`.
- `check-window-in-sync` — the `Window` definitions on both sides.
- `check-template-context-variable-in-sync` — `models/taskinstance.py` against
  `sdk/definitions/context.py` and the templates reference.

Each hook reads class bodies, which is why fields must be declared there: hollowing
the class body makes the hook blind rather than failing it.

- #69311 — fixes an asset-event ingestion crash for Dags using `FixedKeyMapper`,
  where the reconstructed object must carry the fields the constructor expects. The
  only PR citation; the mechanism, not review history, is what this rests on.
