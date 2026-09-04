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

# 2. Asset-driven scheduling is evaluated from serialized, data-only structures

Date: 2026-07-19

## Status

Accepted

## Context

A Dag can be scheduled by an *asset expression* — a boolean condition over
assets, aliases, and references, e.g. `schedule=(asset_a & asset_b) | asset_c`.
Evaluating that condition is a scheduling decision, and by the sibling ADR *the
scheduler never runs user code* (`../../jobs/adr/0001`): everything the scheduler
evaluates comes from the *serialized* representation the Dag processor persisted,
never re-imported author modules or live SDK objects.

To enforce this, the asset classes are split in two: authoring-side classes in
the Task SDK (`airflow.sdk.definitions.asset`) and parallel *serialized*,
data-only classes in `airflow.serialization.definitions.assets`
(`SerializedAsset`, `SerializedAssetRef`, `SerializedAssetAlias`,
`SerializedAssetBooleanCondition`, `SerializedAssetUniqueKey`). `AssetEvaluator`
(`evaluation.py`) is a `functools.singledispatch` over the *serialized* hierarchy
only: it walks the tree, resolves refs/aliases against the database, and
aggregates per-asset booleans — never receiving, importing, or executing an SDK
`Asset` or user callable. The expression *looks* like ordinary Python when
authored, so evaluating the authored objects directly is tempting, but that would
re-introduce user code into the loop and desync the two hierarchies.

## Decision

Asset conditions the scheduler evaluates must be **serialized, data-only
structures** produced by the Dag processor — never authoring-side SDK objects
and never user callables.

- `AssetEvaluator` dispatches over the `Serialized*` asset hierarchy. New
  condition or reference kinds are added by extending that serialized hierarchy
  and registering an evaluator branch — not by evaluating an SDK class.
- The SDK asset classes and the serialized asset classes stay **separate**. The
  evaluator / manager must not import the SDK authoring classes, and the SDK
  must not import the core ORM/serialized classes; the boundary is the
  serialize/deserialize step.
- Resolving a `Serialized*` ref or alias to concrete assets is a **data lookup
  against the database** (`resolve_ref_to_asset`, `expand_alias_to_assets`),
  yielding more serialized structures — it never dereferences back into live
  author code.
- Any new asset-scheduling flexibility must express author intent as serialized
  data. If a feature seems to need a user callable at scheduling time, it
  belongs in an isolated component (worker / triggerer), consistent with
  `../../jobs/adr/0001` and the serialization ADRs.

## Consequences

- The scheduler stays trustworthy and fast when evaluating asset schedules: a
  hostile or buggy asset expression cannot execute author code in the control
  loop.
- The serialized asset classes are the single source of truth for evaluation, so
  a persisted condition round-trips deterministically (reinforcing the
  serialization determinism ADR).
- New asset-condition features cost a serialized-schema addition plus an
  evaluator branch, rather than "just call the user's object".

A change **violates** this decision when it:

- makes `AssetEvaluator` (or any scheduler-reachable asset code) accept, import,
  or dispatch on an SDK `Asset` / `AssetAlias` / `AssetAny` / `AssetAll` object
  instead of its `Serialized*` counterpart;
- evaluates a user-supplied callable or re-imported author module to decide
  whether an asset condition is satisfied;
- collapses the SDK and serialized asset class hierarchies into one, so that
  authoring objects leak into the serialized/evaluation path (or ORM models leak
  back into the SDK);
- resolves a ref/alias by dereferencing author code rather than by a database
  lookup returning serialized structures.

## Evidence

- #58993 — establishes the two separate SDK/serialized hierarchies this decision depends on.
- #48565 — alias resolution during evaluation is a data operation over serialized structures.
- #67725 — types `asset_expression` in the REST API as serialized data end to end.
