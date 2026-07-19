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
Deciding whether such a condition is satisfied is a scheduling decision, and by
the sibling ADR *the scheduler never runs user code* (`../../jobs/adr/0001`):
everything the scheduler evaluates must come from the *serialized*
representation the Dag processor persisted, never from re-imported author
modules or live SDK objects.

To make this enforceable, the asset classes are deliberately split in two. The
authoring-side classes live in the Task SDK (`airflow.sdk.definitions.asset` —
`Asset`, `AssetAlias`, `AssetAny`, `AssetAll`, `AssetRef`), and a parallel set
of *serialized*, data-only classes lives in
`airflow.serialization.definitions.assets` (`SerializedAsset`,
`SerializedAssetRef`, `SerializedAssetAlias`, `SerializedAssetBooleanCondition`,
`SerializedAssetUniqueKey`). `AssetEvaluator` (`evaluation.py`) is a
`functools.singledispatch` over the *serialized* hierarchy only: it walks the
condition tree, resolves refs and aliases against the database, and aggregates
per-asset boolean statuses (`AssetAll` → `all`, `AssetAny` → `any`). It never
receives, imports, or executes an SDK `Asset` object or a user callable.

This split matters because an asset expression *looks* like ordinary Python when
authored (`&`, `|`, custom subclasses), and it is locally tempting to evaluate
the authored objects directly — the scheduler "already has them". Doing so would
both re-introduce user code into the scheduler loop and desynchronise the two
class hierarchies, so a serialized payload could no longer round-trip through
evaluation. The condition must instead be lowered to serialized data at parse
time and evaluated purely as data.

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

- #58993 — "Split SDK and serialized asset classes": establishes the two
  separate hierarchies this decision depends on, so the scheduler evaluates
  serialized data rather than SDK authoring objects.
- #48565 — "Dynamically create assets if referenced by alias": alias resolution
  during evaluation is a database/data operation over serialized structures, not
  a call back into author code.
- #67725 — "Type asset_expression in the REST API so the UI does not cast
  through unknown": treats the asset expression as typed serialized data end to
  end rather than an opaque object.
