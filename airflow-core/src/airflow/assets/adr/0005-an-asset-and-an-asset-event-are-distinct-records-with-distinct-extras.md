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

# 5. An asset and an asset event are distinct records with distinct extras

Date: 2026-07-20

## Status

Accepted

## Context

An `Asset` is a **definition**: a stable, normalised identity (`name`, `uri`,
`group`) declared in a Dag file, whose `extra` describes the asset and changes
only when an author edits the Dag. An `AssetEvent` is an **occurrence**: a record
that a task updated that asset at a point in time, whose own `extra` describes
*that update*, carried from the task's outlet at run time alongside the producing
task instance.

The two are adjacent in code and API and both called `extra`, so conflating them
is the most common substantive error here — and types don't catch it, since both
are JSON blobs. A change to "provide the correct extra" to `on_asset_changed` was
rejected on this ground: the event's extra is not the asset's, and swapping them
changes what every listener receives. The real need was a *different* event-level
listener carrying the producing task instance, which is the shape that landed. The
same distinction governs aliases (the asset's own `extra` must survive resolution
through an `AssetAlias`) and partitions (`partition_key` is provenance on the
event, not folded into asset identity). Getting it wrong is costly because
listener payloads and event extras are consumed by user code and tooling outside
this repository — and since both fields are untyped JSON, the break surfaces as a
`KeyError` in someone else's plugin, not a failing test here.

## Decision

Asset-level and event-level data are kept distinct, and the boundary between them
is treated as a compatibility surface. Concretely:

- **`Asset.extra` describes the asset; `AssetEvent.extra` describes the update.**
  Neither is substituted for the other, and a change must say which one it is
  altering.
- **Event-level information reaches consumers through event-level surfaces.** If a
  listener needs the producing task instance, the partition key, or the update
  payload, that is an asset-event listener or an event field — not a redefinition
  of an asset-level one.
- **A new need for event data is a new listener or a new field, not a changed
  meaning.** Changing what an existing listener receives breaks consumers outside
  this repository.
- **Asset identity survives indirection.** Resolution through an `AssetAlias` must
  preserve the asset's own `extra` and identity; the alias is a lookup, not a
  transformation.
- **Provenance rides on the event.** Partition and lineage metadata attaches to
  the asset event and inherits to downstream events, without becoming part of the
  asset's normalised identity (`adr/0001`).

## Consequences

- Listener/event surfaces accrete rather than change: more narrowly-scoped
  listeners instead of richer payloads on existing ones — a deliberate trade
  against breaking external consumers.
- The two `extra` fields are indistinguishable at a glance, so this is a common
  first correction; being conceptual, it cannot be a lint rule.
- Reviewers must check each field's data-flow direction (definition- vs run-time).
- Keeping provenance off asset identity costs some queries a join to the event;
  identity stability is judged worth it.

A change **violates** this decision when it:

- passes `Asset.extra` where `AssetEvent.extra` is expected, or the reverse;
- alters the payload an existing asset listener receives in order to expose
  event-level data, instead of adding an event-level listener or field;
- folds run-time provenance (`partition_key`, producing task instance, update
  payload) into the asset definition or its normalised identity;
- drops or rewrites an asset's `extra` when resolving through an `AssetAlias`;
- changes an event or listener field's meaning without treating external
  consumers as a compatibility surface.

A reviewer should ask, of every field in an asset-related change: is this a
property of the asset, or of one update to it — and who outside this repository
reads it?

## Evidence

- #54957 — closed unmerged; review stated the event extra is not the asset extra; real need was an event-level listener with the producing TI.
- #55115 — follow-up attempt at that event-level listener, also closed unmerged.
- #61718 — merged shape of the same need: a distinct event-level listener.
- #58038 — merged (backported #58712): preserve `Asset.extra` through alias resolution.
- #67285, #67718 — merged: keep partition provenance on the event and inherit it downstream, not on asset identity.
- #51424 — merged: distinct run-time inlet-reference surface instead of overloading the definition.
