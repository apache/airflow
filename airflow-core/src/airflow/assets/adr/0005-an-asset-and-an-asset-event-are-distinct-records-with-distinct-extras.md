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
`group`) declared in a Dag file, with an `extra` that describes the asset itself
and changes only when an author edits the Dag. An `AssetEvent` is an
**occurrence**: a record that some task updated that asset at a point in time,
with its own `extra` describing *that update* — carried from the task's outlet at
run time, alongside the task instance that produced it.

The two are adjacent in the code, adjacent in the API, and both called `extra`.
Conflating them is the most common substantive error in this area, and it is not
caught by types, because both are JSON blobs. A change intended to "provide the
correct extra" to the `on_asset_changed` listener was rejected on precisely this
ground — the asset event's extra is not the asset's extra, and passing one where
the other is expected changes what every listener receives. The author's own
conclusion was that the real need was a *different listener* carrying event-level
information including the producing task instance, which is the shape that
eventually landed as a distinct asset-event emission listener rather than a
redefinition of the existing one.

The same distinction governs aliases and partitions. When an asset is resolved
through an `AssetAlias`, the asset's own `extra` has to survive the indirection —
a fix was needed because it did not. When partitioned assets propagate to
consumers, `partition_key` is provenance attached to the event as it flows to
downstream asset events, deliberately not folded into the asset's identity or
definition.

Getting this wrong is expensive in a specific way: listener payloads and event
extras are consumed by user code and by downstream tooling outside this
repository. Changing what a listener receives is a compatibility break for
integrations that are not represented in the pull request, and — because both
fields are untyped JSON — the break surfaces as a `KeyError` in someone else's
plugin rather than as a failing test here.

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

- Listener and event surfaces accrete rather than change: the project ends up with
  more listeners, each narrowly scoped, instead of richer payloads on existing
  ones. That is a deliberate trade against breaking external consumers.
- Contributors frequently hit this as their first correction in the area, because
  the two `extra` fields are indistinguishable at a glance. The correction is
  conceptual, so it cannot be automated into a lint rule.
- Reviewers must check the direction of data flow — definition-time or run-time —
  for every field a change touches, which is slower than reading the diff.
- Keeping provenance off the asset identity means some queries need a join to the
  event rather than a lookup on the asset. The stability of asset identity is
  judged worth that cost.

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

- #54957 — "Provide correct extra to `on_asset_changed` listener": closed
  unmerged, with the review stating plainly that the asset event extra is not the
  asset extra; the author's follow-up conclusion was that the real need was an
  event-level listener carrying the producing task instance.
- #55115 — "Create a new listener for asset events creation": the follow-up
  attempt at that event-level listener, also closed unmerged.
- #61718 — "Add asset event emission listener event": the merged shape of the same
  need — a distinct event-level listener rather than a redefinition of the
  asset-level one.
- #58038 — "Preserve `Asset.extra` when using `AssetAlias`" (backported in
  #58712): a merged fix for asset-level data lost through alias resolution.
- #67285 — "Propagate `partition_date` to consumers of partitioned assets" and
  #67718 — "Make `partition_key` provenance-only and inherit it onto asset
  events": merged changes keeping partition provenance on the event and
  inheriting it downstream, rather than attaching it to asset identity.
- #51424 — "Store and expose task inlet references to assets": a merged change
  adding a distinct run-time reference surface instead of overloading the asset
  definition.
