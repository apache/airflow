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

# 1. Asset identity is a stable, normalized contract

Date: 2026-07-19

## Status

Accepted

## Context

Data-aware scheduling wires a *producer* task to a *consumer* Dag entirely by
matching an asset's identity. An `Asset` is identified by its `(name, uri)`
pair: the URI is passed through `_sanitize_uri` (lower-casing the scheme,
dropping user-info passwords, sorting query parameters, stripping trailing
slashes and fragments, then applying any AIP-60 per-scheme normalizer), and the
name is validated. That normalized pair is what gets persisted as `AssetModel`,
what the schedule-reference tables (`DagScheduleAssetReference`,
`DagScheduleAssetNameReference`, `DagScheduleAssetUriReference`) point at, and
what `SerializedAssetUniqueKey.from_asset` collapses to for equality and hashing
during event dispatch.

Because identity is a *stored, normalized string*, the normalization function is
part of the on-disk contract, not an implementation detail. When a producer Dag
emits an event for asset `X`, `register_asset_change` looks up the persisted
`AssetModel` by `(name, uri)` and resolves consumers by the same key. If the
normalization, name validation, or equality/hashing rules change, freshly-parsed
assets normalize to a *different* string than the rows already in the database.
Nothing raises — the lookup simply returns no match, or matches the wrong row —
so producers and consumers that used to be linked silently stop triggering each
other, or two assets that were distinct collapse into one. The failure is
invisible in the diff and only appears at runtime, on data that predates the
change.

The history in this area is a run of *deliberately narrow* normalization fixes
(dropping user-info, per-scheme sanitizers, name-vs-URI reference handling),
each careful not to disturb the identity of already-stored assets. That care is
the point: normalization is easy to "improve" and expensive to change.

## Decision

An asset's normalized `(name, uri)` — together with its equality and hashing —
is a **stable identity contract**. Any change to how that identity is computed
must be treated as compatibility-sensitive, not as a local cleanup.

- URI normalization / sanitisation must be **deterministic**: the same input
  always produces the same canonical URI, with no dependence on host, locale,
  dict ordering, or process state. A new scheme sanitizer must fold equivalent
  URIs together the same way on every parse.
- Consumers must be resolved through the **persisted identity** — the
  schedule-reference tables and `SerializedAssetUniqueKey` — not an incidental
  string comparison that bypasses normalization.
- A change that alters the normalized form, the name-validation rules, or
  `Asset.__eq__` / `__hash__` must **preserve resolution for already-stored
  assets**: version-gate the behaviour and migrate existing rows so that an
  asset stored under the old identity and re-parsed under the new one still
  resolve to the same `AssetModel`.
- Such a change must be covered by a test proving that an asset persisted under
  the *old* rules still matches its producers/consumers under the *new* rules
  (and, for a merge, that two now-equivalent URIs resolve to a single asset).
  The test must **fail without the migration/gate**.

## Consequences

- Producer→consumer wiring survives upgrades: normalization can be refined
  without silently orphaning existing asset schedules.
- Contributors adding a new URI scheme or tightening validation must ship the
  compatibility handling (gate + row migration + test), not just the new rule.
- Some "obvious" normalization improvements are intentionally slow to land,
  because the cost is a data migration, not a one-line change.

A **violating change** looks like any of:

- A diff that changes `_sanitize_uri`, a scheme normalizer, name validation, or
  asset equality/hashing so that an already-persisted asset now normalizes to a
  different string, with no gate and no row migration.
- Resolving a consumer by comparing a raw, un-normalized string instead of the
  persisted identity key, so equivalent URIs fail to match.
- A new scheme sanitizer whose output depends on ordering or environment, so the
  same asset can serialize to two different canonical URIs on different parses.
- A test that only checks the *new* normalization in isolation and never
  exercises an asset stored under the old identity — so it passes even though
  upgrades would break existing wiring.

## Evidence

- #51877 — "Fix Asset URI normalization for user info without password":
  a targeted normalization fix (dropping the password from user-info) made
  carefully so it does not disturb the identity of already-stored assets.
- #66426 — "Add uri sanitizers and asset factories for new schemes": adds
  per-scheme sanitizers that must fold equivalent URIs to one canonical form.
- #66710 — "Some nits in asset normalization": follow-up tightening of the
  normalization path, underscoring that this code is treated as a contract.
- #48961 — "Use asset name in schedule instead of uri": moves schedule matching
  onto the asset name, changing which part of the identity the schedule
  reference stores.
- #44639 — "Respect Asset.name when accessing inlet and outlet events": aligns
  event access with the asset's name identity rather than an incidental URI.
