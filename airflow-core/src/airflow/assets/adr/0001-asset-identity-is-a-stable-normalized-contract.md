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

Data-aware scheduling wires a *producer* task to a *consumer* Dag by matching an
asset's `(name, uri)` identity. The URI is normalized through `_sanitize_uri`
(scheme lower-casing, user-info password dropping, query sorting, trailing-slash
and fragment stripping, AIP-60 per-scheme normalizers) and the name validated.
That normalized pair is what is persisted as `AssetModel`, what the
schedule-reference tables point at, and what `SerializedAssetUniqueKey.from_asset`
collapses to for equality and hashing during event dispatch.

Because identity is a *stored, normalized string*, the normalization function is
part of the on-disk contract. If normalization, name validation, or
equality/hashing change, freshly-parsed assets normalize to a *different* string
than the rows already in the database — nothing raises, the lookup just returns
no match or the wrong row, so linked producers/consumers silently stop triggering
or two distinct assets collapse into one, only at runtime and only on pre-existing
data. The history here is a run of *deliberately narrow* normalization fixes, each
careful not to disturb already-stored identities.

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

- #51877 — targeted user-info-password normalization fix, made not to disturb stored identities.
- #66426 — per-scheme URI sanitizers that must fold equivalent URIs to one canonical form.
- #66710 — follow-up normalization tightening; the path is treated as a contract.
- #48961 — moves schedule matching onto the asset name.
- #44639 — aligns inlet/outlet event access to the asset's name, not an incidental URI.
