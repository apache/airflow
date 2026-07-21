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

# 2. Migrate even "non-breaking" field changes

Date: 2026-07-18

## Status

Accepted

## Context

Cadwyn's own guidance, and a common instinct, is to add a `VersionChange` only for
*technically* breaking changes — a removed field, a rename, a tightened type — and to
treat additive changes (a new optional field) as safe to land without a migration.

That holds when client and server versions are coupled. In the Execution API they are
not: Airflow controls neither which server version a client reaches nor the reverse (see
ADR 0001), and a Task SDK client's models are code-generated from the OpenAPI schema at
build time. Remove or rename a field on the server with no migration, and a client
generated against the older schema — still deployed — sends or expects the old shape
while a regenerated newer worker expects the new one. The "non-breaking" judgement
silently assumes a synchronization that does not exist here, so the safe default is
stricter: when in doubt, add the migration.

## Decision

Err toward adding a Cadwyn `VersionChange` for **any** removed, renamed, or reshaped field
in the Execution API, even when the change is not strictly breaking by Cadwyn's own
classification.

The bar is not "is this technically breaking?" but "could a client generated against a
different version of this schema observe a different shape?" If yes — as it is for
essentially any field removal or rename — the change is versioned. Purely additive
optional fields still warrant a migration when they participate in a response a client
parses strictly. This intentionally overrides Cadwyn's more permissive default for this
API.

## Consequences

- Regenerated Task SDK clients keep working across the independent-deploy matrix because
  every field-shape change is a reversible migration.
- There is more `VersionChange` boilerplate than a naive "only migrate breaking changes"
  policy — accepted deliberately: an extra migration is trivial next to a field-shape
  mismatch found in a production worker.
- Reviewers apply the stricter bar; a PR arguing "this field change is non-breaking so no
  migration is needed" is held to this ADR, not to Cadwyn's default.
- [ADR-7](0007-new-surface-must-prove-no-existing-route-answers-it.md) treats accumulated
  migration weight as a reason to refuse *new* surface, which reads as the opposite cost
  model. It is not: ADR-7 gates whether a route or field comes into existence; once it
  exists, this ADR governs how it changes, and there migration cost is never a reason to
  skip one. "The chain is already long" is not an argument admissible against a migration
  on shipped surface.

**A violating change looks like:** removing or renaming a field in a `datamodels/` schema
(or a nested object) with no `VersionChange`, justified by "it's not technically breaking."
A client regenerated against the old schema then mis-parses or drops the field against the
new server — the failure this ADR exists to prevent.

## Evidence

- #53809 — `include_prior_dates` in `xcom_pull` corrected with attention to the wire shape,
  not an in-place field tweak.
- #49818 — the `map_index` filter on `GetTICount`/`GetTaskStates` carried through the
  versioned schema so older clients still negotiate the request shape.
