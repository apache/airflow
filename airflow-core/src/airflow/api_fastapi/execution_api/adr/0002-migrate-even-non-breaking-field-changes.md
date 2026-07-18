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

Cadwyn's own guidance, and a common instinct, is to add a `VersionChange` only
for changes that are *technically* breaking — a removed field, a renamed field,
a tightened type. Additive changes (a new optional field) are treated as safe to
land without a migration.

That reasoning holds when the client and server versions are coupled. In the
Execution API they are not: Airflow does not control which server version a
given client reaches, nor which client version reaches a given server (see
ADR 0001). A Task SDK client's request/response models are code-generated from
the OpenAPI schema at build time. If a field is removed or renamed on the server
with no migration, a client that was generated against the older schema — and is
still deployed — sends or expects the old shape, and the regenerated model on a
newer worker expects the new shape. The "non-breaking" judgement silently
assumes a synchronization that does not exist in this area.

The safe default is therefore stricter here than Cadwyn's default: when in
doubt, add the migration.

## Decision

Err toward adding a Cadwyn `VersionChange` for **any** removed, renamed, or
reshaped field in the Execution API, even when the change is not strictly
breaking by Cadwyn's own classification.

The bar is not "is this technically breaking?" but "could a client generated
against a different version of this schema observe a different shape?" If the
answer is yes — which it is for essentially any field removal or rename — the
change is versioned. Purely additive optional fields still warrant a migration
when they participate in a response a client parses strictly.

This intentionally overrides Cadwyn's more permissive default for this API.

## Consequences

- Regenerated Task SDK clients keep working across the independent-deploy matrix
  because every field-shape change is expressed as a reversible migration.
- There is more `VersionChange` boilerplate than a naive "only migrate breaking
  changes" policy would produce. That is accepted deliberately: the cost of an
  extra migration is trivial next to a field-shape mismatch discovered in a
  production worker.
- Reviewers apply the stricter bar; a PR that argues "this field change is
  non-breaking so no migration is needed" is held to this ADR, not to Cadwyn's
  default.

**A violating change looks like:** removing or renaming a field in a
`datamodels/` schema (or a nested object) with no `VersionChange`, justified by
"it's not technically breaking." A client regenerated against the old schema
then mis-parses or drops the field against the new server — the failure this ADR
exists to prevent.

## Evidence

- #53809 — `include_prior_dates` handling in `xcom_pull` was corrected with attention to the wire shape rather than an in-place field tweak.
- #49818 — Adding the `map_index` filter to `GetTICount`/`GetTaskStates` was carried through the versioned schema so older clients still negotiate the request shape.
