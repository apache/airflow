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

# 2. Secret values must never be logged or leaked from a backend lookup

Date: 2026-07-19

## Status

Accepted

## Context

The point of this area is to move credentials out of Dag code into a backend; that
value is only safe while confined to the caller that asked for it. Two things
threaten that confinement, both harmless-looking in a diff:

1. **Logging.** On a failed lookup the instinct is to log *what* was resolved or to
   put the backend's response in the error. A resolved URI or variable value in a
   log line is a cluster-wide leaked credential no downstream masking can recall.
   The resolver logs only the backend **type/name** on fall-through.

2. **Widening exposure.** Resolved secrets are redacted via `mask_secret` at the
   resolution point (connection password/extra, variable value). A change that
   returns more than asked, routes a value around the masking call, or lets a
   lookup cross an isolation boundary (a team-scoped secret read as global) widens
   exposure even with nothing logged.

Airflow's security model treats credential confidentiality as first-class; masking
and the no-log discipline are its enforcement points here.

## Decision

A secrets-backend change must not widen the exposure of any secret value:

- **Never log or print a resolved secret** — not on success, not on error, not at
  debug. Diagnostic logging identifies the **backend** (its class/type name) and
  the lookup outcome, never the secret value, the connection URI, or the variable
  contents.
- **Do not bypass or defeat masking.** Values resolved from the chain stay on the
  path that feeds `mask_secret` (connection password/extra, variable value); a
  change must not return a value around that redaction or expand what a lookup
  hands back beyond what was requested.
- **Do not let a lookup cross an isolation boundary.** A team-scoped secret must
  not resolve as a global one (and vice versa); the boundary guards exist so a
  task in one team cannot read another team's — or the global — secret through a
  naming trick.
- **Fail without disclosure.** An error path returns "not found" / falls through
  (see ADR 3) without emitting the value it was probing.

## Consequences

- Errors are debuggable by *which backend* and *which outcome*, not *which value* —
  a small friction accepted to keep secrets out of logs.
- Reviewers check exposure by following every resolved value to a masked sink or the
  caller, rejecting any new log/print/return carrying the raw value.
- Multi-team deployments keep per-team confidentiality even when identifiers collide.

**A violating change looks like:** adding `log.warning("failed to load %s = %s",
key, value)` (or putting the resolved secret/URI into an exception message),
returning a connection's raw secret around the masking path, or relaxing the
team-scope guard so a global lookup can read a team-specific environment secret.
Such a change is rejected.

## Evidence

- #62588 — "Forbid accessing team secrets with environment variable as global
  secret": a direct exposure-widening guard, stopping a global lookup from reading
  a team-scoped environment secret through the naming convention.

*(The masking discipline itself is enforced by `mask_secret` at the point
connections and variables are resolved from the chain; it predates this small
sample rather than landing in a single dedicated PR here, so it is described by
mechanism rather than cited to one number.)*
