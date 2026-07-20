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

# 5. Private-but-contractual API: shape changes go via migration, not standalone rewrite

Date: 2026-07-18

## Status

Accepted

## Context

The Execution API is not a public REST API — it carries no external stability
promise — but it is a hard contract with real consumers: the Python Task SDK and
the Go Task SDK both generate their request/response models and comms types from
its OpenAPI schema. "Internal" here means "not promised to end users," not "free
to reshape at will." Every consumer is code-generated from the spec and deployed
independently (see ADR 0001).

That combination makes response/error-format cleanups deceptively dangerous. A
change that reads as pure hygiene — normalizing error envelopes, standardizing a
response schema, tidying a status shape — is a wire-contract change to every
generated client. Landing it as a standalone rewrite, without a migration wired
through the whole chain, breaks clients that were generated against the old shape
and are still in the field. It also breaks the second SDK (Go) if the change is
made only in Python and the shared shape is expressed as imported Python types
rather than in the language-neutral spec.

## Decision

Treat the Execution API as private-but-contractual: shape changes go through a
Cadwyn migration wired end-to-end, never as a standalone response-format rewrite.

- Do not reshape error or response formats as a cleanup. Any change to a
  response or error shape is a `VersionChange` carried across the full chain:
  **datamodel → route → version migration → Task SDK message type (`comms.py`)
  → SDK client → supervisor handling → regenerated models** (per the end-to-end
  checklist in this area's `AGENTS.md`).
- Shared enums and constants that both sides depend on live in the Execution
  API / OpenAPI spec so both SDKs generate them, not as a Python type imported
  across the boundary. The Go SDK cannot import Python; the spec is the single
  source of truth for shared shapes.

### Scope: the absolute reading governs, not the "bulk" one

An earlier phrasing of this ADR paired an absolute Decision ("*any* change to a
response or error shape") with a narrow violating-change exemplar ("a **bulk**
normalization … across the routes"). Those are not the same rule, and the gap
between them let single-endpoint error-shape fixes land inconsistently depending
on which sentence the reviewer read. **The absolute reading governs.** A
generated client is generated per-endpoint; one endpoint's error shape changing
breaks exactly the callers of that endpoint, and the blast radius of a
single-route change is not smaller in kind, only in count. Number of routes
touched is not a factor.

The one decidable exception is where the shape does not actually change on the
wire *as declared*:

- If the OpenAPI spec **already declares** the shape the change produces — the
  route was returning something other than its own declared response/error model,
  and the fix brings the implementation into line — the contract generated clients
  were built against is unchanged, and no `VersionChange` is owed. The PR must
  show this: point at the declared model and at the diff between it and what the
  route returned.
- Otherwise a `VersionChange` is owed, however narrow the diff, and however much
  the new shape is an improvement (RFC 9457 conformance included).

### Cross-cutting error handling still owes a `VersionChange`

`core_api/adr/0004` requires cross-cutting API behaviour — exception handlers,
middleware, shared dependencies — to be fixed once in the shared layer rather
than patched per route. That decision governs *where* the fix lives; it does not
waive this one. A shared-layer change that alters a wire-visible response or
error shape on Execution-API routes owes a `VersionChange` here exactly as a
per-route change would. The two compose: put the fix in the shared layer, *and*
carry the shape change through the migration chain. Neither ADR is satisfied by
citing the other.

## Consequences

- Both the Python and Go SDKs stay generatable and correct across independent
  deploys because every shape change is a versioned, spec-level migration.
- "Cleanup" PRs that touch response/error shape are held to the same migration
  bar as feature changes — there is no fast lane for reshaping the wire format.
- The end-to-end wiring is more work than a local edit, which is the accepted
  cost of keeping two independently-generated SDK clients in sync.

**A violating change looks like:** a change to an error or response format —
whether on one route or across all of them — with no `VersionChange` and no
update to the SDK comms/client/generated models, where the OpenAPI spec did not
already declare the new shape; a shared-layer exception handler or middleware
change that reshapes an Execution-API response without a `VersionChange`; or
introducing a shared enum as an imported Python type instead of defining it in
the OpenAPI spec — any of which breaks a regenerated client (and silently breaks
the Go SDK) despite the API being "internal."

## Evidence

- #63801 — "Execution API: Normalize error response formats" was closed unmerged: a wire-shape normalization without an end-to-end migration is not a safe standalone cleanup here.
- #63995 — "Standardize response schemas for variables and xcoms in Execution API" was closed unmerged for the same reason — the shape change needed to go through migration, not a rewrite.
