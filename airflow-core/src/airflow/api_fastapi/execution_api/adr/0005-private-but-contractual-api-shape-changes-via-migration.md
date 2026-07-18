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

## Consequences

- Both the Python and Go SDKs stay generatable and correct across independent
  deploys because every shape change is a versioned, spec-level migration.
- "Cleanup" PRs that touch response/error shape are held to the same migration
  bar as feature changes — there is no fast lane for reshaping the wire format.
- The end-to-end wiring is more work than a local edit, which is the accepted
  cost of keeping two independently-generated SDK clients in sync.

**A violating change looks like:** a bulk normalization of error or response
formats across the routes with no `VersionChange` and no update to the SDK
comms/client/generated models; or introducing a shared enum as an imported
Python type instead of defining it in the OpenAPI spec — either of which breaks
a regenerated client (and silently breaks the Go SDK) despite the API being
"internal."

## Evidence

- #63801 — "Execution API: Normalize error response formats" was closed unmerged: a wire-shape normalization without an end-to-end migration is not a safe standalone cleanup here.
- #63995 — "Standardize response schemas for variables and xcoms in Execution API" was closed unmerged for the same reason — the shape change needed to go through migration, not a rewrite.
