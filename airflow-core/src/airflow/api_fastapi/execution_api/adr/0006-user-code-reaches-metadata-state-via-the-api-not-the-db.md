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

# 6. User code reaches metadata state through the Execution API, never the DB directly

Date: 2026-07-18

## Status

Accepted

## Context

Workers, the triggerer, the Dag file processor, and — crucially — **user-authored
code** (task code, callbacks, timetables) run outside the API server. The
metadata database is the server's; the Execution API exists precisely so those
components can read and mutate task/asset/run state without a direct database
connection. Each caller carries a short-lived, narrowly-scoped JWT (see
[ADR-4](0004-auth-token-scopes-change-only-via-security-process.md)), so the API
is also where authorization is enforced. A code path that opens its own
`Session` and writes to the metadata DB bypasses both the boundary and the auth
model — and it does so from a place (a user callback) that may run arbitrary
code.

This decision records, for the Execution API area, the architecture boundary
already stated in the project's security model and architecture-boundaries docs,
so a change that erodes it is caught here.

## Decision

User code and SDK-defined callbacks reach and mutate metadata state **only
through the Execution API** (the Task SDK client / supervisor), never by opening
a metadata-DB session directly. New callback/SDK surfaces that need to record
state (asset events, XComs, TI updates, …) go through an Execution-API endpoint,
versioned per [ADR-1](0001-cadwyn-calver-versioning-for-independent-deploys.md).

## Consequences

The boundary stays enforceable and auditable: every state mutation from a
worker/SDK path is an API call with a scoped token, not an ambient DB write.
New callback features cost an endpoint, but that is the price of the boundary.

A violating change looks like: an SDK-defined callback or task-side code that
calls `create_session()` / opens a `Session` and writes to the metadata DB
directly (e.g. inserting `AssetEvent` rows from a Dag success callback), instead
of going through the Task SDK client to an Execution-API route. Treat such a
change as a boundary violation — route it to discuss-first (or draft-back with
the API-based alternative), not a checklist nit.

## Evidence

- #53719 — "Allow Remote logging providers to load connections from the API
  Server"; merged, and the boundary being applied in the accepted direction: a
  worker-side path that needed a Connection was moved onto the API server rather
  than given a metadata-DB session.
- #65313 — "Extract `persist_parsing_result` from `DagFileProcessorManager` for DB
  call isolation"; merged, isolating the DB call out of the component that must
  not hold one.
- #47599 / #47894 — removal of the `create_session` import from `db.py` and the
  matching migration rule; the mechanical form of the same boundary.
- `airflow-core/docs/security/security_model.rst` and the architecture-boundaries
  section of the repo `CLAUDE.md` — workers never access the metadata DB directly.
