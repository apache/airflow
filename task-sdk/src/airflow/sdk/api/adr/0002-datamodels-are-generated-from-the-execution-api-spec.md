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

# 2. The request/response models are generated from the Execution-API spec, never hand-edited

Date: 2026-07-19

## Status

Accepted

## Context

The request/response bodies this client exchanges with the Execution API are
Pydantic models in `datamodels/_generated.py` — **machine-generated** from the
server's Execution-API OpenAPI spec by `task-sdk/dev/generate_task_sdk_models.py`
(which points datamodel-codegen at the server's `execution/openapi.json`). The
server is the single source of truth for the wire shape; the client's copy is a
projection of it.

Hand-editing is a trap for two reasons. The next regeneration silently overwrites
any manual change, so a local "fix" disappears — and until it does, the client
parses a shape the server never emits. And the two halves of the contract must
agree: the server evolves its schema behind Cadwyn versioning, so a model change
not reflected in a server datamodel *plus* a Cadwyn `VersionChange` breaks
agreement for exactly the deploys where client and server versions differ. Parity
also covers `API_VERSION`, sent as the `airflow-api-version` header.

## Decision

The Execution-API request/response models in `datamodels/_generated.py` are
generated from the server spec and never hand-edited. Concretely:

- A wire-shape change *starts on the server* — a datamodel change plus a Cadwyn
  `VersionChange` — and is then reflected here by regenerating with
  `task-sdk/dev/generate_task_sdk_models.py`, not by patching the file.
- The generated `API_VERSION` is sent as the `airflow-api-version` header and
  stays in sync with the spec the models were generated from.
- A change that alters a request/response model is wired end-to-end (server
  datamodel → route → version file → `comms.py` → client method → regenerated
  `_generated.py` → per-version tests) so the two halves stay in parity.

## Consequences

- Client/server parity is guaranteed by construction; drift shows up as a
  regeneration diff rather than a silent mismatch.
- Contributors must touch the server first and regenerate — that cost is
  deliberate.
- A hand-edit is a latent bug: it works until the next regeneration erases it, so
  it must be caught in review rather than trusted to fail loudly.

A change **violates** this decision when it:

- hand-edits `datamodels/_generated.py` instead of regenerating it from the spec;
- changes a client request/response model without a matching server datamodel
  change and a Cadwyn `VersionChange`;
- lets `API_VERSION` / the `airflow-api-version` header drift from the generated
  spec;
- adds a client method for an endpoint or field the server spec does not define
  (a half-wired change).

## Evidence

- #61469 — established the generation flow making `_generated.py` a projection of
  the server spec.
- #68390 — versioned worker-bound TaskInstance fields; carried through the spec,
  not edited in place.
- #61251 — types shared with the client are defined in the spec, not imported
  from Python.
- #67418 — JSON types for task/asset states; the model change flowed spec →
  regenerate, not a hand-edit.
