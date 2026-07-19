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

# 2. The public REST API v2 is a generated, stable contract

Date: 2026-07-19

## Status

Accepted

## Context

The endpoints under `routes/public/` are Airflow's **public REST API v2**. Their
request and response shapes are defined by the Pydantic models in `datamodels/`,
from which an OpenAPI specification (`openapi/v2-rest-api-generated.yaml`) is
**generated** by `scripts/in_container/run_generate_openapi_spec.py`. That spec
is not documentation-after-the-fact — it is the source that downstream consumers
are code-generated from:

- the bundled **React UI** generates its TypeScript client from the spec;
- the published **Python client** (and the beta Go SDK) are generated from it;
- third-party integrations pin to the documented shapes.

Because the shapes are a contract with consumers a reviewer cannot see, a change
that looks like a harmless tidy-up in a diff — renaming a field, tightening a
type, dropping an attribute, or newly exposing an internal one — silently breaks
or destabilises every generated client. And because the spec is *generated*,
there are two ways to get out of sync: hand-editing the generated YAML (which the
next regeneration reverts), or changing a datamodel and forgetting to regenerate
(so the published contract no longer matches what the server returns).

There is also a confidentiality edge: the response models decide what leaves the
server. An internal payload exposed through a public response — raw trigger
kwargs, an untyped blob the UI has to cast — becomes part of the contract and is
hard to withdraw once clients depend on it.

A separate, deliberately private surface exists alongside this one: the
`routes/ui/` endpoints and their `_private_ui.yaml` spec serve the bundled UI
only and carry **no** public-stability promise. Keeping UI-only shapes out of the
public contract is what lets the UI iterate freely without breaking integrators.

## Decision

The public v2 API is treated as a stable, generated contract:

- **`datamodels/` is the source of truth; the `openapi/` spec is generated.** The
  generated spec files are never hand-edited — change the models and regenerate,
  and keep the regenerated spec committed and in sync.
- **Response/request shape changes on `routes/public/` are backward-compatibility-
  sensitive.** Renaming, retyping, removing, or newly exposing a field is a
  contract change that must be justified and, where it ships in a release, handled
  as a compatibility concern — not reshaped opportunistically as a cleanup.
- **Only intended data is exposed.** Public responses must not leak internal or
  unsafe fields; ambiguous fields are typed precisely rather than shipped as an
  untyped blob.
- **UI-only shapes stay on the private surface.** Endpoints that exist to serve
  the React UI live under `routes/ui/` with the private UI spec, not on the
  published v2 surface.

## Consequences

- Generated clients (UI, Python, Go) stay buildable and stable across releases;
  integrators can rely on documented shapes.
- The generated spec remains a faithful description of the server, because it is
  produced from the same models the server serves and regenerated on every change.
- Contract changes carry real process cost (justification, compatibility, sign-off)
  — intentional friction that protects consumers.
- The UI retains freedom to evolve its own endpoints without a stability promise,
  because that surface is explicitly private.

A change **violates** this decision when it:

- hand-edits a generated spec file under `openapi/` instead of changing a
  datamodel and regenerating — or changes a datamodel without regenerating, so the
  committed spec drifts from the server's actual responses;
- renames, retypes, removes, or newly exposes a field on a `routes/public/`
  request/response model as an incidental cleanup, without treating it as a
  backward-compatibility-sensitive contract change;
- leaks an internal or unsafe field (e.g. raw trigger kwargs) into a public
  response, or ships an untyped blob the client must cast;
- puts a UI-only endpoint/shape on the published v2 surface instead of the private
  `routes/ui/` surface.

A reviewer should reject any diff that changes the published v2 shape without
regenerating the spec and reasoning about downstream consumers.

## Evidence

- #67725 — "Type `asset_expression` in the REST API so the UI does not cast
  through unknown": tightened a public field's type so generated clients get a
  precise shape rather than an untyped value.
- #67868 — "Stop exposing trigger kwargs in the REST API response": withdrew an
  internal payload that should not have been part of the public contract.
- #67570 — "Fix GET /pools list endpoint incorrectly documenting 404 in OpenAPI
  spec": corrected the generated contract to match the endpoint's real behaviour.
- #67571 — "Fix GET /auth/login missing 400 in OpenAPI spec and use status
  constant": another spec-vs-behaviour correction, keeping the generated contract
  truthful.
- #62624 — "Replaced manual response descriptions with
  `create_openapi_http_exception_doc`": consolidated response documentation into
  the generator so the spec is produced consistently, not hand-written per route.
