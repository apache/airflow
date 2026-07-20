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

# 1. The UI reaches Airflow only through the API server's REST API

Date: 2026-07-20

## Status

Accepted

## Context

The React application in this directory is a static asset bundle. It is built by
Vite, served by the API server, and executed in the user's browser. It has no
database driver, no Airflow Python runtime, and no privileged channel of any
kind. Every piece of data it displays — Dags, runs, task instances, logs, assets,
connections, variables — arrives over HTTP from the API server, and every action
it performs is a request to an endpoint under `api/v2/`.

This is not an implementation accident; it is the same boundary the rest of
Airflow 3 is built on. The API server is the *single mediator between every
client and the metadata database*. The UI is deliberately just another client of
that mediator, alongside the published Python client, the Go SDK, and any
third-party integration. Requests are issued by the generated client in
`openapi-gen/requests/`, wrapped by the hooks in `src/queries/`, and carry the
same JWT any other caller would present; the `axios` interceptors in
`src/main.tsx` redirect to `api/v2/auth/login` when the server answers `401`, or
`403` with an invalid token.

Two consequences of this framing are easy to lose sight of while working inside a
component:

- **The UI has no privileges of its own.** It sees exactly what the calling user
  is authorized to see, because authorization is decided by the API server's
  dependencies, not by anything in this codebase. Hiding a control in the UI is a
  usability affordance, never an access control. The `403` short-circuit in
  `src/main.tsx` exists to stop the client re-polling an endpoint the server has
  already refused — it is a politeness to the server, not a permission check.
- **The API server is where server-side defects get fixed.** When an endpoint
  returns a `500` on malformed input, omits a field the UI needs, or aggregates
  state incorrectly, the correct repair is in the API server, where every other
  consumer benefits. A defensive cast, a client-side recomputation, or a
  `try`/`catch` in a component converts a shared bug into a local workaround that
  the next consumer will hit again — and that the next UI refactor will silently
  drop.

There is a related trap on the *deployment* side. Because the UI runs in a
browser at whatever URL an operator has put in front of it — a sub-path, a
reverse proxy, a rewritten static asset root — it must derive its own base path
from the served document (`src/queryClient.ts` reads it from the `<base href>`)
rather than assuming it owns the origin. Hardcoding absolute paths breaks
proxied installs in ways that never appear in local development.

## Decision

The UI is a REST client of the API server and nothing more:

- **No data path bypasses the API server.** The UI never reaches the metadata
  database, a scheduler process, or any Airflow internal directly. If information
  is not available over `api/v2/`, the change required is an API-server change.
- **The UI holds no privileges and enforces no authorization.** It renders what
  the server returns for the authenticated user. Visibility rules in components
  are presentation, and are never relied on as a security control.
- **Requests go through the generated client and the `src/queries/` hooks**, so
  every call inherits the typed contract, the auth interceptors, and the shared
  retry and error strategy in `src/queryClient.ts`.
- **Server-side defects are fixed server-side.** A wrong status code, a missing
  field, or a bad aggregation is corrected in the API server rather than
  compensated for in a component.
- **The client respects the deployment's URL context** — base path, proxy prefix
  and redirect targets are derived at runtime, not assumed.

## Consequences

- The UI inherits Airflow's authentication and authorization model for free, and
  cannot become a privilege-escalation path: a browser-side exploit reaches
  exactly the endpoints the user's own token already reaches.
- Fixes made for the UI benefit every other API consumer, because they land in
  the shared contract rather than in this bundle.
- Some UI work is gated on API work, which is slower than reconstructing data in
  the client — accepted deliberately, because the alternative is a UI whose view
  of Airflow diverges from every other client's.
- The UI can be served from any path or behind any proxy, because it never
  assumes ownership of the origin.

A change *violates* this decision when it:

- introduces any data path that does not go through the API server, or reaches
  Airflow internals from the browser bundle;
- treats a UI-side visibility check, hidden control, or client-side filter as an
  access control, instead of relying on the server's authorization;
- issues a hand-rolled `axios` (or `fetch`) call to an endpoint instead of going
  through the generated client and `src/queries/` hooks, bypassing the auth
  interceptors and the shared error handling;
- works around a server defect in the client — a defensive cast, a swallowed
  error, or a client-side recomputation — instead of fixing the endpoint;
- hardcodes an absolute URL or origin-rooted path that breaks a sub-path or
  reverse-proxied deployment.

A reviewer should reject any diff that gives the browser bundle a capability the
calling user's token does not already carry.

## Evidence

- #62343 — "Add async connection testing via workers for security isolation":
  connection testing was moved off the API server onto workers rather than given
  a shortcut, keeping the isolation boundary the UI depends on intact.
- #66741 — "Restrict owner-link and extra-link href to safe schemes (http, https,
  mailto, relative)": server-supplied, user-controlled URLs are untrusted input
  to the browser and are constrained before rendering.
- #67489 — "UI: Return 400 instead of 500 from `structure_data` on malformed
  `asset_expression`": the malformed-input case was fixed at the endpoint, not
  papered over in the component that consumed it.
- #67543 — "UI: align backend state aggregation with active-over-pending
  priority": the aggregation discrepancy was corrected server-side so every
  client sees the same state, rather than recomputed in the UI.
- #66690 — "UI: Preserve proxied URL on login redirect": the auth redirect
  derives the deployment's real URL instead of assuming the origin.
- #67548 — "UI: Rewrite modulepreload hrefs to the api-server static path": asset
  URLs follow the path the API server actually serves the bundle from.
