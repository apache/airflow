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

The React application here is a static asset bundle: built by Vite, served by the
API server, run in the browser, with no database driver and no Airflow runtime.
Every piece of data it shows and every action it performs is an HTTP request to an
endpoint under `api/v2/`. This is the same boundary the rest of Airflow 3 is built
on — the API server is the single mediator between every client and the metadata
database, and the UI is deliberately just another client of it, alongside the
Python client and the Go SDK. Requests go through the generated client in
`openapi-gen/requests/`, wrapped by `src/queries/`, carrying the same JWT any other
caller presents; the `axios` interceptors in `src/main.tsx` redirect to
`api/v2/auth/login` on `401`, or `403` with an invalid token.

Two consequences are easy to lose sight of inside a component. **The UI has no
privileges of its own** — authorization is decided by the API server's
dependencies, so hiding a control is a usability affordance, never access control;
the `403` short-circuit only stops the client re-polling a refused endpoint. And
**server-side defects are fixed server-side** — a defensive cast, client-side
recomputation, or a component `try`/`catch` turns a shared bug into a local
workaround the next consumer hits again. On the deployment side, the UI runs at
whatever URL an operator fronts it with, so it derives its base path from the served
document (`src/queryClient.ts` reads `<base href>`) rather than assuming it owns the
origin.

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

- The UI inherits Airflow's auth model for free and cannot become a
  privilege-escalation path: a browser-side exploit reaches only the endpoints the
  user's own token already reaches.
- Fixes made for the UI benefit every other API consumer, because they land in the
  shared contract rather than in this bundle.
- Some UI work is gated on API work — slower, accepted deliberately, because the
  alternative is a UI whose view of Airflow diverges from every other client's.
- The UI can be served from any path or behind any proxy.

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

- #62343 — connection testing moved off the API server onto workers, keeping the
  isolation boundary the UI depends on intact.
- #66741 — server-supplied, user-controlled URLs constrained to safe schemes before
  rendering.
- #67489 — malformed `asset_expression` fixed to return `400` at the endpoint, not
  papered over in the component.
- #67543 — state-aggregation discrepancy corrected server-side so every client sees
  the same state.
- #66690 — auth redirect derives the deployment's real proxied URL instead of the
  origin.
- #67548 — modulepreload asset URLs follow the api-server static path.
