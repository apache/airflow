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

# 1. GoogleBaseHook owns credentials, project_id and impersonation

Date: 2026-07-20

## Status

Accepted

## Context

`GoogleBaseHook` (`common/hooks/base_google.py`) is the single place where this
provider turns an Airflow Connection into Google credentials. It resolves an
unusually wide authentication surface: Application Default Credentials, a keyfile
path, an inline keyfile JSON, a key stored in Secret Manager, an external
credential configuration file, anonymous credentials, and an IdP
client-credentials grant flow. On top of that it applies `impersonation_chain`
(turning a string or list into a target principal plus delegates), the
`quota_project_id` (via `with_quota_project`, with format validation), the OAuth
scopes, `num_retries`, and the universe domain that decides which endpoints the
client talks to at all.

It also owns the *project*. `GoogleBaseHook.project_id` derives the project from
the resolved credentials, lets the connection's `project` extra override it, and
falls back to the `PROVIDE_PROJECT_ID` sentinel — a `cast("str", None)` chosen so
that hook methods can type `project_id: str` without every body handling a `None`
that cannot occur at runtime. The
`@GoogleBaseHook.fallback_to_default_project_id` decorator fills the argument in
when the caller left it out, and deliberately rejects positional arguments so the
substitution can never land on the wrong parameter. That decorator appears at
roughly 490 call sites across the package.

This centralisation is what makes the provider's behaviour uniform. Around 60
hooks subclass `GoogleBaseHook`, and each builds its client the same way —
`storage.Client(credentials=self.get_credentials(), project=self.project_id,
client_options=self.get_client_options())` in `GCSHook.get_conn()` is the shape
every other hook repeats. The moment a piece of code constructs a Google client
by some other route, it silently opts out of *all* of it: impersonation stops
being applied, the quota project is not charged, the universe domain reverts to
the public one, and the project falls back to whatever the ambient environment
happens to supply.

Regressions of exactly that kind have been fixed repeatedly, which is why this is
written down rather than left to convention.

## Decision

All credential, project and impersonation resolution happens in `GoogleBaseHook`
(or `GoogleBaseAsyncHook` for the async side). Operators, sensors, transfers,
triggers, secret backends and log handlers obtain clients from a hook.

- **Never construct a Google client outside a hook.** No `google.auth.default()`,
  no direct `google.cloud.*` client instantiation, no `discovery.build(...)` in an
  operator. Clients are created in the hook's `get_conn()` / `get_*_client()`
  from `self.get_credentials()`, `self.project_id` and `self.get_client_options()`.
- **A new hook subclasses `GoogleBaseHook`**, accepts `gcp_conn_id` and
  `impersonation_chain`, and passes them to `super().__init__()`. An async hook
  subclasses `GoogleBaseAsyncHook` and sets `sync_hook_class`, so its token comes
  from the same credential chain via `_CredentialsToken.from_hook`.
- **Hook methods that take a project are decorated with
  `@GoogleBaseHook.fallback_to_default_project_id`**, declare
  `project_id: str = PROVIDE_PROJECT_ID`, and are called with keyword arguments.
- **Operators and triggers carry `gcp_conn_id` and `impersonation_chain` and hand
  them to the hook** — they do not interpret them, and they do not offer an
  alternative way to supply credentials.
- **Endpoint selection goes through `get_client_options()`**, so a non-default
  universe domain reaches every client rather than only the ones a given change
  remembered to touch.
- **Credential material stays out of logs, XCom and world-readable files.**
  Temporary keyfiles are written with restrictive permissions and removed.

## Consequences

- One authentication implementation to reason about, test and fix. A credential
  bug is fixed once, for every service in the package.
- New capabilities — quota project, universe domain, IdP flow — land in
  `base_google.py` and are immediately available to all ~60 hooks, instead of
  being added service by service.
- `impersonation_chain` behaves identically everywhere, which matters because it
  is a security control: users rely on it to scope what a task can reach.
- The cost is that `base_google.py` is a high-blast-radius file. A change there
  touches every service at once, which is precisely why it is listed as a
  structural risk path and why such changes are expected to be discussed before
  the code is written.

A change **violates** this decision when it:

- constructs a `google.cloud.*` client, a `googleapiclient` service, or calls
  `google.auth.default()` from an operator, sensor, transfer or trigger instead
  of going through a hook;
- adds a hook that reaches Google without subclassing `GoogleBaseHook` /
  `GoogleBaseAsyncHook`, or that accepts credentials by a route which bypasses
  `impersonation_chain` and `quota_project_id`;
- adds a project-taking hook method without
  `@GoogleBaseHook.fallback_to_default_project_id`, or calls such a method
  positionally;
- accepts `impersonation_chain` on an operator but does not pass it to the hook,
  or to the trigger the operator defers to;
- hard-codes a `googleapis.com` endpoint instead of using `get_client_options()`;
- writes or logs credential material where another process or the task log can
  read it.

## Evidence

- #65731 — "Fix `GenAIGenerativeModelHook` ignoring Airflow connection
  credentials": a hook that reached Google outside the base-hook chain and so
  ignored the connection entirely. This is the exact failure the decision
  prevents.
- #61654 — "Fix `CloudSecretManagerBackend` regression with explicit
  `project_id`": project resolution diverging from the base hook's, in a
  component that runs outside the task process.
- #56324 — "Add quota project id support to Google cloud base hook": a new
  auth-adjacent capability added once in `GoogleBaseHook`, not per service.
- #66159 — "Add `universe_domain` support for `GoogleBaseHook`", with
  #66917 ("Adapt GCP CloudSQL trigger to run in private cloud"), #66404 ("Adjust
  GCP BigQuery triggers for the private cloud") and #66341 ("Update the Kubernetes
  Engine components to be able to work on Sovereign Cloud from Google
  environments") showing what it costs when endpoint selection has to be
  retro-fitted per service instead of read from the shared client options.
- #62013 — "Pass credentials object to `GCSFileSystem` for automatic token
  refresh": handing the hook's credentials object to a third-party filesystem
  client rather than letting it resolve its own.
- #61124 — "Move exception handling from Google Bigtable operators to hooks":
  service-interaction concerns pushed down to the layer that owns the client.
- #67507 — "Write Cloud SQL `keyfile_dict` credentials with 0600 permissions":
  credential material handling, at the one place where the hook must materialise a
  key on disk.
