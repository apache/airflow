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
provider turns an Airflow Connection into Google credentials. It resolves a wide
authentication surface (ADC, keyfile path, inline keyfile JSON, Secret Manager
key, external credential config, anonymous credentials, IdP client-credentials
grant) and applies `impersonation_chain`, `quota_project_id` (via
`with_quota_project`), OAuth scopes, `num_retries`, and the universe domain that
decides which endpoints a client talks to.

It also owns the *project*: `GoogleBaseHook.project_id` derives it from the
credentials, lets the connection's `project` extra override, and falls back to the
`PROVIDE_PROJECT_ID` sentinel (`cast("str", None)`, so methods can type
`project_id: str`). `@GoogleBaseHook.fallback_to_default_project_id` fills the
argument in when omitted and rejects positional args so the substitution cannot
land on the wrong parameter — it appears at ~490 call sites. Around 60 hooks
subclass `GoogleBaseHook` and build clients the same way
(`storage.Client(credentials=self.get_credentials(), project=self.project_id,
client_options=self.get_client_options())`). Constructing a client by any other
route silently opts out of *all* of it: impersonation, quota project, universe
domain, and project resolution. Regressions of exactly that kind have been fixed
repeatedly.

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

- One authentication implementation to reason about, test and fix — a credential
  bug is fixed once, for every service.
- New capabilities (quota project, universe domain, IdP flow) land in
  `base_google.py` and are immediately available to all ~60 hooks.
- `impersonation_chain` behaves identically everywhere — it is a security control
  users rely on to scope what a task can reach.
- `base_google.py` is a high-blast-radius file: a change there touches every
  service at once, which is why it is a structural risk path and such changes are
  discussed before the code is written.

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

- #65731 — `GenAIGenerativeModelHook` reached Google outside the base-hook chain
  and ignored the connection entirely: the exact failure this prevents.
- #61654 — `CloudSecretManagerBackend` project resolution diverging from the base
  hook's, in a component running outside the task process.
- #56324 — quota project id added once in `GoogleBaseHook`, not per service.
- #66159 — `universe_domain` support; #66917, #66404, #66341 show the cost of
  retro-fitting endpoint selection per service instead of reading shared client
  options.
- #62013 — handed the hook's credentials object to `GCSFileSystem` rather than
  letting it resolve its own.
- #61124 — moved exception handling from Bigtable operators down to the hook that
  owns the client.
- #67507 — wrote Cloud SQL `keyfile_dict` with 0600, at the one place the hook
  materialises a key on disk.
