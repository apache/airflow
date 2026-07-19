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

# 1. BaseSecretsBackend is a stable, provider-facing interface with a fixed search order

Date: 2026-07-19

## Status

Accepted

## Context

`BaseSecretsBackend` is not an internal-only base class. It is the contract that
every bundled backend (`EnvironmentVariablesBackend`, `MetastoreBackend`,
`LocalFilesystemBackend`) and every out-of-tree backend inherits from: Vault, AWS
Secrets Manager, GCP Secret Manager, Azure Key Vault, and custom deployment
backends all subclass it and override its hooks (`get_conn_value` /
`get_connection`, `get_variable`, `get_config`), reusing the base helpers
(`build_path`, `deserialize_connection`).

Those backends live in provider distributions — or in wholly private code —
versioned and released **independently** of `airflow-core`. A deployment
routinely runs a newer provider on an older core, or an older custom backend on a
newer core. That makes the public shape of `BaseSecretsBackend` a cross-version
compatibility boundary, which is *why* the class was moved into the
`secrets_backend` shared library and stripped of its core-only imports, so the
Task SDK and core could depend on the same contract without pulling in
`airflow.models`.

The interface is not only method names — it is also the **lookup order**.
`DEFAULT_SECRETS_SEARCH_PATH` is `[EnvironmentVariablesBackend,
MetastoreBackend]`, and `initialize_secrets_backends()` prepends a configured
custom backend *ahead* of those defaults. The resolver walks the chain in that
order and returns the first hit. The order is a documented, security-relevant
guarantee: which source wins when the same `conn_id`/key exists in two places is
behaviour deployments rely on.

The recurring pressure is "cleanup": renaming a method to something tidier,
tightening a signature, moving a helper, or reordering the default list all look
local to core — but each silently breaks backends running in the mixed-version
combinations we support, or changes which secret a deployment resolves.

A concrete near-miss: AIP-67 (multi-team) added a `team_name` keyword to the
lookup methods. Rather than force every custom backend to grow the parameter,
the change forwards `team_name` *only* to backends whose signature accepts it
(`_accepts_team_name` / `call_secrets_backend_method`) and omits it for pre-3.2
overrides — precisely because the interface must stay callable by older backends.

## Decision

Treat the public surface of `BaseSecretsBackend` — its method names, signatures,
and helper contract — **and** the search-path ordering as a stable,
provider-facing API:

- **Never rename or remove** a public method, nor change a public signature in a
  backwards-incompatible way (removing/reordering positional parameters,
  tightening types, dropping keyword names an override may pass through).
- Make changes **additive**. New behaviour is a new method or a new *optional*
  keyword with a default that preserves the old call semantics — and new keywords
  are forwarded only to overrides that accept them, so pre-existing backends keep
  working (the `_accepts_team_name` pattern).
- **Keep the shared base class free of core-only imports.** Core-specific needs
  (e.g. the concrete `Connection` class) are injected at instantiation, not
  imported into the shared surface.
- **Preserve the search order** — configured custom backend, then environment
  variables, then metastore. Reordering the chain, or `DEFAULT_SECRETS_SEARCH_PATH`,
  is a behaviour/security change requiring explicit discussion, not a cleanup.
- When a public name genuinely must move, keep the old name working via a
  deprecation shim; removal happens only on a documented major boundary.

## Consequences

- Provider and custom backends can be upgraded ahead of, or behind, core without
  `AttributeError` / `TypeError` at the lookup boundary.
- Core carries some deprecated shims and signature-forwarding logic; that cost is
  accepted in exchange for the version-mix guarantee.
- Refactors that want to tidy the interface must do so additively and leave the
  old names and the established order intact.

**A violating change looks like:** renaming or deleting a public
`BaseSecretsBackend` method, tightening `get_conn_value(...)`'s signature with no
shim, re-adding a core import into the shared base class, or reordering the
default search path to put the metastore ahead of environment variables — so that
an independently-versioned backend breaks at runtime or a deployment silently
resolves a different secret. Such a change is rejected.

## Evidence

- #58621 — "Move BaseSecretsBackend to shared library for client server
  separation": established the base class as a shared, client/server-neutral
  contract rather than a core-internal one.
- #61523 — "Remove Connection dependency from shared secrets backend": kept the
  shared surface free of a core type, injecting the concrete class instead.
- #59597 — "Remove core references in secrets backend logic in sdk": same
  direction — no core coupling in the shared/SDK-facing interface.
- #59476 / #58905 — "Check team boundaries in connections" / "... in variables":
  added the `team_name` keyword additively, forwarded only to backends that
  accept it so older overrides keep working.
