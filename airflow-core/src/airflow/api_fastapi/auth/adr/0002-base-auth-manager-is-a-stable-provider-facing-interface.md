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

# 2. BaseAuthManager is a stable, provider-facing interface

Date: 2026-07-19

## Status

Accepted

## Context

`BaseAuthManager` (`managers/base_auth_manager.py`) is not an internal-only base
class. It is the contract that every concrete auth manager implements. The
reference `SimpleAuthManager` ships in core, but the FAB auth manager and the
Keycloak auth manager live in **provider distributions** that are versioned and
released independently of `airflow-core`. A deployment routinely runs a newer or
older provider auth manager against a different core version.

Those managers inherit and override a broad surface:

- the abstract authorization decisions (`is_authorized_dag`,
  `is_authorized_connection`, `is_authorized_pool`, `is_authorized_variable`,
  `is_authorized_view`, `is_authorized_custom_view`, …);
- the collection helpers they may override for efficiency
  (`batch_is_authorized_*`, `filter_authorized_*`, `get_authorized_*`);
- the identity contract (`serialize_user` / `deserialize_user`, and the
  `get_user_from_token` path built on it);
- the extension hooks (`get_fastapi_app`, `get_fastapi_middlewares`,
  `get_cli_commands`, `get_db_manager`, `get_url_login` / `get_url_logout`,
  `get_extra_menu_items`);
- shared value types such as `ResourceMethod` and the `*Details` resource models
  that appear in those signatures.

That means the public shape of `BaseAuthManager` — method names, signatures,
keyword-only parameters, the shared enums/types, and the import paths that resolve
them — is a **cross-version compatibility boundary**, not a detail core can reshape
when convenient. A rename or a signature change that looks local to core silently
breaks every provider auth manager that still implements the old shape, in exactly
the mixed-version combination Airflow supports. This is the same class of contract
as `BaseExecutor` (see the executors ADR): a provider-facing base whose surface is
an API.

## Decision

Treat the public surface of `BaseAuthManager` as a stable, provider-facing API:

- **Never rename or remove** a public method, hook, or shared type of
  `BaseAuthManager`, nor change a public method's signature in a
  backwards-incompatible way (removing/reordering parameters, dropping keyword
  names, tightening a return type an override depends on).
- Make changes **additive**. New behaviour is a new hook or a new optional
  keyword argument with a default that preserves the old call semantics — and that
  default must not force every existing provider manager to override it. When a
  capability may be unsupported, fail closed with a clear signal (the
  `is_authorized_team` "raise `NotImplementedError` unless the manager is
  multi-team-aware" pattern) rather than a default that silently allows.
- When a public name or type genuinely must move, **deprecate rather than delete**:
  keep the old name/type working (an alias, a `@property`, or a deprecated shim
  that forwards to the new one) for a release, and remove it only on a documented
  major boundary once the minimum supported provider version no longer references
  it.
- Curating the interface (removing genuinely unused methods, collapsing a
  redundant value type) is allowed, but it is an **interface change** — justified,
  and done additively/with deprecation, not opportunistically inside an unrelated
  change.

## Consequences

- Provider auth managers (FAB, Keycloak, custom) can be upgraded ahead of or behind
  core without `AttributeError` / `TypeError` at the manager boundary.
- Core carries some deprecated shims for a release or two; that cost is accepted in
  exchange for the version-mix guarantee and for not breaking downstream managers.
- Refactors that tidy the interface must do so additively and leave the old surface
  callable through the deprecation window.

**A violating change looks like:** renaming or deleting a public `BaseAuthManager`
method or shared type (e.g. renaming an `is_authorized_*` method, dropping a hook a
provider manager overrides, or changing `serialize_user` / `get_cli_commands`'s
signature) to "unify" or "clean up" the interface, with no alias/shim and no
deprecation period — so that a provider auth manager on a different core version
raises at runtime. Such a change is rejected.

## Evidence

- #48196 — "Add option in auth managers to specify DB manager": added the
  `get_db_manager` hook additively, with a default that existing managers need not
  override.
- #52883 — "Remove unused batch methods from auth manager": an interface-curation
  change to the provider-facing surface, done deliberately rather than opportunistically.
- #52731 — "Remove `MENU` from `ResourceMethod` in auth manager": a change to a
  shared value type that flows through provider-implemented signatures.
- #60244 — "Literal to str Enum for ResourceMethod & ExtendedResourceMethod":
  reshaped a shared type used across every `is_authorized_*` signature, kept
  compatible for implementers.
- #59109 — "Remove team ID and use team name as PK": threaded a model change
  through the `team_name`-scoped authorization surface that providers implement.
- #55682 — "Override `get_authorized_connections`, `get_authorized_pools` and
  `get_authorized_variables` in FAB auth manager": a provider distribution
  overriding the base hooks — the concrete evidence that this surface is an API
  other distributions build on.
