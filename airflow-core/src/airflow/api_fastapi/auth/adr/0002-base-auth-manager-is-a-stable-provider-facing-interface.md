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

`BaseAuthManager` (`managers/base_auth_manager.py`) is the contract every concrete
auth manager implements. `SimpleAuthManager` ships in core, but the FAB and Keycloak
managers live in **provider distributions** versioned and released independently, and
a deployment routinely runs a newer or older provider manager against a different
core version.

Those managers inherit and override a broad surface: the abstract authorization
decisions (`is_authorized_*`); the collection helpers they may override for efficiency
(`batch_is_authorized_*`, `filter_authorized_*`, `get_authorized_*`); the identity
contract (`serialize_user` / `deserialize_user` and the `get_user_from_token` path);
the extension hooks (`get_fastapi_app`, `get_fastapi_middlewares`, `get_cli_commands`,
`get_db_manager`, `get_url_login` / `get_url_logout`, `get_extra_menu_items`); and
shared value types like `ResourceMethod` and the `*Details` models in those signatures.
So the public shape of `BaseAuthManager` — names, signatures, keyword-only parameters,
shared types, import paths — is a **cross-version compatibility boundary**. A rename or
signature change that looks local to core silently breaks every provider manager on the
old shape, in the mixed-version combination Airflow supports. This is the same class of
contract as `BaseExecutor` (see the executors ADR).

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

- #48196 — added the `get_db_manager` hook additively, default that managers need not
  override.
- #52883 — removed unused batch methods: interface curation done deliberately, not
  opportunistically.
- #52731 — removed `MENU` from `ResourceMethod`, a shared type in provider signatures.
- #60244 — reshaped `ResourceMethod`/`ExtendedResourceMethod` to str Enum, kept
  compatible for implementers.
- #59109 — threaded a model change (team name as PK) through the `team_name`-scoped
  surface providers implement.
- #55682 — FAB overriding the base hooks: concrete proof this surface is an API other
  distributions build on.
