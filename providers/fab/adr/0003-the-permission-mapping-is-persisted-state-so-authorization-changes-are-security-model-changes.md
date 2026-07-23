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

# 3. The permission mapping is persisted state, so authorization changes are security-model changes

Date: 2026-07-20

## Status

Accepted

## Context

`FabAuthManager` implements core's authorization contract, but `_is_authorized`
does not evaluate a policy — it performs a **membership test**: it converts the
caller's `(method, resource_type)` into a FAB `(action, resource_name)` pair and
checks whether that pair is in the user's permission set. Everything interesting
lives in the mapping functions that produce the pair and the `ab_permission_view`
rows a deployment's administrator bound to roles.

Those rows are **persisted state Airflow does not own**: operators created roles
and attached per-Dag permissions (via `permissions.resource_name()`) years ago.
Airflow decides how to *derive* the name it looks up; the deployment decides what
is *stored*. When the derivation changes, the stored rows do not — the lookup just
lands elsewhere. That makes authorization changes here qualitatively different
from ordinary fixes: they are **retroactive** (no flag, no migration — the next
upgrade re-scopes every existing role), **invisible in both directions** (narrowing
looks like unexplained 403s, widening looks like nothing — the dangerous
direction), **not validatable by the happy path** (only the *denied* case
discriminates a correct mapping from a catastrophically permissive one), and
**deployment-wide in blast radius** — this is the auth manager most installs use.

The resolution path itself is the related hazard: `_is_authorized_dag` checks the
global `DAG` resource then falls back to the per-Dag name, `is_authorized_dag`
layers sub-entity checks, and the menu filter and `get_authorized_*` helpers run
the same machinery in bulk — each a place where an early return, a stale cache, or
a concurrent write can turn "could not determine" into "allowed". Core's auth ADRs
establish that the auth manager fails closed and that widening is gated on the
security process; this decision states what that means for an implementation whose
answers are assembled from database rows and derived strings. Core's ADR 2 fixes
the *interface* this provider implements (`is_authorized_*` signatures,
`serialize_user` / `deserialize_user`, the overridden collection helpers); nothing
here relaxes that, and the provider consumes it across its declared core range.

## Decision

Changes to how this provider derives or resolves permissions are treated as
security-model changes, and the resolution path fails closed.

- **Resolution fails closed.** An unmapped method, an unrecognised resource, an
  unresolvable user, or an error while assembling a permission set resolves to
  denied. No branch of `_is_authorized`, `_is_authorized_dag`, the menu filter,
  or a `get_authorized_*` helper may fall through to allow when it could not
  positively establish the permission.
- **The `(action, resource_name)` derivation is a compatibility surface.**
  Changing how a resource name or action is produced — including collision
  handling between a Dag's derived name and a global resource name — re-scopes
  permissions that already exist in production databases. Such a change is
  proposed and agreed as a security-model change, states its effect on existing
  roles, and is called out in `providers/fab/docs/changelog.rst` when operators
  must act.
- **Widening goes through the security process before the code.** A new
  anonymous or public-role path, a relaxed check, a permission added to a default
  role, or a broadened token/session scope is agreed on the devlist or with the
  security team first — not discovered during review of a finished diff.
- **Narrowing is announced, not slipped in.** A correction that removes access
  some users currently have is still a behaviour change for them; say so
  explicitly rather than describing it as a bug fix alone.
- **Permission and role synchronisation is concurrency-safe.** Multiple API
  server workers start simultaneously and race to create the same permission,
  resource, and role rows; handle the resulting integrity errors rather than
  assuming a single writer, and invalidate cached permission sets after a sync so
  a stale cache cannot produce an intermittent wrong answer.
- **Every authorization change carries a denied-case test.** A test that only
  proves the allowed case is not evidence.
- **Implement the core contract rather than forking it** — keep the
  `is_authorized_*` and user-serialization surface aligned with
  `BaseAuthManager`, and gate any newer-core behaviour through the provider's own
  `version_compat.py` against the declared core floor.

## Consequences

- A caller the provider cannot positively authorize gets nothing, on every error
  path — the safe direction.
- Permission-derivation changes arrive with a stated impact on existing roles and
  prior agreement, so review confirms an agreed decision rather than a silent one.
- Some genuine mapping improvements land slower because they must be argued as
  security-model changes — deliberate friction; the alternative is re-scoping every
  operator's roles without telling them.
- Authorization tests are heavier: allowed and denied, plus the filtered-out case
  for collection helpers.

A change **violates** this decision when it:

- lets any path in `_is_authorized`, `_is_authorized_dag`, the menu filter, or a
  `get_authorized_*` helper resolve to allowed when the permission could not be
  positively established;
- alters how a FAB `(action, resource_name)` pair is derived — including per-Dag
  resource naming and its collisions with global resource names — without
  treating it as a security-model change and stating its effect on existing
  roles;
- widens what an unauthenticated caller, an anonymous session, a public role, or
  a default role can reach, without prior devlist or security-team agreement;
- assumes a single writer when creating permissions, resources, or roles, or
  leaves a cached permission set stale after a role synchronisation;
- lands an authorization behaviour change with only an allowed-case test.

A reviewer should reject any authorization change whose failure direction is
"allow", and should send any mapping or access-widening change back to the
security process before reviewing it as code.

## Evidence

- #69106 — a Dag named `DAGs` deriving a per-Dag resource name that collides with
  the global one: the exact hazard of deriving keys persisted rows are bound to.
- #54926, #51462 — reshaped the Dag resolution path, changing what existing role
  grants mean in practice.
- #55682 — overrode `get_authorized_connections` / `_pools` / `_variables`,
  running the same derivation in bulk on the core contract.
- #61462 — wrong PUT permission for `/roles/{name}`: a one-token mapping defect
  with authorization consequences.
- #65685, #69773 — `AUTH_ROLE_PUBLIC` handling diverging between the two web
  stacks: the archetype of an access-scope change that must be deliberate.
- #63842 — race when workers concurrently create permissions, roles, resources.
- #64539 — invalidate cached permissions after LDAP role sync; a stale cache gave
  non-deterministic authorization answers.
- #69374 — verify Azure AD OAuth id_token signatures by default: identity input
  hardened to fail closed.
- #67630 — "Add defensive validation for LDAP search filter configuration":
  validates `AUTH_LDAP_SEARCH_FILTER` so a filter built from Helm/env config fails
  fast. Its commit message says this is defensive hardening, *not* a vulnerability
  fix — evidence that misconfiguration of the identity path is treated as a real
  failure mode, not that it was a security hole.
- #65735 — `FAB_PASSWORD_HASH_METHOD` silently not honoured, passing happy-path
  tests throughout.
- #68100 — per-request user deserialization on the core-contract surface leaking
  resources.
