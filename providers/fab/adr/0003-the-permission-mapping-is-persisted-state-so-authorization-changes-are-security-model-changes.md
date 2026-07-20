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

`FabAuthManager` implements core's authorization contract, but the way it answers
is unlike any other auth manager. `_is_authorized` does not evaluate a policy; it
performs a **membership test**: it converts the caller's core-level question
(`method`, `resource_type`) into a FAB `(action, resource_name)` pair and checks
whether that pair is in the user's permission set. Everything interesting
therefore lives in two places — the mapping functions that produce the pair, and
the `ab_permission_view` rows that a deployment's administrator bound to roles.

Those rows are **persisted state that Airflow does not own**. An operator created
roles years ago, attached per-Dag permissions derived through
`permissions.resource_name()`, and has been running on them since. Airflow's code
decides how to *derive* the name it looks up; the deployment decides what is
*stored*. When the derivation changes, the stored rows do not. The lookup simply
starts landing somewhere else.

This gives authorization changes here a property that makes them qualitatively
different from ordinary provider fixes:

- **They are retroactive.** There is no opt-in, no feature flag, and no
  migration. The next provider upgrade re-scopes every existing role on every
  deployment that installed it.
- **They are invisible in both directions.** A derivation change that narrows
  access looks like a rash of 403s with no code path to blame. One that widens
  access looks like nothing at all — which is the dangerous direction.
- **They cannot be validated by the happy path.** A test asserting that an
  authorized user is allowed passes identically whether the mapping is right or
  catastrophically permissive. Only the *denied* case discriminates.
- **The blast radius is the whole deployment**, not one integration — this is the
  auth manager most existing Airflow installations authenticate against.

The related failure mode is the resolution path itself. `_is_authorized_dag`
checks the global `DAG` resource first and falls back to the per-Dag resource
name; `is_authorized_dag` layers sub-entity checks on top; menu items and the
`get_authorized_*` collection helpers run the same machinery in bulk. Every one
of these is a place where an early return, a cached permission set, or a
concurrent write can turn "I could not determine this" into "allowed". Core's
auth ADRs establish that the auth manager fails closed and that widening access
is gated on the security process; this decision states what that obligation means
for an implementation whose answers are assembled from database rows and derived
strings.

Core's ADR 2 additionally fixes the *interface* this provider implements. Nothing
here relaxes that: the `is_authorized_*` signatures, the `serialize_user` /
`deserialize_user` shape, and the collection helpers this provider overrides are
core's contract, and this provider consumes it across the range of core versions
it declares.

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

- A caller the provider cannot positively authorize gets nothing, including on
  every error path — the safe direction.
- Permission-derivation changes arrive with a stated impact on existing roles and
  prior agreement, so review confirms an agreed decision rather than discovering
  a silent one.
- Some genuine improvements to the mapping are slower to land, because they must
  be argued as security-model changes. That friction is deliberate: the
  alternative is re-scoping every operator's roles without telling them.
- Authorization tests are heavier — allowed and denied, and for collection
  helpers the filtered-out case too.

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

- #69106 — "Fix DAG named `DAGs` colliding with the global DAGs permission
  resource": a derived per-Dag resource name colliding with a global one — the
  exact hazard of deriving lookup keys that persisted rows are bound to.
- #54926 — "Update `is_authorized_dag` method in `FabAuthManager`" and #51462 —
  "allow users with specific DAG permissions to access DAGs when no specific DAG
  is requested": reshaped the Dag resolution path, changing what existing role
  grants mean in practice.
- #55682 — "Override `get_authorized_connections`, `get_authorized_pools` and
  `get_authorized_variables` in Fab auth manager": collection helpers overridden
  on the core contract, running the same derivation in bulk.
- #61462 — "use correct PUT permission for `/roles/{name}` endpoint": a wrong
  action in the mapping — a one-token defect with authorization consequences.
- #65685 — "Honor `AUTH_ROLE_PUBLIC` in FastAPI API server" and #69773 — "Fix
  `AUTH_ROLE_PUBLIC` returning 401 in FastAPI API server": public-role handling
  diverging between the two web stacks, the archetype of an access-scope change
  that must be deliberate.
- #63842 — "Fix race condition in FabAuthManager when workers concurrently create
  permissions, roles, and resources": concurrent permission creation across API
  server workers.
- #64539 — "invalidate cached user permissions after LDAP role sync to prevent
  intermittent 403s": a stale permission cache producing non-deterministic
  authorization answers.
- #69374 — "Verify Azure AD OAuth id_token signatures by default in FAB auth
  manager" and #67630 — "Add defensive validation for LDAP search filter
  configuration": identity-side inputs hardened to fail closed.
- #65735 — "Fix FAB password hashing to respect `FAB_PASSWORD_HASH_METHOD`
  config": a security setting silently not honoured — passing happy-path tests
  throughout.
- #68100 — "Fix fab deserialize user session leak": the per-request user
  deserialization path, on the core-contract surface, leaking resources.
