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

# 5. The FAB action vocabulary is closed; new methods map onto existing actions

Date: 2026-07-20

## Status

Accepted

## Context

FAB authorization is a membership test against a fixed vocabulary of actions:
`can_create`, `can_read`, `can_edit`, `can_delete`, `can_access_menu`. Those
strings are rows in `ab_permission`, joined to `ab_view_menu` rows through
`ab_permission_view`, and bound to roles in every deployment's database — some of
them by administrators, years ago, by hand. ADR 3 records why that persisted
state makes a mapping change a security-model change.

A distinct pressure arrives from the other end: the API grows. A route starts
accepting a verb the auth model does not name — `PATCH` being the concrete case —
and the natural-looking fix is to extend the model: add the verb to core's
`ResourceMethod` / `ExtendedResourceMethod`, add an action for it, teach the
routes about it.

That fix is far more expensive than it looks. `ResourceMethod` is core's
`BaseAuthManager` contract, shared by every auth manager, not just this one. A new
FAB *action* is worse still: `can_patch` would exist in no deployment's database,
so every role would be missing it, and every `PATCH` request from every existing
role would fail closed on upgrade — correctly, per ADR 3's fail-closed rule, and
catastrophically for the user. Granting it would require a data migration that
guesses at intent.

The resolution that actually shipped is deliberately small: keep the verb out of
the auth model, key the map on `str` instead of the enum, and map the new verb
onto the action that already means the same thing. `PATCH` is an edit; roles that
can edit can patch, with no new rows and no migration.

## Decision

**New HTTP methods are mapped onto the existing FAB action vocabulary. Neither
the action set nor core's resource-method enum is extended to accommodate one.**

- **`_MAP_METHOD_NAME_TO_FAB_ACTION_NAME` (`www/utils.py`) is keyed on `str`**
  precisely so a verb that is not part of the auth model can still resolve to an
  action. Adding a verb is an entry in that map — nothing else.
- **Do not add a FAB action.** `can_create` / `can_read` / `can_edit` /
  `can_delete` / `can_access_menu` are the vocabulary. A new action is a row that
  no existing role holds, so it takes access away from everyone until an
  administrator intervenes.
- **Do not widen core's `ResourceMethod` / `ExtendedResourceMethod`** to solve a
  FAB-side mapping problem. That type is the cross-auth-manager contract; a
  change there is a core API change with its own review path, not a provider fix.
- **Map to the action with the same authorization meaning**, and say in the PR
  which existing action the verb inherits and why that is the right privilege
  level. Mapping a mutating verb onto `can_read` is a privilege escalation.
- **An unmapped method resolves to denial**, never to a permissive default —
  this is ADR 3's fail-closed rule, and it is what makes the map safe to extend
  conservatively.
- **The inverse map is derived, not maintained by hand.** Several methods
  legitimately share one action; do not restructure the maps so each action needs
  a unique method.

## Consequences

- New API verbs become supportable with a one-line change and zero migration
  risk: every role that already had the privilege keeps working on upgrade.
- The permission tables stay stable across releases, which is the property
  administrators actually depend on.
- The cost is expressiveness. The model cannot distinguish "may patch" from "may
  put", or express a privilege finer than the five actions. Requests for finer
  granularity are genuinely unmet by this design, and the answer is a different
  auth manager rather than a richer FAB vocabulary — this provider implements
  FAB's model, it does not evolve it.
- Reviewers must check the *meaning* of the mapping, not merely that it type-
  checks, because the map is now keyed on an unconstrained `str`.

A change **violates** this decision when it:

- adds a member to `ResourceMethod` / `ExtendedResourceMethod` in core in order
  to make a FAB route authorize;
- introduces a new `ACTION_CAN_*` constant, or a new permission name that must
  exist in `ab_permission` for existing roles to keep working;
- re-types `_MAP_METHOD_NAME_TO_FAB_ACTION_NAME` back onto the enum, or
  otherwise requires every mapped verb to be part of the auth model;
- maps a mutating verb onto a read-level action, or adds a mapping without
  stating the privilege level it inherits;
- makes an unmapped method fall through to allowed rather than denied.

## Evidence

- #59564 — "Add HTTP PATCH as resource method": closed. Extending the auth
  model's method enum was rejected as the wrong layer; the review proposed
  instead accepting `str` keys and mapping `PATCH` onto the `PUT` action.
- #63359 — "add PATCH to HTTP method-to-FAB action mapping": the reworked
  follow-up that adopted exactly that approach — no enum change, no route change,
  map only. Also closed, on quality grounds rather than approach, and the design
  it settled on is what the code carries today: the `dict[str, str]` annotation
  and the comment in `www/utils.py` explaining why the key is not the enum.
- #58069 — "rename data models related to roles for consistency across requests
  and responses": closed. Renaming role/permission-facing shapes for tidiness is
  the same class of change as extending the vocabulary — it moves names that
  deployments and clients are already bound to.
- #61083 — "Fix Admin access denied for user model views": closed. The failure it
  reported is the exact hazard this decision avoids — permissions that were never
  created in a deployment's database mean roles silently lack access, and the
  remedy is not to synthesise new permission rows at sync time.
