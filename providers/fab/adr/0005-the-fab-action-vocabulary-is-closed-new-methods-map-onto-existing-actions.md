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
`can_create`, `can_read`, `can_edit`, `can_delete`, `can_access_menu`. Those are
rows in `ab_permission`, bound to roles in every deployment's database — some by
administrators, by hand, years ago. ADR 3 records why that persisted state makes a
mapping change a security-model change.

The opposing pressure is that the API grows: a route starts accepting a verb the
auth model does not name (`PATCH` is the concrete case), and the natural-looking
fix is to extend the model. That is far more expensive than it looks.
`ResourceMethod` is core's `BaseAuthManager` contract shared by every auth
manager. A new FAB *action* is worse: `can_patch` exists in no deployment's
database, so every existing role would fail closed on every `PATCH` after upgrade —
correctly per ADR 3, catastrophically for users — and granting it needs a data
migration that guesses intent. The resolution that shipped is small: keep the verb
out of the auth model, key the map on `str`, and map the verb onto the action that
already means the same thing. `PATCH` is an edit; roles that can edit can patch,
with no new rows and no migration.

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

- New API verbs are supportable with a one-line change and zero migration risk:
  every role that already had the privilege keeps working on upgrade.
- The permission tables stay stable across releases — the property administrators
  depend on.
- The cost is expressiveness: the model cannot distinguish "may patch" from "may
  put", or express a privilege finer than the five actions. Finer granularity is
  the job of a different auth manager, not a richer FAB vocabulary — this provider
  implements FAB's model, it does not evolve it.
- Reviewers must check the *meaning* of a mapping, not merely that it type-checks,
  because the map is keyed on an unconstrained `str`.

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

- #59564 — "Add HTTP PATCH as resource method", closed: extending the method enum
  was rejected as the wrong layer; review proposed `str` keys mapping `PATCH` onto
  the `PUT` action instead.
- #63359 — the reworked follow-up adopting exactly that (map only, no enum/route
  change); closed on quality, but its design is what the code carries today —
  `dict[str, str]` and the comment in `www/utils.py`.
- #58069 — renaming role/permission-facing shapes for tidiness, closed: same class
  as extending the vocabulary — it moves names deployments and clients are bound to.
- #61083 — Admin access denied for user model views, closed: the exact hazard here —
  permissions never created in a database mean roles silently lack access, and the
  remedy is not to synthesise new permission rows at sync time.
