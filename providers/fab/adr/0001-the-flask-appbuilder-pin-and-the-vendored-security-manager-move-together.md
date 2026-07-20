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

# 1. The Flask-AppBuilder pin and the vendored security manager move together

Date: 2026-07-20

## Status

Accepted

## Context

This provider does not merely depend on Flask-AppBuilder — it **vendors part of
it**. `FabAirflowSecurityManagerOverride`
(`auth_manager/security_manager/override.py`) is a transplant of upstream FAB's
`BaseSecurityManager`: the DB, LDAP, OAuth, OID and REMOTE_USER authentication
paths, the permission/role synchronisation logic, and user CRUD. The Flask
AppBuilder web layer under `www/` vendors templates and static assets in the same
spirit. This was done deliberately as a step toward removing FAB as a dependency,
and the `pyproject.toml` comment above the pin says so.

Vendoring changes what a dependency version means. Normally a provider declares a
*range* and the installed library is the authority on its own behaviour. Here the
authority is split: some methods run upstream's code, some run Airflow's copy,
and some of Airflow's copies call back into upstream internals. The two halves
are only coherent at **one exact upstream version** — the one `override.py` was
last reconciled against. That is why the dependency is pinned with `==`, not
capped: `flask-appbuilder==5.2.2` is not an over-cautious upper bound to be
relaxed, it is a statement about which source tree the vendored half matches.

The failure mode this guards against is silent and security-relevant. If the pin
moves and `override.py` does not, Airflow keeps executing its stale copy of a
method upstream has since changed — including, potentially, a method upstream
changed *because it was a vulnerability*. Nothing crashes. Authentication still
succeeds for valid users. The deployment simply no longer has the fix it believes
it installed. The reverse direction fails just as quietly: a fix applied to the
vendored copy is reverted by the next bump if nobody records that upstream still
lacks it.

Because none of this is visible in a diff, the coupling is made mechanical.
`tests/unit/fab/auth_manager/security_manager/test_fab_alignment.py` holds
`EXPECTED_FAB_VERSION` — a mirror of the pin — and fails when the installed
Flask-AppBuilder differs. It further compares the vendored class against the
installed `BaseSecurityManager`: every upstream public method must be
implemented, or listed in `AUDITED_EXCLUSIONS` with a written reason, and shared
method signatures must stay compatible. `providers/fab/CONTRIBUTING.rst` records
the bump history and points at the `upgrade-fab-provider` skill that drives the
whole sequence.

The tripwire has real limits, and they are the reason this decision is written
down rather than left to the test. It detects **shape** drift — a new method, a
removed one, a changed signature. It cannot detect that upstream rewrote the body
of a method Airflow already vendors. A green alignment test is therefore evidence
that the transplant is *complete*, never that it is *correct*.

## Decision

The `flask-appbuilder` version, the vendored security-manager code, and the drift
tripwire are a single unit and change in a single, deliberate step.

- **Keep the dependency an exact `==` pin.** Widening it to a range or a cap is
  not permitted, regardless of how compatible a newer release appears.
- **A version bump is three synchronised edits**: the pin in `pyproject.toml`,
  `EXPECTED_FAB_VERSION` in the alignment test, and `override.py` reconciled
  against the new upstream release. A PR that moves fewer than all three is
  incomplete.
- **Reconciliation is a manual read of upstream, not a test run.** For every
  method the vendored code carries, compare against the new release's body and
  transplant behavioural changes — especially security fixes. The alignment test
  is the floor, not the evidence.
- **Every entry in `AUDITED_EXCLUSIONS` carries a justification** naming why
  Airflow does not implement that upstream method. Adding a bare entry to make a
  red test green is prohibited.
- **Record the bump.** Prepend the PR to the version list in
  `providers/fab/CONTRIBUTING.rst`, and state in the PR description what changed
  upstream in the vendored surface.
- **A fix to vendored code says where upstream stands** — same behaviour,
  deliberate divergence, or not-yet-upstreamed — so the next bump does not
  silently revert it.

## Consequences

- The installed Flask-AppBuilder and Airflow's copy of it are always the same
  generation; a deployment cannot end up running a stale vendored method against
  a newer library.
- Bumping FAB is expensive by construction: a manual upstream diff, not a
  one-line dependency change. That cost is accepted, because the cheap version of
  this operation loses security fixes without any signal.
- The exact pin propagates into the `uv.lock` and the generated requirement
  tables in `README.rst` and `docs/index.rst`, so a bump touches generated files
  too — those are regenerated, never hand-edited.
- Users cannot independently upgrade Flask-AppBuilder under this provider. That
  is intended: the vendored half makes an independent upgrade unsafe.

A change **violates** this decision when it:

- relaxes `flask-appbuilder==` to a range, a cap, or a floor;
- moves the pin without reconciling `override.py` against the new upstream
  release, or updates `EXPECTED_FAB_VERSION` so the tripwire passes while the
  vendored code stays on the old generation;
- silences an alignment failure by adding an entry to `AUDITED_EXCLUSIONS`
  without a justification, or by deleting the assertion;
- edits vendored security-manager code without stating its relationship to
  upstream, so the next bump reverts it unnoticed.

A reviewer should reject any Flask-AppBuilder bump whose evidence is "the
alignment test is green", and should ask what changed inside the vendored methods.

## Evidence

- #69730 — "Bump flask-appbuilder to 5.2.2 in FAB provider": the pin,
  `EXPECTED_FAB_VERSION`, and the vendored review moved as one change.
- #66841 — "Bump flask-appbuilder to 5.2.1 and mirror new auth event hooks": the
  bump was not a pin edit — new upstream auth hooks had to be mirrored into the
  vendored manager.
- #69729 — "Add upgrade-fab-provider skill and FAB contributing doc": codified the
  bump sequence and the version history so the coupling is not tribal knowledge.
- #57170 — "Upgrade `flask-appbuilder` to 5.0.1" and #50513 — "Upgrade
  `flask-appbuilder` to 4.6.3 in FAB provider": earlier bumps in the same
  pin-plus-vendored-review shape.
- #66417 — "escape LDAP filter chars in FAB `_search_ldap` and
  `_ldap_get_nested_groups`": a security fix applied *inside* vendored code — the
  class of change a shape-only drift check cannot see and a careless bump would
  revert.
- #68226 — "Import `ldap.filter` in security_manager override": the follow-up
  import fix in the same vendored path, showing how tightly the copy tracks
  upstream module structure.
- #66840 — "Pin pyjwt>=2.11.0 in FAB provider and stabilise JWT tests under PyJWT
  2.12": a transitive dependency of the pinned FAB stack constrained explicitly,
  with the breaking combination named at the pin site.
