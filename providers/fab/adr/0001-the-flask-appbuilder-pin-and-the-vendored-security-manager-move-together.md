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
`BaseSecurityManager` — the DB, LDAP, OAuth, OID and REMOTE_USER auth paths,
permission/role sync, and user CRUD — and `www/` vendors the web layer's templates
and assets. This is a deliberate step toward removing FAB, as the `pyproject.toml`
comment above the pin says.

Vendoring splits the authority for behaviour: some methods run upstream's code,
some run Airflow's copy, some of Airflow's copies call back into upstream. The two
halves cohere only at **one exact upstream version** — the one `override.py` was
last reconciled against — so `flask-appbuilder==5.2.2` is pinned with `==` as a
statement about which source tree the vendored half matches, not an over-cautious
cap. If the pin moves and `override.py` does not, Airflow silently keeps running a
stale copy of a method upstream may have changed *because it was a vulnerability*;
nothing crashes and valid logins still succeed. The reverse is just as quiet: a
fix in the vendored copy is reverted by the next bump if nobody records it.

The coupling is made mechanical: `test_fab_alignment.py` holds
`EXPECTED_FAB_VERSION` (a mirror of the pin) and compares the vendored class
against the installed `BaseSecurityManager` — every upstream public method must be
implemented or listed in `AUDITED_EXCLUSIONS` with a reason, and shared signatures
must stay compatible. But the tripwire detects only **shape** drift; it cannot see
that upstream rewrote a method body Airflow vendors. A green alignment test proves
the transplant is *complete*, never that it is *correct*.

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

- The installed Flask-AppBuilder and Airflow's copy are always the same
  generation; a deployment cannot run a stale vendored method against a newer lib.
- Bumping FAB is expensive by construction — a manual upstream diff, not a one-line
  change — because the cheap version loses security fixes with no signal.
- The exact pin propagates into `uv.lock` and generated requirement tables, so a
  bump touches generated files too; those are regenerated, never hand-edited.
- Users cannot independently upgrade Flask-AppBuilder here — intended, because the
  vendored half makes an independent upgrade unsafe.

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

- #69730 — bump to 5.2.2: pin, `EXPECTED_FAB_VERSION`, and vendored review moved
  as one change.
- #66841 — bump to 5.2.1 required mirroring new upstream auth event hooks into the
  vendored manager, not just a pin edit.
- #69729 — added the `upgrade-fab-provider` skill and contributing doc codifying
  the bump sequence, so the coupling is not tribal knowledge.
- #57170, #50513 — earlier bumps (5.0.1, 4.6.3) in the same pin-plus-review shape.
- #66417 — a fix to the LDAP authentication handler in `override.py`: hardening
  applied *inside* vendored code — the class of change a shape-only drift check
  cannot see and a careless bump would revert. (#67103, closed unmerged, proposed
  the same escaping for `_search_ldap` / `_ldap_get_nested_groups`.) Both PRs are
  authored by an automated security-scanning account and do not resolve via the
  GitHub API; #66417's merge on `main` is commit `3f7756bea7`.
- #68226 — follow-up `import ldap.filter` fix in the same vendored path, showing
  how tightly the copy tracks upstream module structure.
- #66840 — pinned `pyjwt>=2.11.0`, a transitive dependency of the FAB stack, with
  the breaking combination named at the pin site.
