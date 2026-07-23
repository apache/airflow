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

# 4. Behaviour changes to vendored Flask-AppBuilder code go upstream first

Date: 2026-07-20

## Status

Accepted

## Context

`auth_manager/security_manager/override.py` is a transplant: ~2,700 lines of FAB's
`BaseSecurityManager` behaviour copied from one exact released version and adapted
only where Airflow genuinely diverges. ADR 1 records why the pin, the alignment
test, and this file move together.

This ADR records the consequence reviewers spend their time enforcing and
contributors are most surprised by: **the direction of change.** A patch here with
no upstream counterpart does not stay applied — the next bump re-reads the upstream
method and the local improvement silently disappears, or worse survives as an
unexplained divergence the alignment test cannot see (it detects shape drift, not
behavioural drift inside a method body). A change like "make `ab_user.changed_on`
update on modification" is a plausible small fix to persistence behaviour FAB owns;
landing it here makes Airflow behave differently from every other FAB app, recorded
nowhere but a merged diff, until the next bump reverts it.

This does *not* apply to the seams Airflow owns: the subclass methods implementing
Airflow's own authorization, the Airflow-specific resource/permission sync, and
defects that exist only because of how Airflow calls FAB (a missing submodule
import, a wrong session lifecycle in Airflow's request path). Those are local by
construction and fixed here.

## Decision

**A change to vendored FAB behaviour is proposed to Flask-AppBuilder upstream,
released there, and picked up on the next version bump — not written directly
into `override.py`.**

- **Behaviour that FAB owns is fixed in FAB.** User/role/permission CRUD
  semantics, the authentication backends' own logic (DB, LDAP, OAuth, OID,
  REMOTE_USER), and the persistence behaviour of the `ab_*` models are upstream's
  contract. The Airflow PR is the bump that brings the released fix in.
- **Only Airflow-owned divergence is written here** — the parts that exist
  because Airflow's auth manager, its Dag-level resources, or its request
  lifecycle differ from a stock FAB app.
- **A deliberate divergence is labelled at the site.** If a divergence is
  necessary and cannot go upstream, the code says so in a comment, naming what
  upstream does and why Airflow differs, so the next bump reconciles it
  intentionally instead of reverting it by accident.
- **State the upstream status in the PR.** Every diff touching `override.py`
  says whether upstream has the same behaviour, diverges deliberately, has an
  open PR, or has not been reached yet. A PR that does not say is not reviewable.
- **A security fix is the exception that proves the rule** and is handled through
  the ASF security process, not as a public PR — an urgent local patch may
  precede the upstream change, and then it is documented as such.

## Consequences

- The vendored file stays a faithful transplant, so bumps remain a mechanical
  reconciliation rather than a merge of two divergent histories.
- Fixes benefit every Flask-AppBuilder user, which is also why upstream accepts
  them.
- The cost is high and falls on the contributor: a small fix becomes an upstream
  PR, an upstream release, then an Airflow bump — the most expensive change in this
  provider — so real defects stay open longer. Accepted; the alternative is a
  vendored file that corresponds to nothing and a bump nobody can perform safely.
- Contributors must be told this *before* they write the patch, hence the
  provider's `AGENTS.md` guard as well.

A change **violates** this decision when it:

- edits a method in `override.py` that mirrors upstream FAB, to alter its
  behaviour, without a corresponding upstream change or an explicit statement of
  deliberate divergence;
- changes `ab_*` model definitions, defaults, or persistence side effects that
  Flask-AppBuilder owns;
- alters an authentication backend's own logic (LDAP search/bind semantics,
  OAuth token handling, REMOTE_USER resolution) rather than how Airflow invokes
  it;
- introduces an unexplained divergence from the pinned upstream and relies on
  `test_fab_alignment.py` passing as evidence that it is safe;
- silences the alignment test by adding an `AUDITED_EXCLUSIONS` entry without a
  reason, instead of implementing or reconciling the upstream method.

## Evidence

- #60448 — `ab_user.changed_on` not updating: closed with the rule stated in full —
  behaviour FAB owns belongs in a PR to Flask-AppBuilder, picked up on the next
  bump, not written into the override.
- #67535 — `module ldap has no attribute filter`: closed, then landed as #68226.
  The discriminator: an *Airflow-side* import defect (a submodule the vendored code
  assumed imported), not FAB behaviour, so the local fix was correct — closed for
  process, not direction.
- #65462, #65463, #67141 — near-identical PRs for a FAB 3.6.0 cookie defect and its
  Werkzeug 3.0 variant, all closed: breakage originating in third-party releases;
  the durable fix travels with the pin, not a local patch.
- #69706 — bump to 5.2.2, closed: a bump without the reconciled `override.py` is
  not reviewable (ADR 1).
- #61083 — Admin access denied for user model views, closed: cause was upstream's
  registration-driven permission creation, and the local workaround synthesised
  permission rows in Airflow — the shape this decision routes upstream.
