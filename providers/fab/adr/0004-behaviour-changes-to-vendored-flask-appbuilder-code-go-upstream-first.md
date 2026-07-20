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

`auth_manager/security_manager/override.py` is not Airflow code that happens to
resemble Flask-AppBuilder. It is a transplant: roughly 2,700 lines of FAB's
`BaseSecurityManager` behaviour, copied from one exact released version and
adapted only where Airflow genuinely diverges. ADR 1 records why the pin, the
alignment test and this file move together.

This ADR records the consequence that reviewers actually spend their time
enforcing, and the one contributors are most often surprised by: **the direction
of change.** A patch applied here that has no upstream counterpart does not stay
applied. The next FAB bump re-reads the upstream method, reconciles it, and the
local improvement silently disappears — or, worse, survives as an unexplained
divergence that the alignment test cannot see, because the test detects shape
drift (missing or added public methods), not behavioural drift inside a method
body.

The failure mode is concrete. A change like "make `ab_user.changed_on` update
when a user is modified" is a plausible, small, well-intentioned fix to
persistence behaviour that FAB owns. Landing it here means Airflow now behaves
differently from every other FAB application, for a reason recorded nowhere but a
merged diff, until the next bump quietly reverts it.

The same reasoning does *not* apply to the seams Airflow owns: the subclass
methods that implement Airflow's own authorization, the Airflow-specific
resource/permission synchronisation, and defects that only exist because of how
Airflow calls FAB (a missing submodule import, a wrong session lifecycle in
Airflow's request path). Those are local by construction and are fixed here.

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
- Fixes benefit every Flask-AppBuilder user, not only Airflow, which is also why
  upstream tends to accept them.
- The cost is bluntly high and falls on the contributor: a small fix becomes an
  upstream PR, an upstream release, and then an Airflow bump — which is itself
  the most expensive kind of change in this provider. Real defects therefore stay
  open longer than they would if Airflow patched locally. That is accepted; the
  alternative is a vendored file that no longer corresponds to anything and a
  bump nobody can perform safely.
- Contributors need to be told this *before* they write the patch, which is why
  it is stated in this provider's `AGENTS.md` guard as well as here.

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

- #60448 — "Fix `ab_user.changed_on` not updating on user modifications":
  closed with the rule stated in full — Airflow does not modify the security
  manager override on its own; changes are backported from FAB on each version
  update, and a behaviour proposal belongs in a PR to the Flask-AppBuilder
  repository, to be picked up when Airflow moves to that release.
- #67535 — "Fix `AttributeError`: module `ldap` has no attribute `filter`":
  closed, then the same one-line change landed as #68226. The contrast is the
  discriminator: this was an *Airflow-side* import defect (a submodule the
  vendored code assumed was imported), not FAB behaviour, so the local fix was
  correct — the PR was closed for process reasons, not for direction.
- #65462 and #65463 — two near-identical PRs for the same FAB 3.6.0 cookie
  defect, and #67141 for the Werkzeug 3.0 variant of it: all closed unmerged.
  Breakage in this file originates in third-party releases, and the durable fix
  travels with the pin rather than with a local patch.
- #69706 — "Bump flask-appbuilder to 5.2.2": closed unmerged. A bump is not a
  dependency edit; without the reconciled `override.py` it is not a candidate for
  review (ADR 1).
- #61083 — "Fix Admin access denied for user model views": closed. The
  investigation showed the cause was upstream's registration-driven permission
  creation, and the proposed local workaround synthesised permission rows in
  Airflow instead — the shape this decision routes upstream.
