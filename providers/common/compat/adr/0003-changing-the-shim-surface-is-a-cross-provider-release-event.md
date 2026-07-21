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

# 3. Changing the shim surface or the floor is a cross-provider release event

Date: 2026-07-20

## Status

Accepted

## Context

Providers are released independently, each against its own declared
`apache-airflow>=` floor (`providers/adr/0002-…`). Here that ordinary fact
acquires unusual weight: roughly 100 provider `pyproject.toml` files declare
`apache-airflow-providers-common-compat` as a runtime dependency, and every one
declares it as a lower bound — consumers pin `>=`, never a ceiling. So a newly
released `common.compat` is resolved by pip into deployments of providers released
months earlier and never tested against it.

Two kinds of change therefore behave differently here. The **declared floor**:
this provider's `apache-airflow>=2.11.0` is the effective floor of every consumer,
whatever their own pin says — a provider claiming `>=2.11.0` while depending on a
`common.compat` requiring `>=3.0.0` does not work on 2.11, and the break appears at
install-resolution or first import in a user's deployment, not in CI. The **shape
of a re-exported symbol**: because consumers import through the shim, the shim *is*
their API — renaming a map key, changing what a key resolves to on a given core
version, dropping a fallback, or moving a symbol between `common.compat.sdk` and
`common.compat.standard` breaks already-shipped importers, and no `>=` pin can
express a deprecation window. The forward-direction mechanism exists: a consumer
using something only a not-yet-released `common.compat` provides writes
`apache-airflow-providers-common-compat>=X.Y.Z,  # use next version`, and the
release manager resolves the marker when the wave goes out.

## Decision

Treat this provider's floor and its exported surface as a contract with every
consumer, and change either only with the cross-provider consequences worked out
first.

- **Raising `apache-airflow>=` here is a deliberate, announced decision.** It is
  a change to the effective floor of ~100 packages, made in a PR that says so and
  aligned on beforehand (issue or dev list) — never a side effect of needing one
  newer core API. The alternative is almost always a version gate.
- **A re-exported symbol's shape is stable.** Its map key, what it resolves to on
  each supported core version, and the module it is exported from do not change
  without being treated as a breaking change for importers, with the affected
  consumers identified in the PR.
- **Removing a fallback path is a breaking change**, not cleanup, for as long as
  the declared floor still includes the core version that path serves.
- **A consumer adopting a brand-new shim bumps its dependency with the
  `# use next version` marker**, so the release manager resolves it to the
  concrete `common.compat` version at release time and the two packages cannot
  ship in an incompatible pairing.
- **Do not add a ceiling to work around a break.** A consumer pinning
  `common.compat<X` to avoid a surface change is treating the symptom; fix the
  surface change here.
- **The addition path is the safe path.** Prefer adding a new map entry over
  reshaping an existing one; the additive change is invisible to the ninety-nine
  consumers that did not ask for it.

## Consequences

- Consumers can upgrade `common.compat` freely — the property that makes a
  `>=`-only pin across ~100 packages tenable at all.
- Fixing a shim benefits every consumer without any re-releasing — the same
  fan-out that makes a mistake expensive makes a fix cheap.
- Floor bumps become rare and batched, which is why version-gating a new-core API
  is the default and raising the floor is the exception.
- The `# use next version` marker adds a release-process step and a dependency on
  the release manager resolving it — accepted as the price of independent cadences.

A change **violates** this decision when it:

- raises this provider's `apache-airflow>=` floor without stating the
  cross-provider impact and without having considered a version gate instead;
- renames a map key, changes what an existing key resolves to on a supported core
  version, or moves a symbol to a different `common.compat` module, without
  treating it as breaking for importers;
- removes a fallback path that the declared floor still needs;
- lands a consumer-side use of a new shim without bumping that consumer's
  `apache-airflow-providers-common-compat>=` dependency with a
  `# use next version` marker;
- adds an upper bound on `common.compat` in a consumer to route around a surface
  change, instead of fixing the surface change here;
- reshapes an existing entry when adding a new one would have served the same
  consumer without touching the other ninety-nine.

## Evidence

- #68740 — the `# use next version` marker resolved to a concrete floor at release
  time: the mechanism this decision depends on.
- #69218 — the `# use next version` convention made discoverable, because the
  failure it prevents is invisible until release.
- #58612 — a floor move handled as a deliberate, repo-wide event rather than drift.
- #49843 — the same pattern one floor earlier, paired with the branch cleanup it enabled.
- #64933 — a single wrong arm fixed once here and thereby for every consumer: the
  upside of the same concentration that makes surface changes dangerous.
- #60335, #58727 — the additive pattern: a new shim entry adopted by consumers
  afterwards, rather than a reshape.
