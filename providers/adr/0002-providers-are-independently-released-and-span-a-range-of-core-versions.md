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

# 2. Providers are released independently and must work across a range of core versions

Date: 2026-07-20

## Status

Accepted

## Context

Providers are not part of an Airflow core release. Each of the ~100 provider
distributions carries its own version history in `provider.yaml` and is released
from `main` in waves unrelated to the core release train. A user installs a
provider at whatever version PyPI offers, on whatever core they run. That range
is wide: ~95 providers declare `apache-airflow>=2.11.0`, a handful declare
`>=3.0.0` or a patch floor, at least one carries an exclusion
(`>=3.0.0,!=3.1.0`). The declared floor is a promise — a change reaching for a
later-core API does not fail at install time, it fails at import or task-run time
in someone else's deployment, after release.

Two mechanisms keep that promise. Per-provider `version_compat.py` exposes
`AIRFLOW_V_3_0_PLUS` / `AIRFLOW_V_3_1_PLUS`-style constants and is **copied** into
each provider, not imported — `run_check_imports_in_providers.py` rejects a
`version_compat` import resolving anywhere but the importing provider's own root,
so a compat gate cannot couple two independently released packages. `common.compat`
carries the shims that genuinely need sharing. Because the release train is
detached from core, the release manager regenerates each provider's changelog
from `git log`; per-PR newsfragments (towncrier) are not read for providers. The
only surviving channel for a user-facing note is
`providers/<provider>/docs/changelog.rst`.

## Decision

Treat the declared core floor and the independent release cadence as binding
constraints on every provider change.

- **Do not silently require a newer core than the provider declares.** If a
  change needs a newer Airflow, either gate it or raise the
  `apache-airflow>=X.Y.Z` floor explicitly and say so in the PR.
- **Gate new-core behaviour through the provider's own `version_compat.py`.**
  Copy the file into the provider if it is not there; never import compat
  constants from another provider or from test helpers.
- **Share compat shims only through `common.compat`**, which exists for exactly
  that purpose — not by importing a helper out of an unrelated provider.
- **Never add a newsfragment under `providers/`, and never add one for a
  provider-only change.** A user-facing note goes into
  `providers/<provider>/docs/changelog.rst`, directly under the `Changelog`
  header, as the in-file `NOTE TO CONTRIBUTORS` block describes. Routine entries
  are collected by the release manager from commit messages, so most PRs touch no
  changelog at all.
- **Call breaking changes out explicitly**, in the PR description and in the
  changelog when users need migration guidance. A behaviour change that ships
  unannounced on an independent release is indistinguishable from a regression.

## Consequences

- A user can upgrade a single provider without upgrading Airflow, and vice versa.
- Providers carry version-gated branches and a copied `version_compat.py`; the
  duplication is accepted in exchange for decoupled release cycles.
- Reviewers must evaluate a change against the *declared floor*, not against the
  `main` checkout CI runs on — CI passing on latest core is not evidence the floor
  still works.
- Release notes stay accurate without per-PR newsfragment bookkeeping, at the cost
  of requiring good commit messages.

A change **violates** this decision when it:

- uses a core API, attribute, or import path that does not exist on the
  provider's declared `apache-airflow>=X.Y.Z` floor, without a version gate and
  without raising the floor;
- imports `version_compat` constants from another provider, from `airflow`, or
  from test helpers instead of the provider's own copied `version_compat.py`;
- reaches into an unrelated provider for a compat shim rather than using
  `common.compat`;
- adds a newsfragment for a provider-only change, or adds one **under
  `providers/`** at all — the prohibition is about the directory, not the word.
  A PR whose substance is in `airflow-core/`, `chart/` or `dev/mypy/` and which
  happens to touch a provider file still puts its newsfragment in that
  distribution's own `newsfragments/`, which is correct and is not this bullet;
- changes user-visible behaviour or removes a public argument, operator, or hook
  without flagging it as breaking and giving migration guidance.

## Evidence

- #68926 — new-core capability adopted behind a version gate, not by raising the floor.
- #67726 — provider behaviour that must be correct per core version (Airflow 3.2+).
- #69919 — a provider break appearing on only one point of the supported range.
- #69333 — provider imports fine on latest, breaks at its declared floor.
- #60031 — a breaking provider change labelled as such, note in the changelog.
- #67280 — the provider changelog treated as the load-bearing user-facing record.
- #69208 — a shared compat shim landing in `common.compat`.
