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
distributions carries its own version history in `provider.yaml`, and they are
released from `main` in waves on a cadence that has nothing to do with the core
release train. A user installs `apache-airflow-providers-google` at whatever
version PyPI offers, on whatever Airflow core they happen to be running.

That combination is genuinely wide. Roughly 95 providers declare
`apache-airflow>=2.11.0` in their `pyproject.toml`; a handful declare `>=3.0.0`
or a specific patch floor, and at least one carries an exclusion
(`>=3.0.0,!=3.1.0`). The declared floor is a promise: the provider is expected to
install and work on that core version. A change that reaches for an API
introduced in a later core does not fail at install time — it fails at import or
at task-run time, in someone else's deployment, after the release.

Two mechanisms exist to keep that promise. The per-provider `version_compat.py`
exposes `AIRFLOW_V_3_0_PLUS` / `AIRFLOW_V_3_1_PLUS`-style constants derived from
the installed `airflow.__version__`, and it is **copied** into each provider
rather than imported — the file says so in a comment, and
`run_check_imports_in_providers.py` rejects a `version_compat` import that
resolves anywhere but the importing provider's own root, precisely so that a
compat gate does not create a hidden dependency between two independently
released packages. The `common.compat` provider carries the shims that genuinely
need to be shared.

Because the release train is detached from core, the release manager regenerates
each provider's changelog from `git log` at release time. Per-PR newsfragments —
the towncrier mechanism used by `airflow-core/`, `chart/`, and `dev/mypy/` — are
simply not read for providers. The only channel for a user-facing note that
survives is `providers/<provider>/docs/changelog.rst`, edited directly under the
`Changelog` header.

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
- **Never add a newsfragment for a provider.** A user-facing note goes into
  `providers/<provider>/docs/changelog.rst`, directly under the `Changelog`
  header, as the in-file `NOTE TO CONTRIBUTORS` block describes. Routine entries
  are collected by the release manager from commit messages, so most PRs touch no
  changelog at all.
- **Call breaking changes out explicitly**, in the PR description and in the
  changelog when users need migration guidance. A behaviour change that ships
  unannounced on an independent release is indistinguishable from a regression.

## Consequences

- A user can upgrade a single provider without upgrading Airflow, and upgrade
  Airflow without being forced to move every provider in lockstep.
- Providers carry version-gated branches and a copied `version_compat.py` — the
  duplication is accepted deliberately, in exchange for not coupling release
  cycles.
- Reviewers must evaluate a change against the *declared floor*, not against the
  `main` checkout CI happens to run on; CI passing on latest core is not evidence
  the floor still works.
- Release notes stay accurate without per-PR newsfragment bookkeeping, at the
  cost of requiring good commit messages.

A change **violates** this decision when it:

- uses a core API, attribute, or import path that does not exist on the
  provider's declared `apache-airflow>=X.Y.Z` floor, without a version gate and
  without raising the floor;
- imports `version_compat` constants from another provider, from `airflow`, or
  from test helpers instead of the provider's own copied `version_compat.py`;
- reaches into an unrelated provider for a compat shim rather than using
  `common.compat`;
- adds a newsfragment for a provider change, or puts a user-facing note anywhere
  other than `providers/<provider>/docs/changelog.rst`;
- changes user-visible behaviour or removes a public argument, operator, or hook
  without flagging it as breaking and giving migration guidance.

## Evidence

- #68926 — "Use task state store for `common.ai` durable execution on Airflow
  3.3+": new-core capability adopted behind a version gate rather than by raising
  the floor for everyone.
- #67726 — "Fix `TriggerDagRunOperator` `fail_when_dag_is_paused` on Airflow
  3.2+": provider behaviour that has to be correct per core version, not just on
  latest.
- #69919 — "Fix Celery worker crash on Airflow 3.0 with `json_logs`": a provider
  break that only appeared on one point of the supported core range.
- #69333 — "Fix Microsoft WinRM provider import failure under lowest
  dependencies": the failure mode this ADR guards against — a provider that
  imports fine on latest and breaks at its declared floor.
- #60031 — "[breaking] Make pyspark-client as default and pyspark package
  optional": a breaking provider change labelled as such, with the user-facing
  note carried in the provider's changelog.
- #67280 — "Fix wrong changelog entry for `BigQueryInsertJobOperator` in google
  provider 22.0.0": the provider changelog treated as the load-bearing user-facing
  record it is.
- #69208 — "Added `get_async_hook` in `common.compat` provider": a shared compat
  shim landing in `common.compat`, the designated home for cross-provider compat.
