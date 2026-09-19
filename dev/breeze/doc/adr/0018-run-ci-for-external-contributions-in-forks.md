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

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [18. Run CI for external contributions in contributor forks](#18-run-ci-for-external-contributions-in-contributor-forks)
  - [Status](#status)
  - [Context](#context)
  - [Decision](#decision)
  - [Consequences](#consequences)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 18. Run CI for external contributions in contributor forks

Date: 2026-09-19

## Status

Proposed (pending the outcome of the AIP-120 vote; the implementation ships dormant)

## Context

[AIP-120](https://cwiki.apache.org/confluence/spaces/AIRFLOW/pages/451974711/AIP-120+Run+CI+for+external+contributions+in+contributor+forks)
proposes that pull requests from contributors who are not committers, collaborators or
members of an exempted organisation get their CI feedback from GitHub Actions running in the
contributor's own fork, and that project CI in ``apache/airflow`` is reserved for the rest.
The motivation is in the AIP: Airflow is routinely the largest consumer of the shared ASF
GitHub Actions pool by job count, roughly a fifth of externally authored pull requests close
without merging after consuming CI and reviewer attention, and the wait maintainers observe on
their own pull requests is queueing in that shared pool, not execution time. GitHub Actions is
free and unmetered on public repositories, so the cost moved to the contributor is elapsed
time on a 20-job concurrency cap, not money.

Mechanising this touches four places at once: the trigger and selective-check logic of the
main test workflow, an automated lifecycle for gated pull requests, a way for contributors
to check their setup before pushing, and the contribution documentation. The design is in
[Fork CI for external contributors](../ci/08_fork_ci_for_external_contributors.md). This
record captures the decisions that shape it and the alternatives rejected.

## Decision

1. **One eligibility function, one configuration file.** ``fork_ci_gate.decide`` in breeze is
   a pure, standard-library-only function over the fields both the workflow event and the
   GraphQL API expose. Its configuration is ``.github/fork-ci-gate.toml`` in the repository:
   an ``enabled`` switch, an ``enabled_since`` cut-off, ``close_after_days`` and the
   exempted-organisation allowlist. A repository file rather than an Actions variable because
   ASF projects cannot self-serve variables and exemption changes deserve a reviewed pull
   request. The gate reads the file from the base branch so a pull request cannot alter its
   own eligibility. Two labels that only people with triage rights can set override the
   automatic decision per pull request: ``use project ci`` and ``use fork ci``. They take
   effect whether or not the gate is enabled, which makes ``use fork ci`` the pilot switch.

2. **Project CI is stopped by a gate job, not by cancellation.** ``ci-amd.yml`` gets a
   ``gate`` job that executes the eligibility file directly with the runner's ``python3`` and
   makes ``build-info`` conditional on it. Cancelling the run is impossible with the read-only
   token a fork pull request gets, and failing the run would attach a misleading red status.
   Skipping is silent and cheap.

3. **Fork pushes run CI with pull-request semantics.** The push trigger becomes ``'**'`` and
   the previous branch patterns move into the eligibility function so behaviour in
   ``apache/airflow`` is unchanged. In a fork, selective checks compute the change set with the
   cross-repository compare API against ``apache/airflow`` (merge-base semantics, no fork sync
   required), do not force the full matrix the way a ``push`` event does today, and never
   select self-hosted runners. Without this a fork push would run the whole matrix, ~220
   minutes on 20 runners, for every push.

4. **All pull request mutations come from one scheduled, idempotent reconciler.**
   ``breeze ci fork-ci-reconcile`` runs every 15 minutes, discovers newly opened gated pull
   requests through a 24-hour overlapping window, and drives each labelled pull request
   through a small state machine: label, draft, undraft only what it drafted, add or remove
   ``ready for maintainer review``, keep one edited state comment, post a fresh comment for
   the two moments that need attention (workflows enabled, closed), publish the fork CI state
   as a commit status on the head SHA (red / yellow / green, linking to the run in the
   fork), and close after seven days without fork CI activity. Commit statuses were chosen
   over check runs because a check run created with the workflow token links to the creating
   workflow instead of the fork run. ``pull_request_target`` was rejected: it would react to
   untrusted events with write permissions for a gain of a few minutes of latency.

5. **Unknown failures are surfaced, expected states are not.** The reconciler catches errors
   per pull request, continues, and posts everything it could not classify to the
   ``internal-airflow-ci-cd`` Slack channel through the same action and payload convention
   as the CI duration monitor.

6. **``breeze ci audit`` is the contributor's pre-flight.** It checks ``gh``, the
   ``upstream``/``origin`` remote convention, fork existence, who the user is to the project,
   whether the fork's ``Tests (AMD)`` workflow is enabled (externals) or disabled
   (committers and collaborators, whose pull requests run on project CI and whose forks
   cannot be made private), fork ``main`` freshness, and the current branch. ``--fix`` runs
   the offered commands after confirmation.

7. **Ship dormant, pilot by label.** Everything merges with ``enabled = false``. Committers
   pilot the machinery on their own pull requests with ``use fork ci``. Switching on is one
   pull request that sets ``enabled_since`` and ``enabled``. Rollback is the reverse. The backlog
   reset in AIP-120 §9.1 is not implemented here; if adopted it is ``enabled_since`` moved
   back in steps.

## Consequences

- External contributors run one command once per fork,
  ``gh workflow enable ci-amd.yml -R <fork>``, and get CI on every push. Their pull request
  is a draft until the fork run is green, then it is undrafted and labelled for review. The
  ``Fork CI / Tests (AMD)`` status on the pull request shows red, yellow or green and links to
  the fork run, so nobody has to open the fork to know where things stand. A pull request
  that never goes green is closed after a week with an invitation to reopen.
- A committer or collaborator can move any single pull request between the two regimes with
  a label and a re-run, without touching configuration.
- Maintainers see only pull requests that passed in the author's fork in the
  ``ready for maintainer review`` queue. Project CI jobs drop by the volume of gated pull
  requests, which shortens the shared queue for everyone.
- Every pull request in ``apache/airflow`` pays a gate job of a few seconds before
  ``build-info``. Pushes to ad-hoc branches in ``apache/airflow`` create a gate-only run.
- The ``pr-management-triage`` skill must treat gated pull requests as out of scope for
  drafting, the review label and inactivity closing; the reconciler owns those.
- A pull request can edit ``ci-amd.yml`` to remove the gate. This is visible in review and
  bounded to one pull request's CI; it is accepted rather than solved with
  ``pull_request_target``.
- Drafting and the setup comment arrive within a reconciler cadence, not instantly.
- ``COMMITTERS`` in ``global_constants.py`` is not the source of trust for gating; GitHub's
  author association is. That list continues to drive runner selection only.
