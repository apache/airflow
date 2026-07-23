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

# 5. The chart configures Airflow; it does not orchestrate deployments

Date: 2026-07-20

## Status

Accepted

## Context

Every operator running Airflow on Kubernetes has workflow around it — restarting a
component on a schedule, poking an API after a sync, bundling a database. Each is a
small, plausible PR: a boolean, a CronJob template, one more optional resource. Accepted
uncritically, that class of change turns the chart into a general deployment framework,
and the costs land on everyone:

- **Values grow faster than they can be maintained.** Each knob is permanent under ADR 1
  and multiplies the combination space every template change must be correct across.
- **Deployment-specific workflow is where Helm is weakest** — a scheduled restart is
  naturally an extra manifest, a post-render overlay, or the operator's own CronJob, none
  of which need a chart release or constrain anyone.
- **Bundled infrastructure is a liability.** A database subchart makes the project
  responsible for a dependency's upgrade path on the chart's cadence. The chart already
  carries such components — the statsd exporter, redis, pgbouncer and its exporter, and an
  OpenTelemetry collector under `templates/otel-collector/`. *Meeting* that obligation —
  bumping a base image for a CVE, tracking an upstream release — is required work, not
  scope creep. The liability argument is a reason not to take on the *next* one, never a
  reason to leave existing ones unpatched.
- **These changes are direction decisions wearing a template's clothes.** Whether the
  chart bundles a database or offers restart orchestration is a question about what the
  chart is — settled on the devlist or in the chart channel, not inside the review of a
  PR that assumes the answer.

The chart's own answer to genuine gaps is the opposite: extension points that let
operators add their own resources, rather than a value per workflow.

## Decision

- **A new value must be justified against what the operator can already do
  outside the chart.** If an extra manifest, a post-render overlay, or the
  operator's own scheduled job covers it, that is the answer.
- **The chart does not take on scheduling or lifecycle orchestration** beyond
  what running Airflow itself requires — migration gating, log grooming and
  cleanup of the chart's own resources.
- **The chart does not take on *new* third-party infrastructure** — a new subchart
  or a newly bundled image it would then be responsible for maintaining and
  upgrading on its own cadence. Keeping the versions of components the chart
  already bundles current, including security-driven image-tag bumps, is expected
  maintenance and is welcome.
- **Structural and scope questions go to the devlist or the chart channel before
  any template is written**, and a pull request does not re-litigate a decision
  already taken there — the challenge belongs in the same forum. Where the author
  knows of a live thread on the question, the pull request links it.
- **Where a gap is real, prefer a general extension point** over a
  purpose-specific boolean, so one mechanism serves the next request too.

## Consequences

- The values surface grows slowly, keeping the combination space reviewable and ADR 1's
  permanence affordable.
- Some operators do more work in their own overlays; several have said this is fine once
  the alternative is spelled out — the answer comes with the alternative, not a bare
  refusal, and the prose docs in `chart/docs/` are where that pattern lives.
- Contributors can invest effort before the scope question is asked, which is why it
  belongs at the issue or devlist stage, not in review.
- Whether a question is *currently* under discussion is not something a review pass can
  determine from a diff — there is no enumerable list of live threads. It is a reviewer
  prompt, not a violation: a reviewer who knows of an open thread says so and moves the
  discussion there; an author who knows of one links it.

A change **violates** this decision when it:

- adds a value whose only purpose is to trigger, schedule or sequence something
  the operator could run themselves alongside the release;
- introduces a CronJob, hook or controller-like resource unrelated to Airflow's
  own operation;
- adds a **new** third-party component to the chart's dependency set — a subchart,
  or a new bundled image — that the chart would then have to maintain (upgrading a
  component the chart already bundles, such as the pgbouncer or statsd-exporter
  image, is maintenance and is not a violation);
- reverses a direction already settled on the devlist or in the chart channel
  without going back to that forum, or links a live thread on the question and
  implements the outcome it has not reached yet;
- adds a single-purpose boolean where an existing extension point, or a general
  one, would serve the same need.

## Evidence

- #61636 — an api-server rollout-restart CronJob redirected to a discussion about an
  overlay mechanism and closed.
- #63677 — a `dags.gitSync.syncToApiServer` boolean declined; the author implemented it
  in their own deployment and reported it working, the outcome the discussion pointed at.
- #58742 — a bundled postgres subchart upgrade closed because the community had already
  decided on the devlist to remove postgres; challenging that belongs on the devlist.
- #61065 — an overlay-tool restructuring proposal kept as a discussion about the chart's
  shape rather than merged as templates.
- #55802 — declining to stop rendering the Redis broker-url secret for non-Celery
  executors: the chart keeps rendering the object so executor switching keeps working.
- #58398 — StatefulSet workers without a PVC resolved by pointing at an existing config
  path rather than a new deployment mode.
- #65417 — the counter-case: upgrading the pgbouncer / pgbouncer-exporter alpine version
  is mandatory maintenance, and this ADR does not stand in its way.
