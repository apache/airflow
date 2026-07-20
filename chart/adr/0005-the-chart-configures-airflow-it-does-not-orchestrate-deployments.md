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

Every operator running Airflow on Kubernetes has workflow around it: restarting
a component on a schedule, poking an API after a sync completes, bundling a
database, applying site-specific labels, layering their own jobs. Each of those
is a small, plausible pull request against the chart — a boolean, a CronJob
template, one more optional resource.

Accepted uncritically, that class of change turns the chart into a general
deployment framework, and the costs land on everyone:

- **Values grow faster than they can be maintained.** Each knob is permanent
  under ADR 1 and multiplies the combination space that every subsequent
  template change has to be correct across. The chart already renders across
  executors, persistence, KEDA, HPA, pgbouncer and several logging backends.
- **Deployment-specific workflow is where Helm is weakest.** A scheduled restart
  or a post-sync trigger is naturally expressed as an extra manifest, a
  post-render overlay, or the operator's own CronJob — mechanisms that exist,
  need no chart release, and do not constrain anyone else.
- **Bundled infrastructure is a liability.** Shipping a database subchart makes
  the project responsible for upgrade paths and platform compatibility of
  software it does not maintain, on the chart's release cadence rather than the
  dependency's. The chart already carries several such components — the statsd
  exporter, redis, pgbouncer and its exporter, and an OpenTelemetry collector
  with its own Deployment, Service, NetworkPolicy and ServiceAccount under
  `templates/otel-collector/`. Each of those is a standing maintenance
  obligation, and *meeting* that obligation — bumping a base image for a CVE,
  tracking an upstream release — is required work, not scope creep. The liability
  argument is a reason not to take on the *next* one, never a reason to leave the
  existing ones unpatched.
- **These changes are direction decisions wearing a template's clothes.**
  Whether the chart bundles a database, offers restart orchestration, or adopts
  an overlay tool is a question about what the chart is — settled on the devlist
  or in the chart channel, and never inside the review of the pull request that
  assumes the answer.

The chart's own answer to genuine gaps is the opposite direction: extension
points that let operators add their own resources, rather than a value for each
workflow someone might want.

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

- The values surface grows slowly and deliberately, which keeps the combination
  space reviewable and keeps ADR 1's permanence affordable.
- Some operators do more work in their own overlays than they would with a chart
  flag. Several have said this is fine once the alternative is spelled out —
  the answer has to come with the alternative, not as a bare refusal.
- Contributors can invest real effort before the scope question is asked, which
  is why that question belongs at the issue or devlist stage rather than in
  review.
- Documenting the operator-side alternative is part of declining a request; the
  prose docs in `chart/docs/` are where that pattern has to live.
- Whether a question is *currently* under discussion is not something a review
  pass can determine from a diff — there is no enumerable list of live threads.
  It is a reviewer prompt, not a violation: a reviewer who knows of an open thread
  says so and moves the discussion there; an author who knows of one links it.

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

- #61636 — an api-server rollout-restart CronJob: reviewers redirected it to a
  discussion about offering an overlay mechanism instead of adding the
  orchestration to the chart, and it was closed.
- #63677 — a `dags.gitSync.syncToApiServer` boolean, declined; the author
  implemented the behaviour in their own deployment and reported it working
  well, which was the outcome the discussion pointed at.
- #58742 — a bundled postgres subchart upgrade closed because the community had
  already decided on the devlist to remove postgres from the chart entirely;
  challenging that decision belongs on the devlist, not in the pull request.
- #61065 — an exploratory proposal to restructure the chart around an overlay
  tool, kept as a discussion about the chart's shape rather than merged as
  templates.
- #55802 — declining to stop rendering the Redis broker-url secret for non-Celery
  executors: the operator's constraint was real, but the chart keeps rendering
  the object so executor switching keeps working, and the constraint is handled
  on the operator's side.
- #58398 — StatefulSet workers without a PVC, resolved by pointing the author at
  an existing configuration path rather than adding a new deployment mode.
- #65417 — "Update alpine version in pgbouncer and pgbouncer-exporter": the
  counter-case that bounds this decision. Upgrading an image the chart already
  ships is mandatory maintenance, and this ADR does not stand in its way.
