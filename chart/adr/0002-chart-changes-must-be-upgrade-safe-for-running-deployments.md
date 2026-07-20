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

# 2. Chart changes are evaluated against an existing release, not a fresh install

Date: 2026-07-20

## Status

Accepted

## Context

The natural way to review a `chart/templates/` diff is to render it and read the
YAML. That answers "what does `helm install` produce?" — but almost nobody
running the official chart is doing a fresh install. The overwhelmingly common
operation is `helm upgrade` against a release that already has running
schedulers, a StatefulSet with bound PVCs holding task logs, completed migration
Jobs, and workloads mid-flight.

Kubernetes treats many fields as **immutable after creation**. A StatefulSet's
`volumeClaimTemplates` and selector cannot be changed in place; a Job's `spec`
cannot be patched. When a template starts emitting a different value for one of
those, the upgrade does not quietly converge — it either fails outright, or
(worse, when the resource is recreated) detaches or orphans the PersistentVolume
behind it. Log persistence and Redis broker state are the assets at risk, and
losing them is silent until someone looks for the data.

The reverse failure is just as real: templates that render a field another
controller owns. If the chart emits `spec.replicas` while an HPA is scaling the
same Deployment, the two controllers overwrite each other on every reconcile and
the deployment oscillates. The same class of problem covers migration and
database-cleanup Jobs, which components wait on via `waitForMigrations` — a Job
that is not idempotent, or that already exists from the previous release, can
wedge an upgrade with no useful error.

None of this is visible in a diff, and none of it is exercised by rendering the
default values once. The chart's own `chart/tests/helm_tests/` suite renders
templates and asserts on the resulting objects, which catches shape regressions
— but "would this recreate the StatefulSet on an existing release?" is a
question about the *delta between two renders*, and it has to be reasoned about
explicitly by the author.

## Decision

A chart change is assessed for what it does to an **existing** release. A PR
that only demonstrates correct fresh-install rendering has not made its case.
Concretely:

- **Immutable fields are not changed casually.** Anything touching StatefulSet
  `volumeClaimTemplates`, selector labels, or Job `spec` must state the upgrade
  behaviour and, if a recreate is unavoidable, ship a documented migration path
  and a `significant` newsfragment.
- **Stateful resources are never orphaned or force-recreated as a side effect.**
  PVC naming, `persistentVolumeClaimRetentionPolicy`, and the persistence
  on/off branches are treated as data-bearing, not as rendering details.
- **The chart does not render fields owned by another controller** — most
  concretely, `spec.replicas` is omitted when an HPA is enabled.
- **Jobs and hooks stay idempotent and correctly ordered** against the
  components that wait on them, so a re-run upgrade converges rather than
  wedging.
- **New behaviour is opt-in and defaults to the current rendering**, so an
  upgrade that changes nothing in the user's values file changes nothing in the
  cluster.

## Consequences

- Users can upgrade the chart within a major version without a maintenance
  window or a manual PVC dance — the property that makes the chart usable under
  GitOps.
- Some desirable template refactors are simply unavailable outside a major
  version, even when the rendered output is *nicer*, because the transition is
  destructive.
- Authors carry extra work: reasoning about the previous render, and often
  adding a persistence-enabled and a persistence-disabled test rather than one.

A change **violates** this decision when, in `chart/templates/`, it:

- alters a StatefulSet's `volumeClaimTemplates`, selector labels, or another
  immutable field such that an existing release must be recreated, without a
  documented migration and a `significant` newsfragment;
- changes PVC naming or retention behaviour in a way that detaches or orphans an
  existing PersistentVolume;
- emits `spec.replicas` (or another field an autoscaler/controller owns) for a
  workload whose scaling is delegated;
- makes a migration or cleanup Job non-idempotent, or reorders it relative to
  the components gated on `waitForMigrations`;
- changes default rendering so that an upgrade with an unchanged values file
  mutates running resources.

A reviewer should reject any chart change whose upgrade behaviour on an existing
release is unstated.

## Evidence

- #41771 — "Consolidated fix for volumeClaimTemplates (apiVersion and PVC)" and
  #42946 — "Chart: fix VCT for scheduler in local and persistent mode": the
  `volumeClaimTemplates` / PVC seam is where this class of bug repeatedly
  surfaced, and required consolidated rather than piecemeal fixes.
- #59955 — "add redis statefulset persistentVolumeClaimRetentionPolicy support":
  PVC retention treated as an explicit, user-controlled property of a stateful
  component.
- #60118 — "Allow custom volumeClaimTemplates when logs.persistence.enabled is
  true": extended the persistence path additively, without changing what
  existing releases render.
- #63187 — "fix(chart): omit api-server spec.replicas when HPA is enabled": the
  canonical "don't render a field another controller owns" fix.
- #62054 — "Add workers.celery.waitForMigrations section": the migration-gating
  ordering made explicit and configurable per component.
- #62048 — "Add workers.celery.volumeClaimTemplates": stateful worker storage
  added as an opt-in value rather than a change to the default render.
- #63464 — "More restrictive chart rendering logic": tightened which resources
  render under which conditions, so upgrades stop emitting objects a release
  does not need.
