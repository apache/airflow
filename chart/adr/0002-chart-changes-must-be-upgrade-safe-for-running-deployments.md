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

Rendering a `chart/templates/` diff answers "what does `helm install` produce?" — but
almost nobody running the official chart is doing a fresh install. The common operation
is `helm upgrade` against a release with running schedulers, a StatefulSet with bound
PVCs holding task logs, completed migration Jobs, and workloads mid-flight.

Kubernetes treats many fields as **immutable after creation** — a StatefulSet's
`volumeClaimTemplates` and selector, a Job's `spec`. When a template starts emitting a
different value for one, the upgrade fails or, worse, recreates the resource and
detaches the PersistentVolume behind it (log persistence, Redis broker state), silently.
The reverse failure is a template rendering a field another controller owns: emitting
`spec.replicas` while an HPA scales the same Deployment makes the two oscillate. The
same class covers migration/cleanup Jobs that components wait on via `waitForMigrations`.
None of this shows in a diff or in a single default-values render; the delta between two
renders has to be reasoned about explicitly by the author.

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

- Users can upgrade within a major version without a maintenance window or a manual
  PVC dance — the property that makes the chart usable under GitOps.
- Some desirable template refactors are unavailable outside a major version, even when
  the rendered output is nicer, because the transition is destructive.
- Authors reason about the previous render and often add both a persistence-enabled and
  a persistence-disabled test.

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
- changes default rendering — including by introducing a new value whose default
  takes effect — so that an upgrade with an unchanged values file mutates running
  resources. This is the single home for that rule; ADR 1 defers to it.

A reviewer should reject any chart change whose upgrade behaviour on an existing
release is unstated.

## Evidence

- #41771 / #42946 — the `volumeClaimTemplates` / PVC seam, where this class of bug
  repeatedly surfaced and required consolidated fixes.
- #59955 — PVC retention treated as an explicit, user-controlled property of a stateful
  component.
- #60118 — extended the persistence path additively, without changing what existing
  releases render.
- #63187 — the canonical "don't render a field another controller owns": omit api-server
  `spec.replicas` when HPA is enabled.
- #62054 — migration-gating ordering made explicit and configurable per component.
- #62048 — stateful worker storage added as an opt-in value, not a change to the default
  render.
- #63464 — tightened which resources render under which conditions, so upgrades stop
  emitting objects a release does not need.
