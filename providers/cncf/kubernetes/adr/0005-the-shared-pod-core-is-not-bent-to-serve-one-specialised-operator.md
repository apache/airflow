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

# 5. The shared pod core is not bent to serve one specialised operator

Date: 2026-07-20

## Status

Accepted

## Context

This provider has a small shared core — `operators/pod.py`, `pod_generator.py`,
`utils/pod_manager.py`, `kubernetes_helper_functions.py`, `triggers/pod.py` —
and a growing set of specialised things layered on top of it:
`SparkKubernetesOperator`, `KubernetesJobOperator`, the resource operators, the
Kueue integration, and whatever comes next. The specialised operators subclass or
delegate to the core; the core knows nothing about them.

That asymmetry is deliberate and it is under constant pressure. The recurring
shape of a rejected change here is: a real defect is observed in one specialised
operator, the author traces it into the shared core, and proposes a change to the
core — how a pod payload is serialised, how a phase is interpreted, how logs are
followed — that makes the specialised case work. The change is usually small and
usually correct for the case that motivated it.

The cost is not visible in the diff. Every task pod in every KubernetesExecutor
deployment, and every `KubernetesPodOperator` task, goes through the same code.
A change that is a one-line accommodation for a Spark payload is a behaviour
change for workloads whose authors have never heard of Spark, on a release they
did not ask for. And because the core has three lifecycle implementations that
already drift (sync operator, deferrable trigger, executor), a core change made
for one integration has to be reasoned about in all three.

The counter-pressure applies in the other direction too: a specialised operator
that quietly reimplements a piece of the lifecycle instead of using the core is
also a defect, because it becomes a fourth copy that will drift. The rule is not
"never touch the core" — it is "touch the core for reasons that are true of every
pod."

## Decision

**A change to the shared pod core must be justified by behaviour that is true of
every pod Airflow launches, not by one operator's workload.**

- **Fix the specialised operator in the specialised operator.** If the payload,
  the CRD, the finish semantics, or the log shape is specific to Spark, to Jobs,
  or to a CRD-based integration, the fix lives in that module.
- **A core change states its blast radius.** Say explicitly, in the PR, what the
  change does to a plain `KubernetesPodOperator` task and to an executor worker
  pod — not only to the workload that motivated it.
- **A core change is applied to every lifecycle copy that needs it, or the PR
  says why not.** Sync operator, deferrable trigger, and executor are three
  implementations of the same lifecycle; a core fix landed in one is an
  incomplete fix.
- **A specialised operator does not fork the core.** Reuse `PodManager`,
  `PodGenerator.reconcile_pods`, the label/annotation helpers and the deletion
  policy rather than reimplementing them; a divergent copy is the failure this
  decision also guards against.
- **A brand-new integration (a new CRD, a new workload type) is a discussion
  first.** It arrives with its owner, its tests, and its own module — it does not
  arrive as a set of hooks threaded into the core.

## Consequences

- The code every deployment runs stays reviewable by people who do not know the
  specialised workloads, and its behaviour does not shift under them.
- Specialised integrations carry their own complexity, which makes them easier to
  evolve, deprecate, or hand to a different maintainer.
- The honest cost: a fix inside a specialised operator sometimes duplicates a few
  lines of core logic, and the author has to argue for the local fix rather than
  making the obvious one-line core change. Some defects therefore take a second
  round to land. That is the price of not making every pod pay for one workload.
- Genuinely general defects found through a specialised operator still belong in
  the core — this decision asks for the argument, not for a refusal.

A change **violates** this decision when it:

- modifies `operators/pod.py`, `pod_generator.py`, `utils/pod_manager.py`,
  `kubernetes_helper_functions.py` or `triggers/pod.py` and the only motivating
  evidence is one specialised operator or one CRD;
- changes how a pod spec or payload is built/serialised to accommodate a single
  workload's expectations;
- lands a core lifecycle fix on exactly one of the sync / deferrable / executor
  paths without saying why the others are unaffected;
- adds a specialised operator (or extends one) that reimplements pod naming,
  label selection, log following, or deletion policy instead of using the core
  helpers;
- introduces a new workload integration inside this provider without a prior
  issue or dev-list agreement on ownership and scope.

## Evidence

- #55645 — "Fix Airflow 3 Spark k8s pod JSON payload corruption": closed. The
  review position was that changing the shared path fundamentally for one use
  case is disproportionate, and that the fix should be local to the Spark code,
  with an offer to review that follow-up.
- #52051 — "fix xcom push not working in `SparkKubernetesOperator`" and #56399 —
  "Fix `SparkKubernetesOperator` deferrable mode launcher attribute error": both
  closed unmerged; the same integration repeatedly surfacing lifecycle gaps that
  had to be re-scoped locally.
- #55355 — "Fix `KubernetesJobOperator` ignoring `on_finish_action`": closed; a
  specialised operator that had drifted from the core's deletion contract rather
  than a core defect.
- #63938 — "Add KubeRay (RayJob) operator": closed; a new CRD integration
  proposed directly into this provider without prior agreement on ownership.
- #63946 — "Add resource quota awareness to `KubernetesPodOperator`" and #61637 —
  "Add `delete_pods_in_phase` override to `KubernetesPodOperator`": closed;
  cluster-policy behaviour pushed into the operator every pod goes through.
- #63042 — a provider-wide sweep adding HTTP timeouts, closed in favour of
  per-provider splits because a blanket value cannot know which calls
  legitimately block;
  the same reasoning applies to blanket changes inside this provider's core.
- #61778 — "Kubernetes Pod Operator handle container registry rate limits":
  closed; transient cluster-side conditions are surfaced as task failures for
  Airflow's own retry mechanism rather than absorbed by a new wait loop inside
  the operator.
