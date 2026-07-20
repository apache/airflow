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

# 3. Pod specs are composed through one fixed precedence chain

Date: 2026-07-20

## Status

Accepted

## Context

A worker pod spec is assembled from inputs owned by three different people. The
**Deployment Manager** writes the `pod_template_file` — the declarative base that
carries the cluster's non-negotiables: service accounts, image pull secrets,
node selectors, tolerations, security contexts, sidecars, resource defaults. The
**executor** contributes the fields that make the pod a *task* pod: the generated
name, the namespace, the identity labels and annotations from ADR 0001, the
command args, and the image. The **Dag author** contributes `pod_override`, via
`executor_config`, for the per-task deviations: more memory, a GPU, a different
image.

`PodGenerator.construct_pod` (`pod_generator.py`) resolves this by folding the
three through `reconcile_pods` in one fixed order —
`[base_worker_pod, dynamic_pod, pod_override_object]` — so later entries win on
scalar fields while list-shaped fields (containers, init containers, volumes,
env) are merged by name via `reconcile_containers` / `merge_objects` /
`extend_object_field` rather than replaced wholesale. `pod_mutation_hook` runs
last, giving the Deployment Manager a final programmatic say. The
KubernetesPodOperator builds its own spec from operator arguments but shares the
same template-file deserialization (`deserialize_model_file`) and the same
reconciliation helpers.

The order is load-bearing in both directions. Because the template is *first*, a
Dag author can raise a memory limit or swap an image without knowing anything
about the cluster's tolerations — those survive untouched. Because
`pod_override` is *last*, a Deployment Manager reading the template file cannot
assume it is the final word; `pod_mutation_hook` is the enforcement point, not
the template. And because list fields merge by name rather than replace, adding
a sidecar in `pod_override` does not silently drop the ones the template
provided (#62284 is precisely that bug, for init containers).

What makes this fragile is that the chain is easy to bypass. Every new pod-spec
capability arrives as a request to set one more field, and the cheapest
implementation is always to poke it onto the pod after `construct_pod` returns,
or to branch on it before reconciliation. Each such shortcut creates a field
whose precedence is different from every other field's: it either cannot be
overridden by `pod_override` when everything else can, or it overrides the
template when nothing else does. The result is not a broken pod — it is a pod
that is *subtly not what the user asked for*, discovered in production when a
toleration or a security context turns out not to have applied.

The chain also has to survive transport. `pod_override` is a
`kubernetes.client.models.V1Pod` that travels from the Dag through serialization
and, under the executor, across a multiprocessing queue — so the objects
involved must remain serializable and picklable in-cluster (#68848). And
`construct_pod` is the single place that enforces the shared constraints:
`POD_NAME_MAX_LENGTH` truncation with a unique suffix, and wrapping
reconciliation and mutation-hook failures as `PodReconciliationError` /
`PodMutationHookException` so a malformed override is reported as a
configuration problem rather than as an opaque Kubernetes rejection.

## Decision

There is exactly one composition path for a worker pod spec, with a fixed and
documented precedence. Concretely:

- **All pod-spec assembly goes through `PodGenerator.reconcile_pods`**, folding
  in the order: pod template file, then executor-generated fields, then
  `pod_override`. No field is set on a pod after the fold, and no field is
  special-cased before it.
- **The precedence is uniform across fields.** A new capability is added as an
  input to the chain — a field on the template, on the dynamic pod, or on the
  override — so that it obeys the same "later wins, lists merge by name" rule as
  everything else.
- **List-shaped fields merge by name, never replace.** Containers, init
  containers, volumes, volume mounts and env vars contributed by the template
  survive an override that adds to them.
- **`pod_mutation_hook` runs last and is the Deployment Manager's enforcement
  point.** Cluster policy that must not be overridable belongs there or in
  admission control, not in the template file.
- **Everything in the chain stays serializable.** `pod_override` and the model
  classes it is built from must survive Dag serialization and the executor's
  queue.
- **Shared constraints are enforced once, in `construct_pod`** — name-length
  truncation, and translating reconciliation and mutation-hook failures into this
  provider's own exception types.
- **New operator parameters are explicit and documented**, not a generic passthrough
  that lets arbitrary spec fragments in through a second door.

## Consequences

- A Dag author can reason about `executor_config` from the documented precedence
  alone, without reading the deployment's template file.
- A Deployment Manager knows exactly which of their settings a Dag author can
  override, and has one place (`pod_mutation_hook`) to make something
  non-negotiable.
- Adding a pod-spec capability is more work than the one-line assignment it
  appears to be: it must be threaded into the chain and tested at each layer.
- `reconcile_*` and `merge_objects` carry per-field-shape knowledge and grow as
  the Kubernetes API grows. That complexity is centralised on purpose — the
  alternative is the same knowledge scattered across call sites.
- Some Kubernetes features cannot be exposed until they can be expressed in the
  template and the override consistently.

A change **violates** this decision when it:

- mutates the pod returned by `construct_pod`, or sets a spec field outside the
  `reconcile_pods` fold;
- introduces a second composition or merge site for pod specs rather than
  extending the existing chain;
- reorders the fold, or gives one field a precedence different from the rest —
  for example making a template value win over `pod_override`, or an override win
  over `pod_mutation_hook`;
- replaces a list-shaped field wholesale where the reconcile helpers merge by
  name, dropping template-provided containers, init containers, volumes or env;
- adds a pod-spec input that cannot survive Dag serialization or the executor's
  multiprocessing queue;
- exposes a new capability as an untyped passthrough of raw spec fragments
  instead of a documented operator parameter or a `pod_override` field;
- bypasses `construct_pod`'s name-length enforcement, or lets a reconciliation /
  mutation-hook failure surface as a raw client error instead of
  `PodReconciliationError` / `PodMutationHookException`.

A reviewer should reject any pod-spec change that does not state, for the field it
touches, who wins when the template file and `pod_override` both set it.

## Evidence

- #62284 — "fix: pod_override existing init_containers": an override dropping
  template-provided init containers — the canonical merge-by-name failure.
- #68848 — "Make cncf.kubernetes model deserialization picklable in-cluster":
  the composition inputs must survive the executor's queue.
- #68713 — "Decide pod_template and image based on Coordinator for lang-SDK tasks
  on KubernetesExecutor": a new selection rule for the template and image
  expressed as an input to the chain.
- #63952 — "Add runtime_class_name to KubernetesPodOperator": a pod-spec
  capability added as an explicit, documented operator parameter.
- #58391 — "fix(kubernetes): Account for job- prefix when truncating job names":
  the name-length constraint enforced centrally, and what happens when a caller
  computes it separately.
- #59347 — "Fix XCom directory creation logic in Kubernetes decorator": spec-side
  scaffolding for the XCom sidecar, which the chain has to compose consistently.
- #69613 — "Allow configuring XCom sidecar container security context": a
  previously hardcoded piece of the composed spec turned into a configurable
  input.
