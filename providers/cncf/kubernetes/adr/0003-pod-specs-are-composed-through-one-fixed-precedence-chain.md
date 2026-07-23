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

A worker pod spec is assembled from inputs owned by three people. The **Deployment
Manager** writes the `pod_template_file` — the declarative base carrying the
cluster's non-negotiables (service accounts, image pull secrets, node selectors,
tolerations, security contexts, sidecars, resource defaults). The **executor**
contributes what makes the pod a *task* pod: the generated name, namespace,
identity labels/annotations (ADR 0001), command args, and image. The **Dag
author** contributes `pod_override`, via `executor_config`, for per-task
deviations. `PodGenerator.construct_pod` folds the three through `reconcile_pods`
in one fixed order — `[base_worker_pod, dynamic_pod, pod_override_object]` — so
later entries win on scalar fields while list-shaped fields merge by name via
`reconcile_containers` / `merge_objects` / `extend_object_field`; `pod_mutation_hook`
runs last. The KubernetesPodOperator builds its own spec but shares the same
deserialization (`deserialize_model_file`) and reconciliation helpers.

The order is load-bearing both ways: the template being *first* lets a Dag author
raise a limit without knowing the cluster's tolerations; `pod_override` being
*last* means the template is not the final word (`pod_mutation_hook` is the
enforcement point); list-merge-by-name means adding a sidecar in `pod_override`
does not drop the template's (#62284 is exactly that bug for init containers). The
chain is easy to bypass — every new capability arrives as "set one more field", and
the cheapest implementation is to poke it onto the pod after `construct_pod` or
branch before reconciliation, creating a field whose precedence differs from every
other's. The result is not a broken pod but one *subtly not what the user asked
for*, found in production when a toleration or security context did not apply. The
chain also has to survive transport — `pod_override` is a `V1Pod` that travels
through serialization and, under the executor, a multiprocessing queue (#68848) —
and `construct_pod` is the single place enforcing `POD_NAME_MAX_LENGTH` truncation
and wrapping failures as `PodReconciliationError` / `PodMutationHookException`.

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
- A Deployment Manager knows which settings a Dag author can override, and has one
  place (`pod_mutation_hook`) to make something non-negotiable.
- Adding a pod-spec capability is more work than the one-line assignment it
  appears to be: it must be threaded into the chain and tested at each layer.
- `reconcile_*` and `merge_objects` carry per-field-shape knowledge, centralised
  on purpose rather than scattered across call sites.
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

- #62284 — an override dropping template-provided init containers: the canonical
  merge-by-name failure.
- #68848 — the composition inputs must survive the executor's queue.
- #68713 — a new template/image selection rule expressed as an input to the chain.
- #63952 — a pod-spec capability added as an explicit, documented operator parameter.
- #58391 — the name-length constraint enforced centrally.
- #59347 — XCom-sidecar scaffolding the chain has to compose consistently.
- #69613 — a previously hardcoded piece of the composed spec turned into a
  configurable input.
