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

# 1. A task's pod is uniquely identifiable and reattachable

Date: 2026-07-20

## Status

Accepted

## Context

A Kubernetes pod outlives the Airflow process that created it. The scheduler
running `KubernetesExecutor` can be restarted, rescheduled onto another node, or
scaled to several replicas; a worker running `KubernetesPodOperator` can be
killed mid-task. In every one of those cases the pod keeps running, and Airflow
has to find it again from nothing but the Kubernetes API.

There is no stable name to find it by. `create_unique_id` and
`add_unique_suffix` (`kubernetes_helper_functions.py`) slugify `dag_id` and
`task_id`, truncate the result to `POD_NAME_MAX_LENGTH`, and append a random
suffix — because Kubernetes object names are DNS-label-constrained and Airflow
identifiers are not. Two different tasks can collapse onto the same truncated
prefix, and the same task produces a different name on every launch. The name is
a display handle, not a key.

The identity therefore lives in metadata. `build_labels_for_k8s_executor_pod`
(`pod_generator.py`) and `KubernetesPodOperator._get_ti_pod_labels`
(`operators/pod.py`) stamp `dag_id`, `task_id`, `run_id`, `map_index` and
`try_number` onto the pod as labels — each passed through `make_safe_label_value`
so it survives Kubernetes' label-value grammar — and the executor additionally
writes them as annotations, which `annotations_to_key` reverses back into a
`TaskInstanceKey`. Labels are what a selector can query; annotations are what
survives without the grammar restrictions. Ownership is a label too:
`airflow-worker` carries the scheduler job id, which is how
`try_adopt_task_instances` and `_adopt_completed_pods`
(`executors/kubernetes_executor.py`) decide which pods a restarted or surviving
scheduler is entitled to take over, and how `build_selector_for_k8s_executor_pod`
excludes KubernetesPodOperator pods from executor sweeps.

The stakes are asymmetric. If lookup returns *nothing* when a pod exists, Airflow
launches a second pod for the same try — the task runs twice, against the same
external systems, with no error anywhere. If lookup returns *the wrong* pod,
Airflow attributes another task's state and logs to this one. Both failures are
silent, and both are produced by changes that look local: an unsafe label value,
a renamed key, a widened selector.

Reattachment on the operator side is built on the same metadata.
`reattach_on_restart` makes `find_pod` search by
`_build_find_pod_label_selector` rather than launch, and the `POD_CHECKED_KEY`
(`already_checked`) label — added by `patch_already_checked` — is what stops the
*next* try from reattaching to the *previous* try's pod. That marker is a
lifecycle boundary encoded in a label, and removing or mistiming it converts a
retry into a reattachment to a dead pod.

None of this metadata is versioned with the provider release. During a rolling
upgrade there are pods in the cluster created by the *previous* provider version,
and the new code must still recognise them.

## Decision

Every pod this provider creates carries enough metadata to be found again,
attributed to exactly one task instance try, and claimed by exactly one owner.
Concretely:

- **Identity is labels and annotations, never the pod name.** No code path may
  parse, match on, or reconstruct a task identity from `metadata.name`.
- **Every label value goes through `make_safe_label_value`** and tolerates a
  missing value. A value that Kubernetes rejects, or a `None` that renders as an
  empty selector term, produces a query that matches nothing — which is
  indistinguishable from "no pod exists" and therefore launches a duplicate.
- **Lookup keys are treated as a cross-version contract.** Adding a new key to a
  selector, renaming one, or changing how a value is derived must keep pods
  created by the previously released provider version matchable, because they are
  still running during the upgrade.
- **Ownership is explicit and exclusive.** Adoption is scoped to pods whose
  owning scheduler is no longer alive; a live scheduler's pods are never taken.
  Executor selectors exclude KubernetesPodOperator pods and vice versa.
- **The reattachment boundary is respected.** The `already_checked` marker
  separates tries; a change to when a pod is patched, retained or excluded must
  state which pods become reattachable as a result.

## Consequences

- A scheduler restart, a scheduler failover, or a worker crash resumes monitoring
  the existing pod instead of orphaning it or launching a second one — the
  property that keeps a task from executing twice.
- Metadata keys accumulate and are expensive to remove. Cleaning up a label is a
  multi-release operation gated on no in-flight pod carrying the old key, not a
  rename.
- Label-grammar handling looks like defensive noise in the diff and cannot be
  dropped: it is the difference between "found the pod" and "silently duplicated
  the task".
- Multi-scheduler deployments require adoption logic to reason about *other*
  schedulers' liveness, which is more code than a single-scheduler design would
  need.

A change **violates** this decision when it:

- derives which task a pod belongs to from `metadata.name`, or otherwise treats
  the generated name as a key;
- writes a label value without passing it through `make_safe_label_value`, or
  builds a selector term from a value that may be `None`;
- renames, removes, or re-derives a label or annotation used for lookup or
  adoption without keeping pods from the previously released provider version
  matchable;
- widens adoption to pods owned by a scheduler that is still alive, or drops the
  `airflow-worker` scoping that separates executor pods from
  KubernetesPodOperator pods;
- removes, skips, or reorders the `already_checked` patch such that a retry can
  reattach to a prior try's pod;
- adds a launch path that creates a pod without the full identity metadata, on
  the grounds that "this path never needs to look it up again".

A reviewer should reject any change to pod metadata or pod lookup whose behaviour
during a rolling provider upgrade, and under a scheduler restart, is unstated.

## Evidence

- #53477 — "Fix `KubernetesPodOperator` fails to delete pods with None value
  labels": a `None` label value breaking the selector, exactly the silent-lookup
  failure this decision guards.
- #58391 — "fix(kubernetes): Account for job- prefix when truncating job names":
  concrete demonstration that generated names are truncated and cannot carry
  identity.
- #66400 — "KubernetesExecutor: scope periodic completed-pod adoption to dead
  schedulers": adoption narrowed to pods whose owning scheduler is gone, so
  multiple schedulers stop contending for the same pods.
- #67850 — "Fix scheduler crashloop from KubernetesExecutor completed-pod
  adoption": the cost of getting adoption wrong is paid by the whole scheduler.
- #68674 — "KubernetesExecutor: self.completed adoption set is never drained":
  adoption bookkeeping that must actually converge.
- #68507 — "Make pod patching logic explicitly reflect when a pod is retained":
  the `already_checked` / retention boundary made explicit rather than implied.
- #50803 — "Add task context labels to driver and executor pods for
  SparkKubernetesOperator reattach_on_restart functionality" and #60717 — "Ensure
  deterministic Spark driver pod selection during reattach": the same identity
  requirement re-derived for the Spark operator once reattachment was needed
  there.
