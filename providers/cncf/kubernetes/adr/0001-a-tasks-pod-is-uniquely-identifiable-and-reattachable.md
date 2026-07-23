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

A Kubernetes pod outlives the Airflow process that created it. A scheduler running
`KubernetesExecutor` can restart, reschedule, or scale to several replicas; a
worker running `KubernetesPodOperator` can be killed mid-task. The pod keeps
running, and Airflow has to find it again from nothing but the Kubernetes API —
and there is no stable name to find it by. `create_unique_id` / `add_unique_suffix`
slugify and truncate `dag_id`/`task_id` to `POD_NAME_MAX_LENGTH` and append a
random suffix, so two tasks can collapse onto one prefix and the same task gets a
different name every launch. The name is a display handle, not a key.

Identity lives in metadata. `build_labels_for_k8s_executor_pod`
(`pod_generator.py`) and `KubernetesPodOperator._get_ti_pod_labels`
(`operators/pod.py`) stamp `dag_id`, `task_id`, `run_id`, `map_index` and
`try_number` as labels, each through `make_safe_label_value`; the executor also
writes them as annotations, which `annotations_to_key` reverses into a
`TaskInstanceKey`. Ownership is a label too: `airflow-worker` carries the scheduler
job id, which is how `try_adopt_task_instances` / `_adopt_completed_pods` decide
which pods a restarted scheduler may take, and how
`build_selector_for_k8s_executor_pod` separates executor pods from
KubernetesPodOperator pods. The stakes are asymmetric and silent: lookup returning
*nothing* launches a second pod for the same try (the task runs twice); returning
*the wrong* pod attributes another task's state and logs. Reattachment uses the
same metadata — `reattach_on_restart` makes `find_pod` search by
`_build_find_pod_label_selector`, and the `already_checked` label
(`patch_already_checked`) stops the next try reattaching to the previous try's pod.
None of this metadata is versioned with the release, so during a rolling upgrade
the new code must still recognise pods created by the previous version.

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

- A scheduler restart, failover, or worker crash resumes monitoring the existing
  pod instead of orphaning it or launching a second one.
- Metadata keys accumulate and are expensive to remove. Cleaning up a label is a
  multi-release operation gated on no in-flight pod carrying the old key.
- Label-grammar handling looks like defensive noise but cannot be dropped: it is
  the difference between "found the pod" and "silently duplicated the task".
- Multi-scheduler deployments require adoption logic to reason about *other*
  schedulers' liveness — more code than a single-scheduler design would need.

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

- #53477 — a `None` label value breaking the selector: the silent-lookup failure
  this decision guards.
- #58391 — generated names are truncated and cannot carry identity.
- #66400 — adoption narrowed to pods whose owning scheduler is gone.
- #67850 — the cost of getting adoption wrong is paid by the whole scheduler.
- #68674 — adoption bookkeeping that must actually converge.
- #68507 — the `already_checked` / retention boundary made explicit.
- #50803, #60717 — the same identity requirement re-derived for the Spark operator's
  reattachment.
