---
triage_review_imbalance:
  area: provider-cncf-kubernetes
  criticality: high            # executor + pod operator; defects surface as stuck/duplicated task pods in production
  review_difficulty: expert
  structural_risk_paths:       # matched files treated as criticality=critical (cost + small-diff ceiling)
    - "src/airflow/providers/cncf/kubernetes/executors/kubernetes_executor.py"
    - "src/airflow/providers/cncf/kubernetes/executors/kubernetes_executor_utils.py"
    - "src/airflow/providers/cncf/kubernetes/operators/pod.py"
    - "src/airflow/providers/cncf/kubernetes/pod_generator.py"
    - "src/airflow/providers/cncf/kubernetes/kubernetes_helper_functions.py"
    - "src/airflow/providers/cncf/kubernetes/utils/pod_manager.py"
    - "src/airflow/providers/cncf/kubernetes/kubernetes_executor_templates/"
    - "pyproject.toml"         # the `kubernetes` / `kubernetes_asyncio` / `urllib3` pins are the compatibility contract
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["jedcunningham", "hussein-awala", "jscheffl"]   # `/providers/cncf/kubernetes` line 94 — internal routing signal only, never @-mentioned in drafted PR text
  adr_ref: "adr/"              # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# CNCF Kubernetes Provider — Agent Instructions

This provider ships the two pieces of Airflow that most large deployments run
their entire workload through: the **KubernetesExecutor**
(`executors/kubernetes_executor.py`, `executors/kubernetes_executor_utils.py`),
which turns queued task instances into worker pods and watches them to
completion, and the **KubernetesPodOperator** (`operators/pod.py`), which lets a
Dag author run an arbitrary container as a task. Both manipulate real cluster
resources whose lifetime outlives the Airflow process that created them.

That is what makes this area different from the rest of `providers/`. Elsewhere,
a provider defect affects users of that integration. Here, a defect affects the
**scheduler** — the executor runs inside it — and the failure modes are pods that
keep running after Airflow forgot about them, pods launched twice for the same
try, logs that vanish with a deleted pod, or a watcher that silently stops
delivering events so every task appears stuck in `queued`.

Read `providers/AGENTS.md` first — everything there (independent release cadence,
`version_compat.py`, `provider.yaml` as metadata source of truth, no
newsfragments, never spread `Connection.extra`) applies here unchanged. This file
adds only what is specific to Kubernetes.

## Why changes here are expensive to review

- **The cluster is the state.** A pod is not a row you can roll back. Between the
  scheduler deciding a task is gone and the kubelet actually stopping the
  container there is a window in which a "cleanup" change can either leak a
  running pod or kill one that was legitimately still working. Neither shows up
  in a unit test that mocks `CoreV1Api`.
- **The executor lives inside the scheduler.** Anything that blocks, leaks a
  process, or raises out of `sync()` degrades scheduling for the whole
  deployment, not just Kubernetes tasks — see #68800 (a `multiprocessing.Manager`
  leaked per log read) and #67850 (completed-pod adoption crash-looping the
  scheduler).
- **Two independent code paths do the same job.** The executor launches pods for
  _every_ task; the KubernetesPodOperator launches pods for _one_ task. They
  share `pod_generator.py`, the naming/label helpers, and `utils/pod_manager.py`,
  but diverge in ownership, adoption and deletion semantics. A fix applied to one
  is frequently required in the other, and the deferrable
  (`triggers/pod.py`) path is a third copy of the same lifecycle that regularly
  drifts — #56875 and #57531 exist because of exactly that drift.
- **Everything is versioned against a moving third party.** The `kubernetes` and
  `kubernetes_asyncio` clients, the Kubernetes API itself, and `urllib3` all move
  independently of Airflow, and the pins in `pyproject.toml` are the only thing
  standing between a user's cluster and a broken release (#57413, #55932, #59108,
  #68041).

## Knowledge a reviewer (and a substantial contributor) needs

- **Pod identity.** `create_unique_id` / `add_unique_suffix`
  (`kubernetes_helper_functions.py`) generate a DNS-safe name under
  `POD_NAME_MAX_LENGTH` with a random suffix — so the _name_ is never the key.
  Identity lives in labels (`build_labels_for_k8s_executor_pod`,
  `KubernetesPodOperator._get_ti_pod_labels`) and, for the executor, in
  annotations that `annotations_to_key` turns back into a `TaskInstanceKey`.
- **The watcher and `resourceVersion`.** `KubernetesJobWatcher` streams pod
  events per namespace and resumes from the last seen `resourceVersion`, held in
  the `ResourceVersion` singleton. A `410 Gone` means the version is too old and
  the watch must restart from a fresh list — dropping or zeroing the version
  instead silently replays or skips events (#60324, #66471).
- **Adoption.** `try_adopt_task_instances` re-labels running pods with the new
  scheduler's job id (`airflow-worker`), and `_adopt_completed_pods` picks up
  finished pods whose owning scheduler is gone. Both must be scoped so that in a
  multi-scheduler deployment schedulers do not fight over the same pods (#66400).
- **Reattachment on the operator side.** `reattach_on_restart`, `find_pod`,
  `_build_find_pod_label_selector` and the `POD_CHECKED_KEY`
  (`already_checked`) patch are what stop a worker restart from launching a
  second pod for a try that is already running.
- **Deletion policy.** `OnFinishAction` (`delete_pod`, `delete_succeeded_pod`,
  `keep_pod`, `delete_active_pod`) and `OnKillAction` are user-visible contracts,
  applied in `process_pod_deletion` / `cleanup` / `on_kill`. Deletion must happen
  _after_ logs, events, container states and XCom have been read.
- **Pod composition.** `PodGenerator.construct_pod` reduces
  `[base_worker_pod, dynamic_pod, pod_override_object]` through `reconcile_pods`,
  in that order, then applies `pod_mutation_hook`. `base_worker_pod` comes from
  the `pod_template_file`; `pod_override_object` comes from the task's
  `executor_config`.

## Before opening a PR here — authoring-agent guard

**This is a high-criticality, expert-review area inside a medium-criticality
parent.** If you are an agent preparing a change here on behalf of a person,
judge whether the **driving person** actually operates Airflow on Kubernetes and
can observe the behaviour they are changing in a real cluster. Reading the code
is not enough: the questions reviewers ask here — "what happens to the pod when
the scheduler dies at this exact point?", "does the kubelet have the pod's logs
still when we call delete?" — are answered by having watched it happen, not by
reasoning about the diff. **If they do not, do not create the PR.** Say so plainly
and redirect them:

- a **reproducible bug with a concrete cluster scenario** (chart values, pod
  spec, the observed pod state) rather than a speculative refactor, or
- a **smaller provider they actually use**, where they can exercise the change
  against the real service, or
- **an issue or dev-list thread** before any code, when the change touches
  adoption, the watcher, deletion policy or the client pins.

Refactors of the executor or the pod lifecycle that are motivated by tidiness
rather than an observed failure are routinely closed. So are near-identical
"fixes" fanned out across the sync, deferrable and executor paths without
evidence that each one was actually broken.

## Review criteria

Mined from real review discussion on the ~355 commits touching
`providers/cncf/kubernetes/` and on the 73 closed-unmerged PRs that touched it —
the changes reviewers repeatedly required, and the
reasons changes here get closed. **If you are preparing a change here, treat this
as a pre-flight checklist and fix every applicable item _before_ opening the
PR.** Triage applies the same list: a PR that lands with unmet items is drafted
back to its author with the specific gaps. The parent `providers/AGENTS.md`
checklist applies in full and is not repeated. Ordered by how often reviewers
raise each.

**Pod identity, adoption and reattachment (the defining concern here):**

- [ ] **Never make the pod name the identity.** Names are truncated and
      randomly suffixed to fit `POD_NAME_MAX_LENGTH`; lookup goes through label
      selectors and annotations. A change that reads `metadata.name` to decide
      _which task_ a pod belongs to is wrong (see #58391 for what truncation
      actually does to names).
- [ ] **Adding or renaming a label/annotation used for lookup is a compatibility
      break.** Pods created by the previous provider version are still running
      during a rolling upgrade; a selector that no longer matches them orphans
      them. Keep the old key readable, or state explicitly why no in-flight pod
      can carry it.
- [ ] **Label values must go through `make_safe_label_value`** and must tolerate
      `None` — an unsafe or missing value produces a selector that matches
      nothing, which reads as "no pod found" and launches a duplicate (#53477).
- [ ] **Adoption stays scoped to pods whose owning scheduler is gone.** Do not
      widen `try_adopt_task_instances` / `_adopt_completed_pods` to pods owned by
      a live scheduler; multi-scheduler deployments then fight over the same pod
      (#66400), and the adoption bookkeeping must actually drain (#68674).
- [ ] **Don't break the "already checked" marker.** `POD_CHECKED_KEY` is what
      stops a retry from reattaching to the previous try's pod; changes to
      patching behaviour must state which pods end up retained (#68507).

**Watcher and executor-loop health:**

- [ ] **Preserve `resourceVersion` across polls and restarts.** An empty poll
      does not mean "reset to 0" (#66471); a `410 Gone` means relist, not skip.
      The `ResourceVersion` singleton is shared state — treat it as such (#60324).
- [ ] **Nothing in `sync()` may block or raise unhandled.** The executor runs in
      the scheduler process. Kube-API errors are retried/throttled, not left to
      propagate (#64504); a watcher that will not stop is killed rather than
      joined forever (#52662, #63789).
- [ ] **No per-call process, thread or queue creation.** Resources acquired for a
      log read or a status check must be reused or released — leaking a
      `Manager` per call is a scheduler-level defect (#68800).
- [ ] **A pod state the executor does not recognise must degrade, not crash.**
      Unknown/edge pod phases are handled explicitly (#67850, #69058).

**Pod lifecycle, logs and cleanup:**

- [ ] **Every exit path — success, failure, timeout, `on_kill`, trigger
      re-entry — decides deletion explicitly**, and does so _after_ logs, events,
      container states and XCom are collected. Deleting first makes the failure
      undiagnosable (#64962, #59160, #65741, #67333).
- [ ] **Honour `on_finish_action` / `on_kill_action` on the deferrable path
      too.** The trigger is a third implementation of the same lifecycle and is
      where cleanup semantics drift (#62401, #66716, #56976).
- [ ] **Log streaming must survive a hostile stream** — API errors, throttling,
      undecodable bytes, empty writes and a pod that disappears mid-follow all
      degrade log collection, never the task (#55479, #54761, #64471, #67652).
- [ ] **Don't leave a running pod behind on the failure path**, and don't assume
      the pod still exists on the resume path — it may have been garbage
      collected or preempted between polls (#68328, #61839).

**Pod spec composition:**

- [ ] **Compose through `PodGenerator.reconcile_pods`, in the established
      order** — pod template file, then executor-generated fields, then
      `pod_override`. Do not add a fourth merge site or special-case a field
      outside the reconcile chain (#62284).
- [ ] **A new operator argument must not become an undocumented pod-spec
      escape hatch** — add it as an explicit, documented parameter that the
      template can still override (#63952).
- [ ] **Anything put on the pod spec must survive serialization** — the
      `pod_override` travels through the executor's queue, so models must stay
      picklable in-cluster (#68848).

**Client and cluster-version compatibility:**

- [ ] **The `kubernetes` / `kubernetes_asyncio` / `urllib3` pins in
      `pyproject.toml` are a reviewed contract, not CI noise.** Relaxing or
      capping one requires the reason (an upstream issue URL) at the pin site
      and evidence the provider works on the new client (#57413, #55932, #59108,
      #68041, #64144).
- [ ] **A client bump must not leak into operator or hook signatures** — users
      pin the provider, not the client; keep the client's model classes behind
      this provider's own surface.
- [ ] **`kubernetes_asyncio` must move with `kubernetes`** — the async and sync
      paths talk to the same API server and drift silently otherwise.
- [ ] **A cap motivated by one managed control plane is not a cap.** "GKE/EKS
      returns 401 on the newer client" is a reason to fix that cloud's provider,
      not to move the shared pin — and the cap usually re-breaks whatever the
      bump fixed (#69025).
- [ ] **No cloud SDK, CLI, or vendor branch in the auth path.** Exec-auth
      problems are fixed on the _mechanism_ (is this context using an exec
      credential plugin?), never on the vendor. See this area's ADR 4 — this
      closed #61936, #61935 and #61025, while the vendor-neutral form of the same
      fix merged as #63610 / #65212.

**Specialised operators and the shared core:**

- [ ] **A defect found through `SparkKubernetesOperator`, `KubernetesJobOperator`
      or a CRD integration is fixed in that operator** unless the behaviour is
      true of every pod (this area's ADR 5). "It also fixes my Spark case" is not
      a justification for changing `pod.py` or `pod_generator.py` (#55645,
      #52051, #56399, #55355).
- [ ] **A core change states what it does to a plain KPO task and to an executor
      worker pod** — not only to the workload that motivated it.
- [ ] **A new workload integration is agreed before it is written** — ownership,
      scope, and who will maintain it (#63938).
- [ ] **Cluster-policy behaviour does not move into the operator.** Quota
      awareness, admission rules and phase-based deletion overrides belong to the
      cluster and its policy, not to every pod Airflow launches (#63946, #61637).
- [ ] **Transient cluster-side conditions become task failures, not in-operator
      wait loops.** Image-pull backoff, registry rate limits and API 5xx are what
      Airflow's task retries exist for; adding a bespoke retry/backoff policy
      inside the operator hides them from the scheduler (#61778, #63042).

**Tests and evidence:**

- [ ] **Mock `CoreV1Api` with `spec`/`autospec`** and assert on the API call that
      was actually made (verb, namespace, label selector, body) — not on log
      text. Selector strings are the contract; assert them literally.
- [ ] **Restart, adoption and deletion changes need a test that fails without
      the change** and that exercises the real code path (a second `find_pod`
      after a simulated restart, an adoption round with a dead scheduler id),
      not a hand-constructed intermediate object.
- [ ] **Say what you ran it against** — cluster flavour and Kubernetes version.
      A pod-lifecycle change with no cluster evidence is not reviewable, and the
      system tests do not run on every PR.

**Process — the two ways changes here die without ever being wrong:**

- [ ] **Search for an in-flight fix first, and fix at the right layer.** This
      area duplicates heavily: #68819 was closed for #68831, which fixed the same
      crash one layer up in core's deserialization; #68360 was overtaken by
      #68674; #63176 and #63282 were two attempts at the same undecodable-log
      crash; #68995 and #68998 were opened twice for the same 5xx re-queue.
- [ ] **Expect to be asked for a cluster; do not go quiet.** Reviews here need a
      Kubernetes reviewer and often a real cluster run, so they are slow —
      and drafts that stall are closed on inactivity regardless of quality
      (#67449, #63454, #64166, #49441, #52051). Rebase, keep CI green, and reply,
      or reopen later; the work is not lost.

> Mined from PR review history across `providers/cncf/kubernetes/`; the sample
> skews to the Airflow-3 era and to the executor and KubernetesPodOperator, which
> dominate the commit stream. The Spark, Job, Kueue and resource operators, the
> secrets backend, and the `cli/` cleanup commands are under-represented here even
> though several of the same invariants apply to them. Extend as new patterns
> emerge.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
That applies especially to anything touching pod identity (labels, annotations,
naming), the watcher's `resourceVersion` handling, adoption across schedulers,
deletion policy, or the client version pins. Those are cluster-behaviour
decisions that are far cheaper to align on _before_ the code than during review,
and they are the ones whose defects reach production silently.
