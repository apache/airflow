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

# 4. Cluster authentication stays generic; vendor specifics live in the cloud provider

Date: 2026-07-20

## Status

Accepted

## Context

`KubernetesHook` and `AsyncKubernetesHook` authenticate the way the Kubernetes
client authenticates: from an in-cluster service account, from a kubeconfig
file, from a kubeconfig blob in the connection extra, or from a context inside
one of those. Some of those kubeconfigs use *exec-based* auth — the client
shells out to a credential plugin (`aws eks get-token`, `gke-gcloud-auth-plugin`,
an OIDC helper) that mints a short-lived token.

Exec auth is where the vendors leak in, and it is the single most common place
where a change tries to teach this provider about a specific cloud. The
recurring shapes are: adding `botocore` version guardrails to the EKS token
flow; isolating the AWS CLI's on-disk credential cache so parallel
`KubernetesPodOperator` tasks stop corrupting each other's tokens; capping the
`kubernetes` client because a particular managed-control-plane started returning
401 on a newer client.

Each of those describes a real user-visible failure. None of them belongs here.
This provider must run against vanilla clusters, kind, OpenShift, EKS, GKE, AKS
and self-hosted control planes with one code path; a vendor branch inside the
hook is a branch nobody outside that vendor can review, test in CI, or keep
working. Worse, a vendor-motivated *dependency* change — a cap on the shared
`kubernetes` client — is paid for by every user of the provider, including the
ones the cap was never about, and it routinely re-breaks whatever the previous
bump fixed.

The generic form of the same fix is almost always available and is what actually
merged. "Do not cache the client when the active context uses exec auth"
(`_uses_exec_auth` / `_load_config`) fixes the stale-token problem for every
credential plugin at once, without naming a vendor. That is the shape to look
for.

## Decision

**Authentication behaviour in this provider is expressed in terms of the
Kubernetes client's own mechanisms, never in terms of a specific cloud
provider.**

- **No vendor names, vendor CLIs, or vendor SDKs in this provider's auth path.**
  No `botocore` import, no AWS CLI cache path, no `gcloud` invariant, no
  provider-specific environment variable in `hooks/kubernetes.py` or the async
  hook.
- **Generalise the fix to the mechanism.** If a problem is observed on EKS but
  the cause is "exec-plugin credentials expire and we cached the client", fix it
  for exec auth generally. State in the PR which mechanism the fix is keyed on.
- **A vendor-specific workaround goes in that vendor's provider** — the Amazon
  or Google provider owns its EKS/GKE hooks and can carry the knowledge that
  belongs to it, with owners who can test it.
- **Do not move the `kubernetes` / `kubernetes_asyncio` pins to fix one managed
  control plane.** The pins are a whole-ecosystem contract (ADR 3's sibling
  concern, and the client-compatibility criteria in `AGENTS.md`). A cap needs the
  upstream issue URL at the pin site and evidence that the problem is in the
  client, not in one provider's use of it.
- **Connection extras that configure auth are read by name**, and their meaning
  is client-level (`kube_config`, `kube_config_path`, `cluster_context`,
  `in_cluster`, `disable_verify_ssl`, …) — not vendor-level.

## Consequences

- One auth path, reviewable by anyone who knows the Kubernetes client, and
  exercised identically on every cluster flavour.
- Fixes reach every credential plugin at once instead of the one the reporter
  happened to use.
- The cost is real and falls on the reporter: an EKS-only or GKE-only problem
  takes longer to fix, because it either has to be restated in generic terms or
  routed to a provider whose maintainers the reporter has not met. Some genuine
  vendor bugs stay open longer as a result. That is accepted — the alternative
  is a hook that accumulates a branch per cloud and is correct on none of them.
- Users on managed clusters occasionally have to pin the client themselves while
  an upstream fix lands, instead of getting a cap shipped for everyone.

A change **violates** this decision when it:

- imports or version-checks a cloud SDK / CLI (`botocore`, `boto3`, `google.auth`,
  `azure.identity`, …) from this provider's hooks, operators, or triggers;
- special-cases a vendor's credential cache, token file, or plugin binary path;
- adds a parameter, connection extra, or config option whose name or semantics
  only make sense on one managed Kubernetes offering;
- changes the `kubernetes` / `kubernetes_asyncio` / `urllib3` pins to work
  around a single control plane's behaviour, rather than a defect in the client
  itself;
- fixes an exec-auth problem by keying on the plugin's command line instead of
  on the presence of exec auth.

## Evidence

- #61936 — "`KubernetesHook`: add AWS exec-auth botocore guardrails for EKS
  token flow": closed; AWS SDK version knowledge in this provider's hook.
- #61935 and #61025 — two attempts to isolate the AWS CLI credential cache
  during parallel `KubernetesPodOperator` authentication: both closed. The
  problem was real; the merged fix (#63610, and #65212 for the async hook)
  is the vendor-neutral one — detect exec auth via `_uses_exec_auth` and stop
  caching the loaded config.
- #61738 — "Avoid caching kubeconfig for exec-based auth in
  `AsyncKubernetesHook`": the generic framing, closed only for inactivity and
  then completed as #65212. The contrast with #61935/#61025 is the point.
- #69025 — "Cap kubernetes client to <36 to fix cluster auth 401 regression":
  closed. The review response was explicit — capping re-introduces the proxy
  defect the bump to 36 fixed, and the managed-cluster behaviour should be
  handled in the cloud provider instead.
