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
client does: from an in-cluster service account, a kubeconfig file, a kubeconfig
blob in the connection extra, or a context inside one of those. Some kubeconfigs
use *exec-based* auth — the client shells out to a credential plugin
(`aws eks get-token`, `gke-gcloud-auth-plugin`, an OIDC helper) that mints a
short-lived token.

Exec auth is where the vendors leak in, and the single most common place a change
tries to teach this provider about a specific cloud: adding `botocore` guardrails
to the EKS token flow; isolating the AWS CLI's credential cache so parallel
`KubernetesPodOperator` tasks stop corrupting each other's tokens; capping the
`kubernetes` client because a managed control plane started returning 401. Each
describes a real failure; none belongs here. This provider must run against
vanilla clusters, kind, OpenShift, EKS, GKE, AKS and self-hosted control planes on
one code path — a vendor branch inside the hook is one nobody outside that vendor
can review or test in CI, and a vendor-motivated *dependency* cap on the shared
client is paid for by every user and routinely re-breaks whatever the previous
bump fixed. The generic form of the fix is almost always available and is what
merged: "do not cache the client when the active context uses exec auth"
(`_uses_exec_auth` / `_load_config`) fixes the stale-token problem for every
credential plugin at once, without naming a vendor. That is the shape to look for.

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

- One auth path, reviewable by anyone who knows the Kubernetes client and
  exercised identically on every cluster flavour.
- Fixes reach every credential plugin at once instead of the one the reporter used.
- The cost falls on the reporter: an EKS-only or GKE-only problem takes longer,
  because it is restated generically or routed to another provider. Some genuine
  vendor bugs stay open longer — accepted; the alternative is a hook with a branch
  per cloud, correct on none.
- Users on managed clusters occasionally pin the client themselves while an
  upstream fix lands, instead of getting a cap shipped for everyone.

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

- #61936 — AWS SDK version knowledge in this provider's hook; closed.
- #61935, #61025 — two attempts to isolate the AWS CLI credential cache; both
  closed. The merged fix (#63610, and #65212 for the async hook) is the
  vendor-neutral one — detect exec auth via `_uses_exec_auth` and stop caching.
- #61738 — the generic framing, closed for inactivity then completed as #65212;
  the contrast with #61935/#61025 is the point.
- #69025 — a `kubernetes<36` cap closed: capping re-introduces the proxy defect the
  bump to 36 fixed, and the managed-cluster behaviour belongs in the cloud provider.
