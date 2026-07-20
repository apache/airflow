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

# 4. AWS, botocore and managed-offering behaviour is not patched inside this provider

Date: 2026-07-20

## Status

Accepted

## Context

This provider sits between Airflow and three layers it does not own: the AWS
APIs themselves, the `boto3` / `botocore` / `aiobotocore` client stack, and the
managed Airflow offerings that repackage Airflow on AWS. Bugs surface *here*,
because this is where users see them, but they frequently do not belong here.
A fix written at the point of observation rather than the point of cause becomes
a permanent workaround: it survives the upstream fix, it is invisible to the
next person reading the code, and removing it later requires reconstructing a
condition nobody remembers.

Three variants recur.

The first is a **botocore bug already fixed upstream**. A client-stack defect
gets a guard in Airflow code while, in parallel, the vendor ships a release that
fixes it — and this provider's dependency floor already requires that release or
better. Two successive PRs proposed pre-creating a botocore credential cache
directory to dodge a race, first in the `cncf.kubernetes` provider and then here;
both were closed once it was established that the pinned botocore floor already
contained the fix. The workaround would have shipped guard code with no live
condition to guard.

The second is a **managed-offering behaviour**. A restriction that exists only
inside a vendor's hosted Airflow product — not in Airflow and not in AWS — is
not fixed by adding a parameter to an operator that every self-hosted user also
carries. The remedy is a report to the offering's own support channel. The
project explicitly leaves room for the vendor's own maintainers, who participate
here, to argue otherwise; absent that, the default is "won't fix".

The third is **AWS-side semantics inferred from a single log line**. AWS error
messages are terse and frequently describe a transient state rather than a
rule. Reading "a paused cluster cannot be deleted" out of one failure and
encoding it as an ordering requirement in an operator turned out to describe a
mid-transition window, correctly handled by retrying — a smaller and more honest
fix than the state machine that was first proposed. The same shape appeared in a
deferrable Neptune change written from a traceback whose real cause was an
async-to-sync botocore context problem the change did not touch.

The common cost is the same: workarounds accumulate faster than they are
retired, and the provider slowly acquires behaviour that AWS does not have.

## Decision

A defect is fixed at the layer that owns it. This provider absorbs AWS
behaviour; it does not re-implement or paper over it.

- **Check the client stack first.** Before guarding a `boto3` / `botocore` /
  `aiobotocore` bug, check whether the vendor has released a fix and whether
  this provider's declared floor already includes it. If it does, the change is
  a floor bump or nothing at all.
- **A dependency floor bump is the preferred fix** for an upstream defect —
  stated in the PR with the upstream issue or release it corresponds to.
- **Behaviour specific to a managed Airflow offering is out of scope.** Report
  it to that offering; it is not a reason to change an operator every user runs.
- **Do not infer an AWS rule from one error message.** A claimed AWS constraint
  is backed by AWS documentation or by a reproduction that distinguishes a
  permanent rule from a transient state — and a transient state is handled by
  the provider's retry and waiter machinery, not by new operator logic.
- **A workaround that must stay carries a tracking issue** and a comment at the
  workaround site with the full issue URL, stating the condition under which it
  can be removed.
- **Dependency pins that a vendor asked for are left alone** until that vendor
  signals otherwise; the reason is recorded in the PR that declines the change.

## Consequences

- The provider stays close to AWS semantics, so an operator's behaviour can be
  predicted from AWS documentation rather than from this package's history.
- Some user-visible pain persists longer, because the fix has to travel through
  a vendor release before the floor can move. This is accepted as the cost of
  not carrying permanent guards.
- Contributors must do upstream research — vendor changelogs, issue trackers —
  before proposing a fix here, which is slower than writing the guard.
- The workarounds that do exist are individually justified and removable,
  because each one names the condition that would retire it.

A change **violates** this decision when it:

- adds a guard, retry, or pre-creation step for a `botocore` / `boto3` /
  `aiobotocore` defect without checking whether the declared dependency floor
  already contains the vendor's fix;
- implements in Airflow a behaviour restriction that exists only in a vendor's
  managed Airflow offering;
- encodes an AWS API rule inferred from a single error message, with no AWS
  documentation reference and no reproduction distinguishing a rule from a
  transient state;
- adds new operator sequencing where the condition is transient and the existing
  waiter or retry budget covers it;
- lands a workaround with no tracking issue and no removal condition recorded at
  the workaround site;
- changes a dependency pin a vendor explicitly asked to keep, without a fresh
  signal from that vendor.

## Evidence

- #63610 and #62307 — two attempts to pre-create the botocore credential cache
  directory to avoid an exec-plugin race, first via the `cncf.kubernetes` hook
  and then here. Closed once review established the fix was already released in
  botocore and covered by this provider's declared floor: "I don't understand
  what we are fixing here."
- #63400 — a `deserialize_payload` option on the Lambda invoke operator, closed
  as a won't-fix: the problem is specific to one vendor's serverless Airflow
  offering, and the author was directed to that vendor's support channel, with
  the door left open for the vendor's own maintainers to disagree.
- #68922 — resuming a paused Redshift cluster before deleting it, withdrawn by
  the author once the claimed AWS rule turned out to describe a mid-transition
  window that a retry already handles.
- #69838 — a Neptune deferrable fix withdrawn after the real traceback showed an
  async-to-sync botocore context failure that the waiter rename and parameter
  forwarding did not touch.
- #61664 — a `sagemaker-studio` floor bump closed on an explicit vendor request
  to leave the dependency as it was, with the follow-up recorded.
- #59793 — a fallback instance-type change asked to state its reason in the
  provider changelog before being considered.
