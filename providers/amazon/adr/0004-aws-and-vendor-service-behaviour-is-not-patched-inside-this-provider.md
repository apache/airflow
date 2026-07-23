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

This provider sits between Airflow and three layers it does not own: the AWS APIs,
the `boto3` / `botocore` / `aiobotocore` client stack, and the managed Airflow
offerings that repackage Airflow on AWS. Bugs surface *here*, because this is
where users see them, but frequently do not belong here. A fix written at the
point of observation rather than the point of cause becomes a permanent
workaround: it survives the upstream fix, it is invisible to the next reader, and
removing it later requires reconstructing a condition nobody remembers.

Three variants recur. A **botocore bug already fixed upstream**: a guard is added
in Airflow while the vendor ships a fix the provider's floor already requires —
two successive PRs to pre-create a botocore credential-cache directory were closed
once it was established the pinned floor already contained the fix. A
**managed-offering behaviour**: a restriction that exists only inside a vendor's
hosted product is not fixed by adding a parameter every self-hosted user also
carries; the remedy is a report to that offering's support, with room left for the
vendor's own maintainers to argue otherwise. And **AWS-side semantics inferred
from a single log line**: "a paused cluster cannot be deleted" read from one
failure turned out to describe a mid-transition window handled by retrying — a
smaller, more honest fix than the state machine first proposed. The common cost is
that workarounds accumulate faster than they are retired, and the provider slowly
acquires behaviour AWS does not have.

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
- Some user-visible pain persists longer, because the fix has to travel through a
  vendor release before the floor can move — accepted as the cost of not carrying
  permanent guards.
- Contributors must do upstream research before proposing a fix here, which is
  slower than writing the guard.
- The workarounds that exist are individually justified and removable, because
  each names the condition that would retire it.

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
  directory, closed once the fix was found already released and covered by the
  floor: "I don't understand what we are fixing here."
- #63400 — a Lambda `deserialize_payload` option closed as won't-fix: specific to
  one vendor's serverless offering, author directed to its support channel.
- #68922 — resuming a paused Redshift cluster before deletion, withdrawn once the
  claimed AWS rule turned out to be a mid-transition window a retry already handles.
- #69838 — a Neptune deferrable fix withdrawn after the real traceback showed an
  async-to-sync botocore context failure the change did not touch.
- #61664 — a `sagemaker-studio` floor bump closed on an explicit vendor request.
- #59793 — a fallback instance-type change asked to state its reason in the changelog.
