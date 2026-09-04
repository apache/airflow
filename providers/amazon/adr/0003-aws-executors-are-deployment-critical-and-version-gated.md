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

# 3. The AWS executors are deployment-critical and every core API they touch is version-gated

Date: 2026-07-20

## Status

Accepted

## Context

`AwsEcsExecutor`, `AwsBatchExecutor` and `AwsLambdaExecutor` — registered in
`provider.yaml` under `executors:` and living in
`aws/executors/{ecs,batch,aws_lambda}/` — are unlike everything else in this
provider. The rest is worker-side code whose blast radius is the tasks that use
it; an executor is loaded by the scheduler, subclasses core's `BaseExecutor`, and
runs inside airflow-core. A defect here does not break an S3 operator; it stops
task execution for the entire deployment.

That places the executors on the wrong side of the usual provider assumption.
They talk to core's most actively evolving internals — workload types,
`ExecuteCallback`, `try_adopt_task_instances`, task-log plumbing, the queueing
surface — which changed materially across Airflow 3.0, 3.1 and 3.3, while this
provider still declares `apache-airflow>=2.11.0` and ships on its own cadence.
They therefore branch explicitly on `AIRFLOW_V_3_0_PLUS` / `AIRFLOW_V_3_3_PLUS`
from the provider's own `version_compat.py`, and that gating is load-bearing:
wiring it wrong has broken an executor at import time, and
`try_adopt_task_instances` compatibility was needed separately for ECS and Batch —
the same gap found twice, because the executors were not kept in step. Two further
consequences of running inside core: the prek hook forbidding `conf` imports from
`airflow.configuration` deliberately excludes executor modules (a narrow
carve-out, not a licence to reach into core internals); and an executor's config
*is* production behaviour — retry budgets, adoption semantics and defaults are what
a cluster operator tunes, so a changed default lands on every user at their next
upgrade with no Dag change to signal it.

## Decision

The AWS executors are treated as deployment-critical code with an explicit
cross-version contract.

- **Every core API an executor touches is version-gated** through
  `airflow.providers.amazon.version_compat` (`AIRFLOW_V_3_0_PLUS`,
  `AIRFLOW_V_3_3_PLUS`, …). Correctness is judged on the *declared floor*, not
  on a main checkout.
- **The gate is verified in both directions.** A change must be shown to take a
  working path on the oldest supported core as well as the newest — including
  at import time, since a mis-scoped compatibility import fails the executor
  before it runs anything.
- **The three executors stay in step.** A capability added to one of ECS, Batch
  or Lambda is either added to the others in the same change or explicitly
  scoped, with the reason stated. Silent divergence between them produces the
  same fix twice.
- **`BaseExecutor` is treated as the stable interface it is** — override its
  hooks, do not depend on core internals beyond them, and prefer inheriting
  generic behaviour over redeclaring it.
- **Executor registration lives in `provider.yaml`.** A new executor or a moved
  class path is a metadata change as much as a code change.
- **Configuration is contract.** Adding, renaming, or re-defaulting an executor
  config key, a retry budget, or an adoption behaviour is called out in the PR
  and, when users need to act, in `providers/amazon/docs/changelog.rst`.
- **Compatibility shims are removed deliberately**, when the declared floor no
  longer needs them — not opportunistically in a change that has another
  purpose.

## Consequences

- A cluster can upgrade the amazon provider independently of core without its
  executor failing to load or losing task adoption.
- Executor code carries visible version branches — the mechanism by which one
  distribution serves several core releases.
- Executor changes cost more review than an equivalent operator change and are a
  poor fit for a contributor who cannot exercise the executor in a real cluster.
- Some core capabilities arrive in the executors later, because they must be added
  behind a gate rather than adopted directly.

A change **violates** this decision when it:

- calls a core API that does not exist on the provider's declared floor without
  a `version_compat` gate, or places the gate so that the module fails to import
  on a supported core;
- imports the version-compat flags from anywhere other than the provider's own
  `version_compat.py`;
- adds a capability to one AWS executor and leaves the other two divergent
  without saying why;
- changes an executor config key, default, retry budget, or task-adoption
  behaviour without flagging it as an operational change for existing
  deployments;
- adds or moves an executor class without updating the `executors:` list in
  `provider.yaml`;
- removes a compatibility shim while the declared core floor still requires it,
  or treats the executors' `airflow.configuration` carve-out as permission to
  reach into other core internals.

## Evidence

- #56280 — the gating mechanism itself mis-wired, in the component that must load
  for tasks to run.
- #62192, #68027 — the same `try_adopt_task_instances` core-API gap fixed twice,
  months apart, because the executors were not kept in step.
- #63657, #63035, #62984 — `ExecuteCallback` rolled out across all three executors.
- #52121, #51009 — adoption behind a gate, and removal of the old path only once
  the floor allowed it.
- #60301 — a deliberate shim retirement.
- #60666 — executor configuration behaving as operational contract.
- #61321, #60920 — a core-side multi-team change threaded through the executors
  deliberately.
