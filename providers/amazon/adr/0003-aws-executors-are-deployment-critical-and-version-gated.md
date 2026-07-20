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
provider. The rest of the provider is worker-side code whose blast radius is the
tasks that use it. An executor is the component that *runs* tasks. It is loaded
by the scheduler, it subclasses core's `BaseExecutor`, and it is the one part of
this provider that runs inside airflow-core rather than on a worker. A defect
here does not break an S3 operator; it stops task execution for the entire
deployment.

That placement puts the executors on the wrong side of the usual provider
assumption. A worker-side hook mostly talks to AWS, and its exposure to core is
a handful of stable SDK entry points. An executor talks to core's most actively
evolving internals: workload types, `ExecuteCallback`, `try_adopt_task_instances`,
task-log plumbing, and the queueing surface. Those changed materially across
Airflow 3.0, 3.1 and 3.3 — while this provider still declares a floor of
`apache-airflow>=2.11.0` and ships on its own cadence. A deployment routinely
runs the newest amazon provider on an older core.

The executors therefore branch explicitly on `AIRFLOW_V_3_0_PLUS` and
`AIRFLOW_V_3_3_PLUS` imported from the provider's own `version_compat.py`, and
that gating is load-bearing, not defensive decoration. Wiring it wrong has
already broken an executor at import time on a supported core, and executor
compatibility fixes for `try_adopt_task_instances` have been needed separately
for ECS and for Batch — the same core API, the same gap, found twice, because
the three executors were not kept in step.

A second consequence of running inside core is that the SDK-only boundary the
parent `providers/adr/` sets for worker code does not apply the same way here:
the prek hook that forbids importing `conf` from `airflow.configuration` in
providers deliberately excludes executor modules. That carve-out is narrow and
exists for exactly this reason — it is not a general licence for executor code
to reach into core internals.

Finally, an executor's configuration *is* production behaviour. Its retry
budgets, adoption semantics, capacity defaults and config keys are what an
operator of a cluster tunes. Changing a default is not a code cleanup; it is a
change that lands on every existing user on their next provider upgrade, with no
Dag change to signal it.

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
  executor failing to load or silently losing task adoption.
- Executor code carries visible version branches. That verbosity is accepted:
  it is the mechanism by which one distribution serves several core releases.
- Executor changes cost more review than an equivalent operator change, and are
  a poor fit for a contributor who cannot exercise the executor in a real
  cluster.
- Some core capabilities arrive in the executors later than in core, because
  they must be added behind a gate rather than adopted directly.

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

- #56280 — "Fix wrong import of `AIRFLOW_V_3_0_PLUS` in `AwsLambdaExecutor`":
  the gating mechanism itself mis-wired, in the component that must load for
  tasks to run at all.
- #62192 — "Fix ECS Executor compatibility with Airflow 3.x in
  `try_adopt_task_instances`" and #68027 — "Adds Airflow 3 compatibility in
  `try_adopt_task_instances` for BatchExec": the same core-API gap fixed twice,
  months apart, because the executors were not kept in step.
- #63657 — "Add `ExecuteCallback` support to AWS ECS Executor", #63035 — "Add
  `ExecuteCallback` support to `AwsLambdaExecutor`", #62984 — "feat: add
  callback support to aws batch executor": a core capability rolled out across
  all three executors rather than one.
- #52121 — "Add Airflow 3.0+ Task SDK support to AWS Batch Executor" and
  #51009 — "Remove Airflow 2 code path in executors": adoption behind a gate,
  and removal of the old path only once the floor allowed it.
- #60301 — "Remove the compatibility shim for `log_task_event`
  `AwsEcsExecutor` and `AwsBatchExecutor`": a deliberate shim retirement.
- #60666 — "Fix lambda executor max attempts config": executor configuration
  behaving as operational contract.
- #61321 — "`AwsLambdaExecutor`: Support multi-team configuration" and
  #60920 — "AIP-67 - Multi-team: `AwsBatchExecutor` per team executor config":
  a core-side architectural change that had to be threaded through the
  executors deliberately.
