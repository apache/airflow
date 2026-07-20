---
triage_review_imbalance:
  area: provider-amazon
  criticality: medium          # large surface, but a break lands on this provider's users, not the cluster
  review_difficulty: high
  structural_risk_paths:       # matched files treated as criticality=high (cost + small-diff ceiling)
    - "src/airflow/providers/amazon/aws/hooks/base_aws.py"   # every AWS hook inherits this
    - "src/airflow/providers/amazon/aws/executors/"          # deployment-critical; runs inside airflow-core
    - "src/airflow/providers/amazon/aws/triggers/base.py"    # every waiter-based deferrable path
    - "src/airflow/providers/amazon/aws/waiters/"            # declarative polling budgets for ~28 services
    - "src/airflow/providers/amazon/aws/utils/connection_wrapper.py"  # connection extras → session config
    - "src/airflow/providers/amazon/aws/auth_manager/"       # AWS auth manager (authorization decisions)
  codeowners_ref: ".github/CODEOWNERS"
  experts: ["o-nikolas"]       # internal signal only — never @-mentioned in drafted PR text
  adr_ref: "adr/"              # area Architecture Decision Records — checked for conformance (step §2c)
---

<!-- SPDX-License-Identifier: Apache-2.0
     https://www.apache.org/licenses/LICENSE-2.0 -->

# Amazon Provider — Agent Instructions

This is the largest provider in the repository by change volume — roughly 676
commits touch `providers/amazon/` — and the widest by surface: ~54 hooks, ~42
operator modules, ~31 trigger modules, custom waiter models for ~28 AWS
services, three executors, an auth manager, a Dag bundle, a secrets backend,
and a log handler. It ships on the provider release cadence against a declared
core floor of `apache-airflow>=2.11.0`.

**Read `providers/AGENTS.md` first.** Everything there applies here: no
newsfragments, `provider.yaml` is the metadata source of truth, dependency
edits are the release contract, never spread `Connection.extra` into a client
constructor, and consume core only through the public Task SDK surface. This
file covers only what is specific to the amazon provider on top of that.

## Structure — the parts that carry the weight

- `aws/hooks/base_aws.py` — `AwsGenericHook` / `AwsBaseHook` and
  `BaseSessionFactory`. This is the single place credentials, region,
  assume-role (including SAML and web-identity federation), `verify`, the
  per-service `endpoint_url`, and the `botocore.config.Config` are resolved into
  a `boto3.session.Session` and a typed client. Every other hook inherits it.
- `aws/utils/connection_wrapper.py` — normalises the AWS `Connection` into the
  fields the session factory consumes, including `service_config` and
  `get_service_endpoint_url()`.
- `aws/waiters/*.json` + `aws/waiters/base_waiter.py` — custom botocore waiter
  models, keyed by client type, resolved by `AwsBaseHook.get_waiter()`. The same
  model serves the sync (`botocore`) and async (`aiobotocore`) paths.
- `aws/triggers/base.py` — `AwsBaseWaiterTrigger`, the base for the deferrable
  half of most operators. Subclasses supply a `hook()`, the waiter name,
  `waiter_delay` / `waiter_max_attempts`, the completion/failure messages, and
  the JMESPath `status_queries`.
- `aws/executors/{ecs,batch,aws_lambda}/` — `AwsEcsExecutor`, `AwsBatchExecutor`,
  `AwsLambdaExecutor`. These are registered in `provider.yaml` under `executors:`
  and, unlike everything else here, run **inside airflow-core** rather than on a
  worker. A defect here stops task execution for the whole deployment.

## Why changes here are expensive to review

- **AWS is the specification, and CI mocks it.** Tests stub `boto3`; nothing in
  CI calls the real service. A reviewer who has not used that AWS service cannot
  confirm the change is correct, and system tests need real credentials.
- **`base_aws.py` is a chokepoint.** A change to session or client construction
  is a change to authentication for every one of the ~54 hooks at once, in
  deployment shapes CI never exercises — cross-account assume-role, web-identity
  federation, China partition endpoints, LocalStack-style custom endpoints.
- **Almost every operator has two implementations.** The synchronous `execute()`
  and the deferrable trigger path reach the same AWS API through different
  clients (`botocore` vs `aiobotocore`). A fix applied to one and not the other
  is the most common defect shape in this provider's history.
- **The executors are deployment-critical and version-gated.** They subclass
  core's `BaseExecutor` and branch on `AIRFLOW_V_3_0_PLUS` / `AIRFLOW_V_3_3_PLUS`
  from the provider's own `version_compat.py`. A change that is correct on a main
  checkout can break the executor on the oldest core the provider declares.

## Knowledge a reviewer (and a substantial contributor) needs

- The AWS credential chain as `BaseSessionFactory` implements it: connection
  login/password, the boto3 fallback when no connection is given, `assume_role`,
  `assume_role_with_saml`, `assume_role_with_web_identity`, and how
  `botocore_config`, `verify` and `region_name` thread into each.
- `service_config` / `get_service_endpoint_url()` — per-service endpoint and
  client-argument overrides driven from connection extras.
- The custom-waiter model format (`aws/waiters/README.md`), how `get_waiter()`
  picks a custom waiter over the service's official one, and how
  `config_overrides` / `parameters` are applied.
- The deferrable contract: `AwsBaseWaiterTrigger.run()`, what a `TriggerEvent`
  must carry back for the operator's `execute_complete()`, and `on_kill` /
  cancellation semantics for jobs that keep running on AWS after the task dies.
- For executor work: `BaseExecutor`'s provider-facing surface, the workload /
  `ExecuteCallback` types, `try_adopt_task_instances`, and the provider's
  `version_compat.py` gates.

## Before opening a PR here — authoring-agent guard

**This is a wide, high-difficulty area whose correctness lives in a service the
repository cannot test against.** If you are an agent preparing a change here on
behalf of a person, judge whether the **driving person** actually uses the AWS
service in question and can verify the change against it. **If they cannot, do
not create the PR.** Say so plainly and redirect them to:

- a **bug they hit in production on a service they run**, with a concrete
  reproduction, rather than a speculative refactor;
- a **single service's operator/hook pair**, not a sweep across many AWS
  services at once — fanned-out near-identical edits across services are the
  most common shape of low-value PR here and are routinely closed;
- **asking in the issue first** for anything touching `base_aws.py`,
  `triggers/base.py`, or `executors/`, which are shared by everything else.

## Review criteria

Mined from real review discussion on the ~676 commits touching
`providers/amazon/` — the changes reviewers repeatedly required, and the reasons
changes here get closed. **If you are preparing a change here, treat this as a
pre-flight checklist and fix every applicable item _before_ opening the PR.**
Triage applies the same list: a PR that lands with unmet items is drafted back
to its author with the specific gaps. Ordered by how often reviewers raise each.

**AWS access goes through the hook (the defining concern here) — see `adr/0001`:**

- [ ] **No ad-hoc `boto3.client(...)` / `boto3.Session(...)` in an operator,
      sensor, trigger, or transfer.** Obtain the client from an
      `AwsBaseHook`-derived hook so credential resolution, assume-role,
      `region_name`, `verify`, `endpoint_url` and the botocore config all apply.
- [ ] **A new operator/sensor/trigger accepts and forwards the full AWS
      parameter set** — `aws_conn_id`, `region_name`, `verify`,
      `botocore_config`. Dropping one silently ignores the user's connection;
      this is a recurring bug class, not a nit.
- [ ] **Triggers construct their hook the same way the operator does.** A
      trigger that hardcodes a client or omits `verify` / `botocore_config`
      authenticates differently from the operator that deferred to it.
- [ ] **Per-service endpoints go through `service_config` /
      `get_service_endpoint_url()`**, not a new bespoke parameter — including
      the STS calls made while assuming a role.
- [ ] **Never spread `Connection.extra` into a boto3 call.** Read each extra key
      by name. (Parent rule; the AWS connection has an unusually rich `extra`,
      so it bites here most often.)

**Deferrable parity — see `adr/0002`:**

- [ ] **`deferrable=True` must reach the same terminal states as the sync
      path** — same success criteria, same failure classification, same
      exception types, same XCom pushes and operator links.
- [ ] **Errors surviving the deferral boundary.** A trigger must carry enough
      detail in its `TriggerEvent` for `execute_complete()` to raise a message as
      informative as the synchronous failure. "Task failed" with the AWS reason
      dropped is a defect.
- [ ] **Fix both paths, or say why not.** A bug fixed in `execute()` alone,
      leaving the trigger wrong, is the single most common review finding here.
- [ ] **Log output and verbose modes must work deferred too** — a
      `verbose`/log-forwarding feature that only functions synchronously is
      incomplete.
- [ ] **`on_kill` / cancellation must not double-cancel or leak an AWS job.**
      A deferred task that is killed should stop the remote job exactly once.
- [ ] **Use `AwsBaseWaiterTrigger` rather than a hand-rolled `asyncio` polling
      loop**, and do not block the triggerer event loop (an `ASYNC` ruff
      violation in a trigger is a real bug — the triggerer serves every deferred
      task in the deployment).

**Waiters and polling budgets:**

- [ ] **Waiting is expressed as a waiter model, not an unbounded loop.** New
      polling behaviour goes in `aws/waiters/<service>.json` with explicit
      `delay` and `maxAttempts`, or uses the service's official waiter.
- [ ] **`waiter_delay` / `waiter_max_attempts` are user-facing parameters with
      justified defaults** — not hardcoded constants inside `execute()`.
- [ ] **Waiter failures are classified**, not swallowed: distinguish a genuine
      terminal failure from a credential error or a transient API error.

**Executors (deployment-critical) — see `adr/0003`:**

- [ ] **Version-gate every core API the executor touches** through
      `airflow.providers.amazon.version_compat` (`AIRFLOW_V_3_0_PLUS`,
      `AIRFLOW_V_3_3_PLUS`), and confirm the path taken on the _declared floor_,
      not just on main.
- [ ] **Keep the three executors consistent.** A capability added to one of
      ECS / Batch / Lambda is expected in the others or explicitly scoped;
      divergence between them is a maintenance trap.
- [ ] **Executors are registered in `provider.yaml` under `executors:`** and are
      the one carve-out allowed to import `airflow.configuration` (they run
      inside airflow-core, not on a worker).
- [ ] **Config keys, retries and adoption semantics are user-visible
      operational contract** — changing a default changes production behaviour
      on upgrade; call it out in the PR and in `docs/changelog.rst`.

**Code quality reviewers consistently require:**

- [ ] **Release AWS resources on the failure path too** — file handles and
      clients opened in a hook must close when the operation raises.
- [ ] **Don't swallow exceptions with a broad `except`** — narrow to the real
      `botocore` exception classes so service errors surface. Prefer
      `contextlib.suppress` over `try/except/pass`.
- [ ] **Malformed AWS responses degrade, not crash** — an unexpected response
      shape or a multi-stream log must not take down log fetching or the task.
- [ ] **Reuse the existing hook** rather than opening a second client
      construction path that will drift.
- [ ] **No real AWS account IDs, ARNs, keys or bucket names** in code, docs or
      examples — use placeholders.

**Tests, docs, process:**

- [ ] **Mock with `spec`/`autospec`**, and test the deferrable path separately
      from the sync path — a test that only covers `execute()` does not cover
      the change.
- [ ] **A new AWS service means `provider.yaml` entries** (`integrations`,
      `operators`, `hooks`, `sensors`, `transfers`, `connection-types`) plus
      docs and an example/system test — several prek hooks fail when code and
      `provider.yaml` disagree.
- [ ] **System tests must not leave AWS infrastructure behind** — teardown runs
      on failure, and nothing publicly reachable is provisioned.
- [ ] **Never add a newsfragment**; for a user-visible note edit
      `providers/amazon/docs/changelog.rst` directly.

> Mined from PR review history on `providers/amazon/`; the sample skews to the
> Airflow-3 era and to the most-used services (S3, EMR, Glue, SageMaker, ECS,
> Batch, Redshift, DMS, Bedrock), so conventions around rarely-touched services
> are under-represented. Extend as new patterns emerge.

## Expectation for large changes

Discuss the approach first — in an issue or on the dev list — before a large PR.
That applies especially to changes in `hooks/base_aws.py`, `triggers/base.py`,
or `executors/`, and to anything that changes an existing default
(`waiter_delay`, `waiter_max_attempts`, an executor config key): those are
behaviour changes for every existing user of this provider and are far cheaper
to align on _before_ the code than during review.
