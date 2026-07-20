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

# 1. All AWS access goes through AwsBaseHook session and credential resolution

Date: 2026-07-20

## Status

Accepted

## Context

Authenticating to AWS is not a one-line `boto3.client("s3")`. In this provider it
is the job of `BaseSessionFactory` and `AwsGenericHook` /`AwsBaseHook` in
`aws/hooks/base_aws.py`, and it resolves — in order — the Airflow connection's
credentials or the boto3 default chain when no connection is given, the region,
an optional role assumption (`assume_role`, `assume_role_with_saml`,
`assume_role_with_web_identity`, each with its own kwargs and federation
options), the `verify` TLS setting, a `botocore.config.Config`, the Airflow user
agent, and a per-service `endpoint_url` derived from the connection's
`service_config` via `get_service_endpoint_url()`. `AwsConnectionWrapper` in
`aws/utils/connection_wrapper.py` normalises the connection into the fields that
factory consumes.

That machinery is why a deployment can point one Airflow connection at a
cross-account role, another at a China-partition endpoint, and a third at a
local endpoint emulator, and have all ~54 hooks behave identically. Every piece
of it is invisible to a caller that constructs its own boto3 client: such a
caller silently ignores `aws_conn_id`, `region_name`, `verify` and
`botocore_config`, and it authenticates as whatever ambient credentials the
worker happens to have.

The failure mode is not theoretical, and it recurs. Operators have shipped
ignoring `aws_conn_id` outright. Triggers have shipped resolving credentials
differently from the operator that deferred to them, so a task authenticated one
way while running and another way while waiting. STS calls made during
web-identity role assumption bypassed the user's botocore config. Each of these
was a separate bug fix, and each was the same underlying mistake: a second,
partial path to a boto3 client.

Constructing clients elsewhere also destroys testability. The hook is the seam
the provider's tests mock; a client built inline in `execute()` has no seam.

## Decision

Every AWS API call in this provider is made through a client obtained from an
`AwsBaseHook`-derived hook.

- **No ad-hoc `boto3.client(...)`, `boto3.resource(...)` or
  `boto3.session.Session(...)` in operators, sensors, transfers, triggers,
  notifiers, or utility modules** — with the two by-construction exceptions
  named in the violations list, both of which run where no Airflow connection
  exists. Clients come from `hook.conn`,
  `hook.get_conn()`, `hook.get_client_type()`, or the service hook's own typed
  accessor.
- **The hook owns the whole resolution chain** — credentials, region,
  assume-role in all its forms, `verify`, botocore `Config`, user agent, and
  service endpoint. New authentication behaviour is added there, not at a call
  site, so it reaches every hook at once.
- **Public AWS parameters are accepted and forwarded intact.** Anything that
  constructs a hook — operator, sensor, or trigger — takes `aws_conn_id`,
  `region_name`, `verify` and `botocore_config` and passes them through
  unchanged. Dropping one is a defect, not a simplification.
- **A trigger resolves its hook exactly as its operator does.** The deferrable
  half of an operator must produce a session with the same identity, region and
  transport configuration as the synchronous half.
- **Per-service overrides go through `service_config` /
  `get_service_endpoint_url()`**, including the endpoint used for STS during
  role assumption — not through a new bespoke constructor argument.
- **Connection extras are read by name**, never spread into a boto3 call. This
  is the parent `providers/AGENTS.md` rule; the AWS connection's `extra` is
  unusually rich, so it applies here with unusual force.

## Consequences

- Authentication behaves the same across the whole provider. A deployment that
  configures cross-account roles, a custom partition, or a private endpoint
  configures it once, in the connection.
- New credential capabilities land in one file and are immediately available to
  every service.
- Hooks stay mockable, so tests can assert on call arguments without reaching
  into boto3 internals.
- Adding a service costs slightly more up front — a hook must exist before the
  operator — and that cost is the point: it is what keeps the second path from
  being created.

A change **violates** this decision when it:

- constructs a `boto3` client, resource, or session directly in an operator,
  sensor, transfer, trigger, notifier, or hook helper, instead of going through
  a hook. Two places are outside the hook layer by construction and are not
  violations: `aws/utils/eks_get_token.py`, a standalone CLI entrypoint that
  runs with the caller's ambient AWS environment variables and no Airflow
  connection, and `aws/executors/aws_lambda/docker/app.py`, which runs inside
  the Lambda image where no Airflow connection exists. Adding a *third* such
  site needs the same argument made explicitly;
- adds an operator, sensor, or trigger that omits `aws_conn_id`, `region_name`,
  `verify`, or `botocore_config`, or accepts them and does not forward them to
  the hook;
- builds a trigger's client differently from the operator's, so the deferred
  path authenticates or connects differently from the synchronous one;
- introduces a bespoke endpoint or client-config parameter instead of using
  `service_config` / `get_service_endpoint_url()`, or makes an STS call that
  bypasses the user's botocore config;
- passes `**conn.extra_dejson` (or an unfiltered kwargs dict derived from it)
  into a boto3 client constructor or API call.

## Evidence

- #63137 — "Fix - `S3GetBucketTaggingOperator` ignoring `aws_conn_id`
  parameter": an operator that took the connection id and never reached the
  hook with it.
- #65335 — "fix: `EksPodOperator` 401 with cross-account AssumeRole via
  `aws_conn_id`": role assumption not surviving the path to the client.
- #64216 — "Fix `assume_role_with_web_identity` not using botocore config for
  STS calls": a credential-resolution step that bypassed the user's transport
  configuration.
- #68923 — "Standardize `ECS TaskDoneTrigger` on `region_name` and AWS hook
  parameters", #68925 — "Propagate AWS hook parameters through
  `RedshiftClusterTrigger`", #68921 — "Propagate `verify` and `botocore_config`
  through `EC2StateSensorTrigger`", #67508 — the same propagation fix for the
  batch triggers: four separate fixes for the same defect
  — triggers that did not resolve their client the way the operator did.
- #68927 — "Make Amazon SageMaker triggers inherit AWS base classes" and
  #52243 — "Use base AWS classes in Glue Trigger / Sensor and implement custom
  waiter": bespoke client construction retired in favour of the base classes.
- #65821 — "Add multi-team dimensions to boto3 user agent string": a change made
  once in the session factory and inherited by every hook.
