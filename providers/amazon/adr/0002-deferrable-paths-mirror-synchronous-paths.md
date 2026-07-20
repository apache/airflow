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

# 2. Deferrable paths mirror synchronous paths rather than forking behaviour

Date: 2026-07-20

## Status

Accepted

## Context

Most operators in this provider have two implementations of the same job.
`deferrable=False` runs a synchronous `execute()` that polls AWS with a
`botocore` waiter and returns. `deferrable=True` submits the work, yields an
`AwsBaseWaiterTrigger` subclass, and the triggerer polls with an `aiobotocore`
waiter until it emits a `TriggerEvent` that the operator's `execute_complete()`
turns back into success or failure.

The design already anticipates this. `AwsBaseHook.get_waiter()` serves both
modes from the *same* waiter model, and `BaseBotoWaiter` builds either a
`botocore` or an `aiobotocore` waiter from one JSON config. `AwsBaseWaiterTrigger`
exists so that the deferrable half is a declaration — hook, waiter name, delay,
max attempts, completion and failure messages, status queries — rather than a
second hand-written polling loop.

What the design cannot enforce is that the two halves stay semantically equal,
and in practice they drift. The drift is always in the same direction: a
behaviour is implemented or fixed on the synchronous side, and the deferrable
side keeps the old, wrong, or missing behaviour. Verbose job logs have appeared
only when running synchronously. A sensor's filtering and callback arguments have
been honoured only in sync mode. Exit-code handling, cancellation on kill, and
error-detail propagation have each been correct on one side and not the other.

The user impact is worse than an ordinary bug because `deferrable` is advertised
as a resource optimisation, not a behaviour switch. A deployment flips it on to
free worker slots and silently gets different failure semantics — often *less*
diagnostic information, because the exception detail that the synchronous path
raised directly has to survive serialization through a `TriggerEvent` to reach
`execute_complete()`, and it is easy to drop on the way.

There is a second-order cost too: triggers run in the triggerer, which serves
every deferred task in the deployment. A hand-rolled polling loop that blocks
the event loop degrades unrelated Dags, which a synchronous implementation of
the same mistake never would.

## Decision

`deferrable=True` changes *where* the waiting happens, and nothing else.

- **The deferrable path must reach the same terminal states as the synchronous
  path** — the same success criteria, the same classification of failure, the
  same exception types raised to the task, the same XCom pushes and operator
  links.
- **A behaviour change or bug fix applies to both halves in the same change.**
  If only one half is touched, the PR states explicitly why the other is
  unaffected.
- **Failure detail must survive the deferral boundary.** A trigger's
  `TriggerEvent` carries enough information for `execute_complete()` to raise a
  message as informative as the synchronous failure. A generic "task failed"
  that discards the AWS reason is a defect.
- **Wait through `AwsBaseWaiterTrigger` and a waiter model**, not a bespoke
  `asyncio` loop. Trigger code must not block the event loop; a blocking call in
  a trigger is a deployment-wide problem, not a style issue.
- **Cancellation is symmetric and idempotent.** If the synchronous operator
  stops the remote AWS job on kill, the deferrable one does too — exactly once,
  with no double cancellation across core versions.
- **The trigger builds its hook the way the operator does** (ADR 1), so the two
  paths also authenticate identically.
- **Both paths are tested.** A test that exercises only `execute()` does not
  cover a change to an operator that supports deferral.

## Consequences

- `deferrable` stays a safe operational toggle: turning it on changes resource
  usage, not outcomes.
- The waiter model remains the single description of "what done looks like" for
  a given AWS operation, shared by both clients.
- Adding deferral to an operator costs more than adding a trigger class — the
  terminal states, error text and cancellation semantics have to be matched —
  and that cost is deliberate.
- Some fixes become two-sided and therefore larger. A one-sided fix that leaves
  the other path wrong is not the cheaper option; it is the more expensive one,
  paid later by a user who flipped a flag.

A change **violates** this decision when it:

- fixes or extends behaviour in `execute()` while leaving the corresponding
  trigger unchanged (or vice versa), without stating why the other path is
  unaffected;
- lets the deferrable path reach a different terminal state, raise a different
  exception type, or push different XComs than the synchronous path for the same
  AWS outcome;
- drops the AWS failure reason when converting a waiter error into a
  `TriggerEvent`, so `execute_complete()` can only report a generic failure;
- implements waiting as a hand-rolled polling loop instead of a waiter model, or
  makes a blocking call inside a trigger;
- cancels a remote job on kill in only one mode, or cancels it twice;
- ships a deferrable feature whose tests cover only the synchronous path.

## Evidence

- #64085 — "Fix `AwsBaseWaiterTrigger` losing error details on deferred task
  failure": the deferral boundary discarding exactly the diagnostic detail the
  synchronous path raised.
- #64342 / #63086 (and its revert, #64340) — "fix(glue): Fix `GlueJobOperator`
  verbose logs not showing in deferrable mode": a user-facing feature that
  existed only in sync mode, and a fix hard enough to need two attempts.
- #56910 — "Fix: `S3KeySensor` deferrable mode ignores `metadata_keys`, returns
  only key names, and doesn't pass context to `check_fn`": three separate
  divergences in one sensor.
- #60978 — "Fix/ssm deferrable exit code handling": success/failure
  classification differing between the two paths.
- #60440 — "Add `cancel_on_kill` support for EMR Serverless deferrable
  operator" and #65997 — "Fix double cancellation for airflow 3.3+ for
  `EmrServerlessStartJobTrigger`": cancellation first missing on the deferred
  path, then firing twice.
- #61195 — "Fix hardcoded waiter logic in `EmrCreateJobFlowOperator`": waiting
  behaviour baked into the operator instead of expressed as configuration
  shared with the trigger.
- #66157 — "Fix ASYNC110 violation in `RedshiftDataTrigger`": a hand-rolled
  wait inside a trigger, in the process that serves every deferred task.
