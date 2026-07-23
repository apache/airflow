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

Most operators here have two implementations of the same job. `deferrable=False`
runs a synchronous `execute()` that polls AWS with a `botocore` waiter;
`deferrable=True` submits the work, yields an `AwsBaseWaiterTrigger` subclass, and
the triggerer polls with an `aiobotocore` waiter until a `TriggerEvent` that
`execute_complete()` turns back into success or failure. The design anticipates
this: `AwsBaseHook.get_waiter()` serves both modes from the *same* waiter model,
and `AwsBaseWaiterTrigger` makes the deferrable half a declaration rather than a
second hand-written polling loop.

What the design cannot enforce is that the two halves stay semantically equal,
and they drift — always in the same direction, with a behaviour implemented or
fixed on the synchronous side while the deferrable side keeps the old or missing
behaviour (verbose logs sync-only, a sensor's filtering honoured only in sync
mode, exit-code and cancellation handling correct on one side). The user impact
is worse than an ordinary bug because `deferrable` is advertised as a resource
optimisation: a deployment flips it on to free worker slots and silently gets
different failure semantics — often *less* diagnostic detail, because the
exception the sync path raised directly must survive serialization through a
`TriggerEvent`. There is a second-order cost too: triggers run in the triggerer,
which serves every deferred task, so a hand-rolled loop that blocks the event loop
degrades unrelated Dags.

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
- The waiter model remains the single description of "what done looks like",
  shared by both clients.
- Adding deferral costs more than adding a trigger class — terminal states, error
  text and cancellation semantics have to be matched — and that cost is deliberate.
- Some fixes become two-sided and larger. A one-sided fix is not the cheaper
  option; it is the more expensive one, paid later by a user who flipped a flag.

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

- #64085 — the deferral boundary discarding exactly the diagnostic detail the
  synchronous path raised.
- #64342 / #63086 (and its revert, #64340) — verbose logs existing only in sync
  mode; a fix hard enough to need two attempts.
- #56910 — three separate sync/deferred divergences in one sensor.
- #60978 — success/failure classification differing between the two paths.
- #60440, #65997 — cancellation first missing on the deferred path, then firing twice.
- #61195 — waiting behaviour baked into the operator instead of shared with the trigger.
- #66157 — a hand-rolled wait inside a trigger, in the process serving every deferred task.
