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

# 5. Async AWS clients are per-trigger and are never pooled or shared

Date: 2026-07-20

## Status

Accepted

## Context

Every deferred task runs on the triggerer, which runs its triggers as coroutines
on a single event loop per process. This provider is the heaviest user of that
loop — AWS waiters are the canonical deferrable pattern, so a busy deployment holds
thousands of AWS triggers at once, each with an `aiobotocore` client. That makes
client construction look like the obvious thing to optimise (it resolves
credentials, may assume a role, loads service models), and three separate attempts
proposed caching, pooling, or sharing async clients.

All three were closed for structural reasons. An `aiobotocore` client is bound to
the loop that created it — connector, connection pool and transport all belong to
that loop — so using it from another loop or thread produces failures far from
their cause, under load, in the component whose failure affects every deferred
task; a pool keyed coarser than "this trigger, on this loop" is a latent
cross-thread bug. Sharing is also wrong on identity: `ADR 0001` resolves all AWS
access through `AwsBaseHook`, so a client is specific to an `aws_conn_id`,
`region_name`, `verify`, `endpoint_url`, botocore config and any assumed role, and
a cache that gets the key wrong silently performs one tenant's call with another
tenant's credentials. The performance concern is real and has a supported answer
needing no shared state: do not block the loop, keep waiter polling in
`AwsBaseWaiterTrigger`, let each trigger own its client for its own lifetime, and
justify any change with a measurement.

## Decision

An async AWS client belongs to exactly one trigger instance, on one event loop,
for that trigger's lifetime.

- **No cross-trigger client pool, cache, or registry.** Do not introduce
  module-level, class-level, or process-level caches of `aiobotocore` clients or
  their sessions.
- **No client sharing across event loops or threads.** A client created on one
  loop is used only on that loop, and is closed on that loop.
- **Reuse within a single trigger is fine, via `async with await
  hook.get_async_conn()`** covering that trigger's own polling — the pattern
  every trigger in `aws/triggers/` uses. Do **not** reach for a `cached_property`
  holding an async client: `AwsBaseHook.async_conn` is deprecated precisely
  because touching it from async code blocks the event loop
  (`aws/hooks/base_aws.py`), and a cached client also outlives the `async with`
  scope that is supposed to close it.
- **Credential identity is part of client identity.** Any reuse is scoped by the
  full connection identity — `aws_conn_id`, `region_name`, `verify`,
  `endpoint_url`, botocore config, assumed role — and never by service name
  alone.
- **Do not block the triggerer loop.** Synchronous hook calls are wrapped;
  waiting is expressed through `AwsBaseWaiterTrigger` or an awaited primitive,
  never a synchronous sleep or a tight poll.
- **A triggerer performance change carries a measurement.** State what was
  measured, under what concurrency, and what changed — a plausible efficiency
  argument is not enough for code on the shared loop.
- **Clients are closed on the failure path**, including when the trigger is
  cancelled, so a killed deferred task does not leak connections on the loop.

## Consequences

- The triggerer stays correct under multi-account and multi-region load: no
  trigger can act with another trigger's credentials.
- Client construction cost is paid per trigger — accepted; correctness on a
  shared, deployment-wide component outweighs it.
- Legitimate triggerer optimisations are harder to land, because they must be
  measured. The bar is deliberate.
- The provider stays within `aiobotocore`'s supported usage, so upgrades do not
  require re-auditing bespoke pooling code.

A change **violates** this decision when it:

- adds a module-, class-, or process-level cache, pool, or registry of
  `aiobotocore` clients or sessions;
- passes an async client between triggers, threads, or event loops, or stores
  one where another loop could reach it;
- keys any client reuse on less than the full connection identity — for example
  on service name or region alone, ignoring `aws_conn_id`, `verify`,
  `endpoint_url`, the botocore config or an assumed role;
- blocks the triggerer event loop with a synchronous AWS call or a synchronous
  sleep, or hand-rolls a polling loop where `AwsBaseWaiterTrigger` applies;
- proposes a triggerer performance change with no measurement of the behaviour
  it claims to improve;
- leaves an async client open when the trigger raises or is cancelled.

## Evidence

- #53454 — closed after review asked whether sharing a client between hooks with
  different `aws_conn_id` is safe; each `aiobotocore` client is attached to a
  single asyncio loop tied to one thread, so sharing is not cross-thread safe.
- #54250 — a persistent/thread-safe client pool, withdrawn by its author as not
  worth the nuances.
- #54184 — the earlier client-pool attempt, abandoned in favour of investigating
  per-trigger reuse.
- #62239 — replacing S3 trigger sleep loops with `anyio.Event`; did not proceed on
  an unmeasured efficiency argument alone.
