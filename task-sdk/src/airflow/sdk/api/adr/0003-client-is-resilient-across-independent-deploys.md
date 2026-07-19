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

# 3. The client is resilient across independent worker/server deploys

Date: 2026-07-19

## Status

Accepted

## Context

Workers and API servers are upgraded on their own schedules and connected only by
the network in between: an operator may roll the control plane forward while a
worker fleet still runs an older SDK, and the link itself drops packets, times
out, and returns transient `5xx` during a rolling restart. A task that has
already started must not fail merely because the server it talks to is a
different build, briefly unreachable, or mid-deploy. This client is what absorbs
that.

Three properties carry it. *Transient-failure tolerance*: `request()` is wrapped
in a tenacity `@retry` that retries **only** `httpx.RequestError` and `5xx`
responses (`_should_retry_api_request`) with randomized exponential backoff and a
bounded timeout, so a blip is ridden out but a `4xx` or a non-idempotent
duplicate is not. *Token continuity*: a long-running task's JWT is refreshed in
place from the server's `Refreshed-API-Token` header, so the task doesn't die of
an expired credential while it waits. *Transport trust across environments*: a
cached SSL context supports public CAs, private CAs, and mutual TLS, so the same
client works whether the server presents a public or an internal certificate.
Underneath all three is the assumption the client must *not* make — that the
server on the other end is the exact version the worker shipped with.

## Decision

The client is built to survive independent worker/server deploys rather than
assume a matched pair. Concretely:

- Transient failures (`httpx.RequestError`, `5xx`) are retried with bounded
  exponential backoff and a configurable timeout; `4xx` responses and
  non-idempotent operations are **not** blindly retried.
- The task token is refreshed from the server's `Refreshed-API-Token` header *in
  place*, so long-running work isn't killed by expiry.
- The SSL context (public CA / private CA / mTLS) is built once and cached, and
  server errors are classified (`ServerResponseError`, typed `ErrorResponse`)
  rather than crashing the worker.
- The client tolerates talking to an older or a newer server — model parsing and
  error handling do not assume the server speaks exactly the version the worker
  shipped.

## Consequences

- Workers survive rolling upgrades, brief outages, and transient `5xx` without
  failing the task that happened to be mid-flight.
- The client carries more code and more configuration knobs (retry count, wait
  bounds, timeout, TLS trust) than a naive HTTP wrapper — that surface is the
  cost of safe independent deploys.
- The retry/backoff discipline must stay narrow (transient-only, idempotent-only)
  or it amplifies load against a struggling server and risks double-writes.

A change **violates** this decision when it:

- retries a `4xx` or a non-idempotent write, or removes the transient-only guard
  so every error is retried;
- drops the timeout/backoff bounds or the token-refresh path, so a slow or
  long-running call hangs or dies of expiry;
- recreates the SSL context per request (memory growth) or hard-codes a single
  trust mode, breaking private-CA / mTLS deploys;
- assumes the server is on the worker's exact version — e.g. hard-requires a
  field an older server won't send — instead of degrading.

## Evidence

- #56762 — "Migrate retry handler in task SDK API client to use tenacity instead
  of retryhttp": the transient-only retry/backoff layer this decision rests on.
- #56969 — "Add configurable timeout for Execution API requests": the bounded
  timeout that keeps a call from hanging against a slow server.
- #57334 — "Fix memory leak in Client via SSL context creation" (with #57401,
  "make `_get_ssl_context_cached` a static method"): the cached SSL context.
- #67214 — "Add support for mTLS and private CAs to the api client / server" (and
  #62105, "Support for client-side certificates using task-sdk"): transport trust
  across public and internal environments.
- #68129 — "Escape URL for DagOperations lookup in task sdk client": robust
  request construction so a worker request survives server-side routing.
