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

Workers and API servers are upgraded on their own schedules, connected only by
the network: an operator may roll the control plane forward while a worker fleet
still runs an older SDK, and the link drops packets, times out, and returns
transient `5xx` during a rolling restart. A task already started must not fail
merely because its server is a different build, briefly unreachable, or
mid-deploy. This client absorbs that through three properties: *transient-failure
tolerance* — `request()` is wrapped in a tenacity `@retry` that retries **only**
`httpx.RequestError` and `5xx` (`_should_retry_api_request`) with randomized
exponential backoff and a bounded timeout, never a `4xx` or non-idempotent
duplicate; *token continuity* — the JWT is refreshed in place from the
`Refreshed-API-Token` header; and *transport trust* — a cached SSL context
supports public CAs, private CAs, and mTLS. Underneath all three is the
assumption the client must *not* make: that the server is the exact version the
worker shipped with.

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
  failing a mid-flight task.
- The client carries more code and config knobs (retry count, wait bounds,
  timeout, TLS trust) than a naive HTTP wrapper — the cost of safe independent
  deploys.
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

- #56762 — migrate retry handler to tenacity; the transient-only retry/backoff
  layer this rests on.
- #56969 — configurable timeout that keeps a call from hanging against a slow
  server.
- #57334 (with #57401) — cached SSL context, fixing a memory leak.
- #67214 (with #62105) — mTLS and private-CA support; transport trust across
  public and internal environments.
- #68129 — escaped URL for DagOperations lookup; robust request construction
  surviving server-side routing.
