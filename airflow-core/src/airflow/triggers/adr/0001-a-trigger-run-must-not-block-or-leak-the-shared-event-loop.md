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

# 1. A trigger's run() must not block or leak the shared event loop

Date: 2026-07-19

## Status

Accepted

## Context

The triggerer is a multiplexer: one process reconstructs every trigger handed to
it by deferred tasks and drives their `run()` coroutines **cooperatively on one
asyncio event loop** — one lightweight process watching thousands of conditions
instead of a blocked worker slot per condition.

This only holds if every trigger is a *good citizen* of the shared loop.
`BaseTrigger.run()` is an async generator that `await`s its I/O, `yield`s a
`TriggerEvent`, and returns. The loop only makes progress on other triggers while
the current one is `await`ing, so a single synchronous call — blocking socket
read, `requests`, `time.sleep`, a CPU-bound loop — parks the *entire* loop and
freezes every unrelated deferred task on that triggerer.

Shared lifetime also makes resource discipline non-negotiable. The runner cancels
`run()` on shutdown, timeout, and redistribution; a trigger that is not
cancellation-safe or leaks a connection / task / socket leaks into a long-lived
process. The two lifecycle hooks are *not* interchangeable: `cleanup()` runs on
**every** exit and releases this trigger's local resources; `on_kill()` runs
**only** on explicit user action and is the one place to cancel *external* work.
Putting external-job cancellation in `cleanup()` would cancel in-flight work on
every restart or rolling deploy. The recurring pressure is to treat `run()` like
synchronous code and fold external teardown into the always-run path.

## Decision

A trigger's `run()` must be a well-behaved citizen of the shared event loop,
and its teardown must respect the two-hook contract. Concretely:

- `run()` must **never block the event loop**: no synchronous network / file /
  DB call, no `time.sleep`, no unbounded CPU-bound work. Use `await` on real
  async I/O, and delegate unavoidable blocking work to an executor.
- `run()` must be **cancellation-safe**: the runner cancels it on shutdown,
  timeout, and redistribution. `CancelledError` must be allowed to propagate
  (never swallowed by a broad `except`), and any partial state unwound.
- Every local resource the trigger opens must be **released in `cleanup()`**,
  which the runner calls on every exit. External work is cancelled **only in
  `on_kill()`**, on explicit user action — never in `cleanup()`.
- A single trigger failure must **degrade only that trigger**, never crash the
  triggerer or its sibling triggers.

## Consequences

- The triggerer stays a dense, reliable multiplexer: one bad trigger cannot
  stall or crash deferral for its process-mates.
- Trigger authors must write genuinely async I/O and handle cancellation — more
  demanding than a synchronous sensor, intentionally so.
- The `cleanup()` / `on_kill()` split is what lets the triggerer redistribute and
  restart triggers without killing external jobs.

A change **violates** this decision when, in a trigger's `run()` (or the
lifecycle code around it) reachable from the triggerer, it:

- introduces a synchronous blocking call, `time.sleep`, or CPU-bound spin on
  the event-loop path instead of awaiting async I/O or offloading to an
  executor;
- swallows `CancelledError` (or otherwise defeats the runner's cancellation),
  or leaves a connection / task / socket open across a cancelled or completed
  `run()`;
- cancels external work in `cleanup()` instead of `on_kill()`, or moves
  local-resource release out of `cleanup()`;
- lets one trigger's failure propagate far enough to crash the triggerer or
  its sibling triggers.

A reviewer should reject any trigger change that would let one trigger monopolise
or corrupt the shared event loop.

## Evidence

- #66584 — collapses per-trigger poll loops into one shared loop, reducing each
  trigger's load on the shared event loop.
- #65590 — introduced the `on_kill()` hook and the `cleanup()` line, so
  external-work cancellation happens only on user action.
- #68888 — `shared_stream_cohort_grace_period` tunes restart/redistribution so
  cancelling and re-establishing triggers does not silently drop events.
