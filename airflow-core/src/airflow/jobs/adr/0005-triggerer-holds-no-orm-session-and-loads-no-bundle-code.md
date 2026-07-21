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

# 5. The async trigger runner holds no ORM session and loads no Dag-bundle code

Date: 2026-07-20

## Status

Accepted

## Context

ADR 1 states the scheduler side of the isolation boundary: it holds a privileged
database session and never runs user code. The triggerer is the mirror image, and
the half contributors trip over. `TriggererJobRunner` runs user-authored trigger
classes — thousands concurrently, in one asyncio event loop, in a long-lived
process. Two properties follow.

**The runner subprocess is on the untrusted side.** Because it executes author
code, it must not hold a direct metadata-DB session; it reaches server state
through the Execution API like a worker. The runtime enforces this — a
`@provide_session` call reached from a running trigger raises *"Direct database
access via the ORM is not allowed"*. The fix for a new server-side model the
triggerer wants is not to grant it a session but to expose the data through the
Execution API, or move the work to a component that legitimately has one.

**The triggerer does not initialize Dag bundles.** This is documented behaviour:
everything runs in one process with no per-run isolation and no versioning of
trigger code, so there is no correct answer to "which bundle version does this
trigger come from" when a trigger outlives a Dag version. Triggers must therefore
be importable from `sys.path`, not from a Dag bundle. The recurring PR shape — add
the bundle root to the subprocess path, or load triggers from a zip — is refused:
it reintroduces unversioned user code into a process with no mechanism for code
that changes over time. Both failures share a shape: the triggerer looks like a
convenient place to put work precisely because it has given up the privileges that
would make the work safe.

## Decision

- **The boundary is the supervisor/runner split, not the component.**
  `TriggererJobRunner` and `TriggerRunnerSupervisor` are server-side: they hold
  ORM sessions deliberately (`@provide_session` on `is_needed`, `create_session()`
  in `build_trigger_workloads`, `Trigger.bulk_fetch`), and `_execute()` sets
  `_AIRFLOW_PROCESS_CONTEXT="server"` on purpose so it can read metastore
  connections on the subprocess's behalf (#64022, merged). That is the design, not
  a violation of it.
- **The async runner subprocess never opens an ORM session.** `TriggerRunner`
  executes user trigger code and sets `_AIRFLOW_PROCESS_CONTEXT=client`; on that
  path there is no `@provide_session`, no
  `create_session()`, no direct model query on any code path reachable from a
  running trigger. Server state is read and written through the Execution API.
- **A new model the triggerer needs is an Execution API surface, not a session
  grant.** If a feature requires the triggerer to persist or read state, the
  design question to settle first is which Execution API endpoint carries it.
- **Triggers are imported from `sys.path`, never from a Dag bundle.** Do not add
  bundle initialization, bundle-root path injection, or archive loading to the
  triggerer or its subprocesses.
- **Keep the client context on the runner subprocess.** Code shared with the
  scheduler or API server must set and respect `_AIRFLOW_PROCESS_CONTEXT=client`
  on the triggerer side rather than assuming server privileges are available.
- **Do not move scheduler responsibilities into the triggerer** — queueing,
  dispatch, or state transitions that belong to the scheduler loop stay there,
  however tempting the triggerer's existing event loop looks.

## Consequences

- Features trivial with a database session cost an Execution API endpoint, its
  schema, and its versioning — the point being to keep the privileged surface
  enumerable.
- Shared helper code for triggers must be installed on `sys.path`, not shipped in
  a Dag bundle. Trigger code cannot be versioned per Dag version — a known
  limitation, not a defect. And a whole class of "make the triggerer do X" PRs is
  refused early, cheaper than refusing them after implementation.

A change **violates** this decision when it:

- adds `@provide_session`, `create_session()`, `NEW_SESSION`, or a direct model
  query to the async runner subprocess (`TriggerRunner`) or any code path
  reachable from a running trigger — `TriggererJobRunner` and
  `TriggerRunnerSupervisor` are server-side and hold sessions by design, so a
  change to *their* DB access is not a violation;
- wires a read or write of a metadata-DB model into the runner subprocess or a
  trigger, without a corresponding Execution API surface;
- relaxes, bypasses, or special-cases the direct-ORM-access guard so a triggerer
  path stops raising;
- initializes Dag bundles in the triggerer, injects a bundle root onto the
  triggerer's `sys.path`, or teaches the triggerer to load triggers from an
  archive;
- sets or leaves `_AIRFLOW_PROCESS_CONTEXT` to a server value on the **runner
  subprocess** path that executes user trigger code (the supervisor setting it is
  correct and intended — see above), or on a triggerer code
  path;
- shifts queueing, dispatch, or Dag-run state transitions from the scheduler loop
  into the triggerer.

A reviewer seeing `session` in a triggerer diff, or bundle handling anywhere near
trigger import, should stop there and ask which side of the boundary the code is
on.

## Evidence

- #55216 — adding a `TriggerWatermarks` model: the author showed `@provide_session` from a running trigger raises *"Direct database access via the ORM is not allowed in Airflow 3"*; did not land.
- #63353 — decoupling the triggerer client and server via the Execution API: closed as touching core internals reserved for contributors with deep familiarity.
- #66558 — earlier attempt at the deadline/server-context problem, closed by its author as a duplicate of #64022, which then **merged** and is the shape now on `main`. Cited for history, not as a prohibition.
- #65457 — "Fix triggerer subprocess unable to import helpers from Dag bundle root": closed with the docs quoted back — a trigger must not come from a Dag bundle; anywhere else on `sys.path` is fine.
- #52091 — "Triggerer: support loading triggers from zip archives": closed for the same reason, citing the AIP-66 note that bundles are not initialized in the triggerer because it cannot handle trigger code changing over time.
