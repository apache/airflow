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

# 5. Bundle identity travels as data; scheduler-side components never import user code

Date: 2026-07-20

## Status

Accepted

## Context

The scheduler must never run user code. Executors run in the scheduler process, so
the constraint extends to them directly: whatever an executor hands a worker has
to be data — identifiers, versions, serialized fields — never the result of
importing a Dag file or anything reachable from a Dag bundle. The triggerer is a
**separate component** that reaches the same conclusion by its own route (it runs
user trigger code). **The triggerer half of this rule is decided in
`../../jobs/adr/0005`**, which owns the triggerer's isolation contract and the
bundle-loading question, including #65457. This ADR is authoritative only for the
executor and workload side, and states the triggerer case because the two share a
mechanism — not to judge the same PR twice.

The pressure to break the rule is constant and sympathetic: a helper module next
to a trigger fails to import, which reads as a missing `sys.path` entry, and the
one-line fix makes the symptom disappear — proposed and rejected exactly that way.
The supported alternative moves identity, not code: the executor carries which
bundle and version a task belongs to as fields, and worker-side initialisation
resolves that into a checkout before user code is imported, in the worker. That is
what `BundleInfo` / `version_data` threading does, and why the manifest is pinned
to the Dag run's version — two tasks in a run must see the same code, decided
where the run's state lives. The same data-not-behaviour rule shapes the payload:
the serialized workload is a minimal, versioned schema travelling through JWTs,
container argv, and queue bodies; executor-only fields are kept out, and secrets
are excluded or marked non-representable (a JWT was excluded from the workload
`repr`).

## Decision

Scheduler-side components pass identity, not code:

- **Executors and their supervisors do not import from a Dag bundle**, do not add
  a bundle path to `sys.path`, and do not initialise bundles. Code they need must
  be importable independently of any bundle. The same holds for the triggerer,
  under `../../jobs/adr/0005`.
- **Bundle and version identity travel in the workload** (`BundleInfo`,
  `version_data`) and are resolved to a checkout on the worker side, before user
  code is imported and only there.
- **Bundle version is pinned at the Dag run level**, so every task in a run
  executes against the same code; do not resolve "latest" at dispatch time.
- **The workload payload stays a minimal, versioned data schema** — no
  executor-only fields, no whole serialized Dag where a task reference suffices,
  and nothing secret-bearing in it or in its `repr`.

## Consequences

- The scheduler and triggerer stay immune to user code — a bad import in one Dag
  cannot take down the process serving every other Dag — and execution is
  reproducible, because a run's code is decided once and recorded.
- The cost lands on Dag authors: a trigger cannot import the helper beside it, and
  the failure gives no hint it is intentional. The workaround is packaging and
  installing the helper — real deployment work a `sys.path` line would have
  avoided — so contributors keep arriving with that one-line fix.

A change **violates** this decision when it:

- adds a Dag-bundle path to `sys.path`, or imports a module from a bundle, in
  executor or executor-supervisor code (the triggerer case is judged under
  `../../jobs/adr/0005`);
- initialises or checks out a bundle inside a scheduler-side process in order to
  resolve something at dispatch time;
- resolves a bundle version per task or at dispatch rather than carrying the
  run's pinned version through the workload;
- adds executor-only fields, long-lived credentials, or secrets other than the
  run's own short-lived scoped execution token, to the serialized workload schema,
  or widens it to carry code or a full serialized Dag.

## Evidence

- #65457 — triggerer-side illustration of the same mechanism (closed as contrary to documented behaviour). Owned by `../../jobs/adr/0005`; context only, not a basis for flagging a PR against this ADR.
- #67217 — `version_data` threaded through `BundleInfo` to worker-side bundle initialisation.
- #69941 — task bundle manifest pinned to the Dag run's version.
- #68390 — worker-bound `TaskInstance` fields versioned in the execution API schema.
- #62964 — JWT excluded from the workload `repr` to prevent log exposure.
- #62467 — a JWT-in-task-logs fix, closed in favour of #62129, which addressed it where the concern belongs rather than in the executor's log path.
