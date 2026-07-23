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

# 3. Dag code is loaded from versioned bundles, and parsing records the bundle version

Date: 2026-07-19

## Status

Accepted

## Context

Airflow 3 replaced the single shared "DAGs folder" with **Dag bundles**: named,
independently-versioned sources of Dag code (`dag_processing/bundles/`), so runs
are reproducible across distributed components. A Dag is produced from a specific
bundle at a specific version, and that identity has to travel with it: when the DFP
parses a file it records `bundle_name` / `version_data` on the `DagVersion`, and
that identity is threaded into callbacks and into the worker so it initializes the
*same* bundle version before running the task. If any link drops the version, a
worker can run against a different revision than was scheduled — a silent
correctness bug ordinary tests rarely reproduce.

Bundle ownership is also shared state: in multi-processor / multi-team deployments
more than one dag-processor writes the bundles table, so a processor must not treat
it as exclusively its own.

## Decision

Dag code is sourced from versioned bundles, and the bundle identity is persisted
by the DFP and threaded end-to-end:

- The DFP records `bundle_name` / `version_data` with the serialized Dag /
  `DagVersion` when it parses a file.
- Any change that adds or moves bundle metadata must carry it through the whole
  chain — processor → serialized Dag → callbacks → worker-side bundle init — so
  the version that scheduled a run is the version that runs it.
- A dag-processor scopes its bundle writes to the bundles it owns; it does not
  deactivate or mutate bundles owned by another processor / team.

## Consequences

- Runs are reproducible: the code a worker executes is pinned to the bundle
  version the DFP recorded, not "whatever is on disk now."
- Bundle metadata is a cross-cutting field: adding to it is not a local change —
  it must be wired through serialization and the worker hand-off, and kept in
  sync with `get_serialized_fields()`.
- Multi-processor / multi-team deployments are first-class: shared bundle state
  must be written defensively, scoped to ownership.

A change **violates** this decision when it:

- loads Dag code by bypassing the bundle abstraction (e.g. reading a raw shared
  path directly) instead of resolving through a bundle;
- persists or moves bundle metadata without threading `bundle_name` /
  `version_data` through serialization, callbacks, and worker-side bundle init
  (a half-wired change that breaks run reproducibility);
- deactivates, reassigns, or mutates bundles it does not own, ignoring
  multi-processor / multi-team ownership;
- adds a bundle-related field to the serialized Dag without keeping
  `get_serialized_fields()` and the worker-side reader in sync.

## Evidence

- #66491 — records the bundle version with the `DagVersion`.
- #68583 — carries `bundle_name` into the serialized Dag the scheduler/worker read.
- #67217 — threads `version_data` to worker-side bundle init, completing the chain.
- #69185 — the same identity must also reach callbacks.
- #69964 — don't deactivate bundles owned by other dag-processors: writes must be scoped.
