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
independently-versioned sources of Dag code (`dag_processing/bundles/`). This
exists to make runs reproducible across distributed components. The DFP,
scheduler, and workers no longer all assume one mutable directory on a shared
filesystem; instead a Dag is produced from a specific bundle at a specific
version, and that identity has to travel with it.

Concretely, when the DFP parses a file it records **which bundle and which
version** produced the serialized Dag (`bundle_name`, `version_data` on the
`DagVersion`). Downstream that identity is threaded onward: into callbacks, and
into the worker so the worker initializes the *same* bundle version before it
runs the task. If any link in that chain drops the version, a worker can execute
a task against a different revision of the code than the one that was scheduled —
a silent, hard-to-diagnose correctness bug that ordinary tests rarely reproduce.

Bundle ownership is also shared state: in multi-processor and multi-team
deployments more than one dag-processor writes to the bundles table, so a
processor must not treat the table as exclusively its own.

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

- #66491 — "Add BundleVersion dataclass and version_data persistence to
  DagVersion": establishes recording the bundle version with the Dag version.
- #68583 — "Add bundle_name to serialized dag": carries bundle identity into the
  serialized representation the scheduler/worker read.
- #67217 — "Thread version_data through BundleInfo to worker-side bundle
  initialization": completes the chain so the worker initializes the same
  version.
- #69185 — "Thread version_data to callbacks": the same identity must reach
  callbacks, not just the worker task path.
- #69964 — "Don't deactivate DAG bundles owned by other dag-processors": bundle
  ownership is shared state and writes must be scoped.
