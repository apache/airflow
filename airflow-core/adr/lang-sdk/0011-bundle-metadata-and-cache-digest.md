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

# ADR-0011: Bundle Metadata — Retiring the Build-Time Inventory, Converging on a Cache Digest

## Status

Proposed

## Context

[ADR-0010](0010-persisted-task-handler-bindings.md) resolves a stub task to its artifact during Dag
processing and persists the result, so nothing searches for an artifact at execution time. Two
consequences land on the artifact format: the build-time Dag inventory loses its only purpose, and
the Dag processor gains a new need — a stable value it can compare cheaply to decide whether
re-validation is required.

Today that artifact carries a build-time inventory of the Dag and task ids it exposes. This is what
`airflow-go-pack` emits, and what the published schema requires
([`$id`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/airflow-metadata.schema.json#L3),
[`required`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/docs/airflow-metadata.schema.json#L7)):

```yaml
airflow_bundle_metadata_version: "1.0"
sdk:
  language: "go"
  version: "0.1.0"
  supervisor_schema_version: "2026-06-16"
source: "main.go"
dags:                       # <-- frozen when the artifact was built
  etl:
    tasks:
      - "extract"
      - "transform"
  reporting:
    tasks:
      - "publish"
```

## Decision

### The artifact carries no Dag or task identifiers

After this change an artifact contains exactly three things:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  compiled artifact     the executable, JAR, or bundled code             │
│  entrypoint source     the authored source, verbatim, for display       │
│  metadata              only what is needed to launch and to trust       │
└─────────────────────────────────────────────────────────────────────────┘
```

and the metadata region is reduced to this:

```yaml
airflow_bundle_metadata_version: "1.0"
sdk:
  language: "go"                          # this is an Airflow Lang-SDK artifact
  version: "0.1.0"
  supervisor_schema_version: "2026-06-16" # how to speak to it
source: "main.go"                         # display name only
digests:
  integrity: "<sha256 of the executable region>"
  cache: "<sha256 of all logical content>"
# no dags:
# no task_handlers:
```

No `dag_id` appears anywhere in it, and no `task_id`.

Candidate *detection* is unaffected and still requires executing nothing — it is exactly the "this is
an Airflow Lang-SDK artifact" marker doing its job: the `AFBNDL01` trailer magic for Go
([`Magic`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/go-sdk/internal/bundlefooter/footer.go#L57)), the `.min.mjs` suffix plus a valid layout header for
TypeScript ([`BUNDLE_SUFFIX`](https://github.com/apache/airflow/blob/79991cd4db0c9346a28b23c453377f6df0c6b4ed/task-sdk/src/airflow/sdk/coordinators/node/coordinator.py#L43)), a `Main-Class` attribute
for Java.

The entrypoint source region stays, for display.

### A cache digest, distinct from the integrity hash

The Dag processor must answer "has this artifact changed since I validated it" tens of times a
minute. That is a different question from "is this artifact intact", and the two must not be
conflated:

| | integrity hash | cache digest |
|---|---|---|
| answers | is this artifact intact? | has this artifact changed? |
| checked | at task execution, per launch | during Dag processing, per parse |
| how | **computed** and compared | **read** |
| must cover | the executable region, at minimum | all logical content |

Reading a stored value cannot substitute for computing one — a truncated or half-downloaded artifact
still reports a plausible stored digest. Every existing integrity check stays exactly where it is and
keeps computing.

**The digest is opaque and coordinator-defined.** It is not "SHA-256 of the file". Each runtime
supplies a value that is stable across rebuilds changing nothing and differs across rebuilds changing
something; consumers compare for equality and interpret nothing.


## Consequences

- Dynamic Dag rendering works.
- The canonical schema drops the identifier mapping entirely, the coordinator task execution side will rely on persisted rel_path instead of discovering the artifact every time.
- The packer no longer needs to execute the artifact at all. `supervisor_schema_version` is a compile-time constant of the SDK.


