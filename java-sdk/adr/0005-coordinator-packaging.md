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

# ADR-0005: Coordinator Packaging, Module Layout, and Registration

## Status

Proposed

> **Note:** This ADR describes coordinator packaging as a standalone distribution separate
> from the Task SDK. The current plan is to ship coordinators as part of the Task SDK
> (`apache-airflow-task-sdk`); a separate distribution may be introduced later once the
> coordinator interface exits experimental status, but is not committed. This document is
> retained for reference if that split is revisited. Tracked operationally in
> [apache/airflow#66451](https://github.com/apache/airflow/issues/66451).

## Context

[ADR-0001](0001-java-sdk-airflow-integration.md) introduces a coordinator extension point.
Reviewers on PR #65958 raised three related but separable questions:

1. **PyPI package name.** Should the Java coordinator ship as
   `apache-airflow-providers-sdk-java` (consistent with every other provider) or under
   `airflow.sdk.coordinators` as part of the Task SDK (recognizing that "language
   coordinator" is a structurally new kind of distribution that does not behave like
   operators/hooks/sensors)?
2. **Source-tree module layout.** Should it live under `providers/sdk/java/` alongside other
   providers, or as a subpackage of the Task SDK?
3. **Discovery / registration mechanism.** Should coordinator classes be discovered through
   the existing `ProvidersManager`, or through some other mechanism?

A second concern, raised separately, is **runtime configuration**: a single `JavaCoordinator`
class is not enough to express "use JDK 11 for the legacy queue and JDK 17 for the modern
queue, with different `-Xmx` values." Class-only registration forces operators to subclass for
every variant or hardcode environment lookups, which the issue calls out explicitly:

> How can I use different JDK version? How can I use different JVM arguments? We hardcoded the
> subprocess cmd … so users have to subclass another Coordinator to override the Java config.
> — [apache/airflow#66451](https://github.com/apache/airflow/issues/66451)

## Decision

### A. Distribution: included in the Task SDK

The Java coordinator ships as part of the **Task SDK** (`apache-airflow-task-sdk`) and is
importable as `airflow.sdk.coordinators.java.JavaCoordinator`. This avoids extra packaging
infrastructure (separate release cadence, testing matrix, constraints files) while the
coordinator interface is still stabilising. New language coordinators (`go`, `typescript`, …)
follow the same model.

A coordinator distribution exposes:

- A `BaseCoordinator` subclass under `airflow.sdk.coordinators.<lang>`.
- No operators, hooks, sensors, triggers, or `provider.yaml`.

### B. Module layout: namespace package under `airflow.sdk.coordinators`

Each coordinator contributes a subpackage to the **namespace package** `airflow.sdk.coordinators`.
The Task SDK owns the namespace; individual language coordinators add
`airflow.sdk.coordinators.<lang>`.

The Java coordinator therefore resolves as:

```python
from airflow.utils.module_loading import import_string

JavaCoordinator = import_string("airflow.sdk.coordinators.java.JavaCoordinator")
```

Both Airflow Core (DAG processor) and the Task SDK (task runner) import coordinators by this
path. The namespace package layout means the physical distribution can change in the future
without altering import paths or user configuration.

### C. Discovery via `[sdk] coordinators` (Airflow configuration)

Coordinators are **not** discovered through `ProvidersManager` /
`ProvidersManagerTaskRuntime`, and there is no `coordinators` key in `provider.yaml`. They are
registered as named instances in `airflow.cfg`:

```ini
[sdk]
coordinators = {
    "jdk-11": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
            "java_executable": "/usr/lib/jvm/java-11-openjdk-amd64/bin/java",
            "jvm_args": ["-Xmx512m"],
            "jars_root": ["/files/legacy/lib"]
        }
    },
    "jdk-17": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
            "java_executable": "/usr/lib/jvm/java-17-openjdk-amd64/bin/java",
            "jvm_args": ["-Xmx1024m", "-Xms256m"],
            "jars_root": ["/files/new/lib"]
        }
    }
}

queue_to_coordinator = {"legacy-java-queue": "jdk-11", "modern-java-queue": "jdk-17"}
```

`[sdk] coordinators` is a JSON object: the key is the coordinator's name (used as the routing
target in `[sdk] queue_to_coordinator`), and the value supplies `classpath` and free-form
`kwargs` passed to the constructor. Two entries with the same `classpath` (e.g., both
`JavaCoordinator`) but different keys and `kwargs` are independent instances — this is how
JDK 11 and JDK 17 tasks run on the same worker without subclassing.

### Why not `provider.yaml` / `ProvidersManager`?

Coordinators are not providers in the Airflow sense:

- They expose no operators / hooks / sensors / triggers.
- They are consumed by both Airflow Core (in the DAG processor) **and** the Task SDK (in the
  task runner). The provider system is not designed to be loaded from inside a worker subprocess
  that intentionally has no Airflow-Core import.
- They need **per-instance** runtime configuration (interpreter path, JVM flags, …).
  `provider.yaml` registers classes, not instances, and bolting kwargs onto provider entries
  would distort the provider data model.
- A coordinator is the only thing in this distribution; there is no benefit to sharing the
  provider's discoverability surface (`airflow providers list`, etc.). On the contrary, listing
  `apache-airflow-providers-sdk-java` next to AWS/GCP providers is misleading for users.

Putting the registry in `airflow.cfg` keeps the data model honest (instances, with their kwargs)
and makes the per-host opt-in (install + config-edit) explicit rather than implicit
(install-implies-active).

## Consequences

- The Java coordinator ships **inside the Task SDK**; the namespace package layout ensures that
  if packaging arrangements change in the future, no changes to user configuration or Airflow
  Core are required.
- **`airflow.sdk.coordinators`** is a namespace package owned by the Task SDK; language
  coordinator modules contribute subpackages to it. Multiple coordinator modules can be
  installed side by side without colliding.
- **`[sdk] coordinators`** carries instance-level configuration; **`[sdk] queue_to_coordinator`**
  carries queue → instance routing.
- Multiple instances of the same coordinator class (e.g., `jdk-11` and `jdk-17`) can be
  registered with different `kwargs` and bound to different queues — solving the multi-JDK and
  JVM-flag use cases raised in
  [apache/airflow#66451](https://github.com/apache/airflow/issues/66451) without subclassing.
- The provider registry no longer shows coordinators, removing the "Java appears, Go does not"
  asymmetry that earlier drafts of this ADR flagged as a transitional UX wart.
