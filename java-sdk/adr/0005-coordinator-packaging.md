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

Accepted — coordinators are a new distribution type, **not** Airflow providers, and are activated through Airflow configuration rather than `provider.yaml`. Tracked operationally in [apache/airflow#66451](https://github.com/apache/airflow/issues/66451).

## Context

[ADR-0001](0001-java-sdk-airflow-integration.md) introduces a coordinator extension point. Reviewers on PR #65958 raised three related but separable questions:

1. **PyPI package name.** Should the Java coordinator ship as `apache-airflow-providers-sdk-java` (consistent with every other provider) or as `apache-airflow-coordinators-java` (recognizing that "language coordinator" is a structurally new kind of distribution that does not behave like operators/hooks/sensors)?
2. **Source-tree module layout.** Should it live under `providers/sdk/java/` alongside other providers, or as a new top-level peer to `providers/`, `airflow-core/`, and `task-sdk/`?
3. **Discovery / registration mechanism.** Should coordinator classes be discovered through the existing `ProvidersManager` (and its task-runtime equivalent `ProvidersManagerTaskRuntime`), or through some other mechanism?

A second concern, raised separately, is **runtime configuration**: a single `JavaCoordinator` class is not enough to express "use JDK 11 for the legacy queue and JDK 17 for the modern queue, with different `-Xmx` values." Class-only registration forces operators to subclass for every variant or hardcode environment lookups, which the issue calls out explicitly:

> How can I use different JDK version? How can I use different JVM arguments? We hardcoded the subprocess cmd … so users have to subclass another Coordinator to override the Java config.
> — [apache/airflow#66451](https://github.com/apache/airflow/issues/66451)

The existing `[sdk] queue_to_sdk` config (introduced in [ADR-0001](0001-java-sdk-airflow-integration.md)) maps a queue to a *language*, not to a *runtime variant*, and is therefore insufficient for this need.

## Decision

### A. Distribution name: `apache-airflow-coordinators-<lang>`

Coordinators are not Airflow providers; they are a separate distribution type. The Java coordinator ships as **`apache-airflow-coordinators-java`**. New language coordinators follow the same pattern (`apache-airflow-coordinators-go`, `apache-airflow-coordinators-rust`, …).

A coordinator distribution exposes:

- A `BaseCoordinator` subclass under `airflow.sdk.coordinators.<lang>`.
- No operators, hooks, sensors, triggers, or `provider.yaml`.

### B. Module layout: namespace package under `airflow.sdk.coordinators`

Each coordinator distribution contributes a subpackage to the **namespace package** `airflow.sdk.coordinators`. The Task SDK owns the namespace; concrete coordinator distributions add `airflow.sdk.coordinators.<lang>`.

The Java coordinator therefore resolves as:

```python
from airflow.utils.module_loading import import_string

JavaCoordinator = import_string("airflow.sdk.coordinators.java.JavaCoordinator")
```

Both Airflow Core (DAG processor) and the Task SDK (task runner) import coordinators by this path. As long as `apache-airflow-coordinators-java` is installed on a host, that `import_string` call resolves correctly without any registry lookup.

### C. Discovery via `[sdk] coordinators` (Airflow configuration)

Coordinators are **not** discovered through `ProvidersManager` / `ProvidersManagerTaskRuntime`, and there is no `coordinators` key in `provider.yaml`. They are registered as instance entries in `airflow.cfg`:

```ini
[sdk]
coordinators = [
    {
        "name": "jdk-11",
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
            "java_executable": "/usr/lib/jvm/java-11-openjdk-amd64/bin/java",
            "jvm_args": ["-Xmx512m"],
            "jdk_home": "/usr/lib/jvm/java-11-openjdk-amd64"
        }
    },
    {
        "name": "jdk-17",
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
            "java_executable": "/usr/lib/jvm/java-17-openjdk-amd64/bin/java",
            "jvm_args": ["-Xmx1024m", "-Xms256m"],
            "jdk_home": "/usr/lib/jvm/java-17-openjdk-amd64"
        }
    }
]

queue_to_coordinator = {"legacy-java-queue": "jdk-11", "modern-java-queue": "jdk-17"}
```

The shape is intentionally similar to `AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST`: a list of self-describing entries with `name`, `classpath`, and free-form `kwargs`.

**Renames vs ADR-0001's earlier draft:**

| Old (`[sdk] queue_to_sdk`) | New (`[sdk] queue_to_coordinator`) |
|---|---|
| Maps queue → language tag (e.g., `"java"`) | Maps queue → coordinator instance name (e.g., `"jdk-17"`) |
| One coordinator per language | Many coordinator instances per language, distinguished by `kwargs` |

`queue_to_coordinator` replaces `queue_to_sdk` everywhere.

### Why not `provider.yaml` / `ProvidersManager`?

Coordinators are not providers in the Airflow sense:

- They expose no operators / hooks / sensors / triggers.
- They are consumed by both Airflow Core (in the DAG processor) **and** the Task SDK (in the task runner). The provider system is not designed to be loaded from inside a worker subprocess that intentionally has no Airflow-Core import.
- They need **per-instance** runtime configuration (interpreter path, JVM flags, …). `provider.yaml` registers classes, not instances, and bolting kwargs onto provider entries would distort the provider data model.
- A coordinator is the only thing in this distribution; there is no benefit to sharing the provider's discoverability surface (registry listings, `airflow providers list`, etc.). On the contrary, listing `apache-airflow-providers-sdk-java` next to AWS/GCP providers is misleading for users.

Putting the registry in `airflow.cfg` keeps the data model honest (instances, with their kwargs) and makes the per-host opt-in (install + config-edit) explicit rather than implicit (install-implies-active).

## Consequences

- **`apache-airflow-coordinators-java`** ships as a new distribution type with its own release docs and constraints handling, distinct from providers.
- **`airflow.sdk.coordinators`** is a namespace package owned by the Task SDK; concrete coordinator distributions contribute subpackages to it. Multiple coordinator distributions can be installed side by side without colliding.
- **`[sdk] coordinators`** carries instance-level configuration; **`[sdk] queue_to_coordinator`** carries queue → instance routing. `[sdk] queue_to_sdk` is removed.
- Operators can register multiple instances of the same coordinator class (e.g., `jdk-11` and `jdk-17`) and bind different queues to them — solving the multi-JDK and JVM-flag use cases raised in [apache/airflow#66451](https://github.com/apache/airflow/issues/66451) without subclassing.
- The provider registry no longer shows coordinators, removing the "Java appears, Go does not" asymmetry that earlier drafts of this ADR flagged as a transitional UX wart.
- Future static-source DAG parsers (e.g., YAML / `dag-factory`) that fit the same coordinator shape can use the same `[sdk] coordinators` registry without inventing a new extension point.
