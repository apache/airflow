# Module sdk

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

Write Apache Airflow Dags and tasks in Java, Kotlin, Scala or any other JVM language.

It requires Java 11 or later.

## Quick start

Apply the Gradle plugin, and add the SDK and its annotation processor:

```groovy
plugins {
    id("org.apache.airflow.sdk") version "<version>"
}

dependencies {
    annotationProcessor("org.apache.airflow:airflow-sdk-processor:<version>")
    implementation("org.apache.airflow:airflow-sdk:<version>")
}

airflowBundle {
    mainClass = "com.mycompany.airflow.hello.Main"
}
```

Replace `<version>` with a release from [Maven Central](https://central.sonatype.com/artifact/org.apache.airflow/airflow-sdk).

Declare a Dag with `@Airflow.Dag`, its tasks with `@Airflow.Task`, and its graph in an `@Airflow.Deps` class.
Passing the value one task returns into another is how the tasks depend on each other:

<!-- BEGIN SHARED SNIPPET hello-dag -->

```java
package com.mycompany.airflow.hello;

import org.apache.airflow.sdk.*;

@Airflow.Dag(id = "java_hello", schedule = "@daily", queue = "java")
public class HelloDag {

  @Airflow.Task
  public long extract() {
    return 42L;
  }

  @Airflow.Task
  public void report(long rows) {
    System.out.println("extracted " + rows + " rows");
  }

  @Airflow.Deps
  static class Wiring implements HelloDagDeps {
    void depends() {
      report(extract());
    }
  }
}
```

<!-- END SHARED SNIPPET hello-dag -->

Serve it from the main class:

<!-- BEGIN SHARED SNIPPET hello-main -->

```java
package com.mycompany.airflow.hello;

import org.apache.airflow.sdk.Bundle;
import org.apache.airflow.sdk.Server;

public class Main {
  public static void main(String[] args) {
    Server.create(args).serve(new Bundle().register(HelloDag.class));
  }
}
```

<!-- END SHARED SNIPPET hello-main -->

Build the bundle with `./gradlew bundle`, and see the [Java SDK guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html) for deploying it, and for everything
a Dag declared in Java can do.

## Language SDK compatibility matrix

Which Airflow TaskInstance states and capabilities the Java SDK currently supports. The normative
meaning of each dimension is defined in the
[Language SDK conformance specification](https://github.com/apache/airflow/blob/main/contributing-docs/30_new_language_sdk.rst).

<!-- BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

*Min. Airflow version: 3.4.0 · supervisor schema: 2026-10-30*

| Dimension | Tier | Supported | Since (Airflow) | Since (SDK) | Notes |
|---|---|---|---|---|---|
| **TaskInstance states** |  |  |  |  |  |
| state: `success` | MUST | ✓ | 3.4.0 | 1.0.0 |  |
| state: `failed` | MUST | ✓ | 3.4.0 | 1.0.0 |  |
| state: `up_for_retry` | MUST | ✓ | 3.4.0 | 1.0.0 | RetryTask |
| state: `skipped` | SHOULD | ✓ | 3.4.0 | 1.0.0 | a TriggerDagRun task that skips an existing run |
| state: `deferred` | MAY | ✓ | 3.4.0 | 1.0.0 | a TriggerDagRun task that waits with deferrable |
| state: `up_for_reschedule` | MAY | ✗ | – | – | runtime does not emit RescheduleTask yet |
| state: `awaiting_input` | MAY | ✗ | – | – | runtime does not emit AwaitInputTask yet |
| state: `removed` | MAY | ✓ | 3.4.0 | 1.0.0 |  |
| **Task runtime** |  |  |  |  |  |
| capability: `task-logging` | MUST | ✓ | 3.4.0 | 1.0.0 | SLF4J + JPL bridged to the task log |
| capability: `xcom-read-write` | MUST | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `connection-read` | MUST | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `variable-read-write` | MUST | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `self-contained-bundle` | MUST | ✓ | 3.4.0 | 1.0.0 | Airflow metadata embedded in the jar artifact |
| capability: `retry-policy` | MAY | ✗ | – | – | no task-facing retry-policy API yet |
| capability: `task-state-store` | MAY | ✓ | 3.4.0 | 1.0.0 | Client.getTaskStateStore() get/set/delete/clear |
| capability: `asset-state-store` | MAY | ✗ | – | – | no task-facing asset state-store API yet |
| capability: `asset-event-emit` | MAY | ✗ | – | – | runtime does not emit asset events yet |
| capability: `asset-event-read` | MAY | ✗ | – | – | no task-facing asset-event API yet |
| capability: `object-store` | MAY | ✗ | – | – | no object-storage API yet |
| capability: `listeners` | MAY | ✗ | – | – | no hook into the Python listener API, so a task emits no OpenLineage facets of its own |
| **Dag definition** |  |  |  |  |  |
| capability: `dag-definition-authoring` | SHOULD | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `dag-args` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `task-args` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `dag-params` | MUST † | ✗ | – | – |  |
| capability: `dag-callbacks` | MAY † | ✗ | – | – |  |
| capability: `explicit-dependencies` | MUST † | ✓ | 3.4.0 | 1.0.0 | before / after, or @Airflow.Deps |
| capability: `edge-labels` | MAY † | ✓ | 3.4.0 | 1.0.0 | Label.of(text).on(task) |
| capability: `taskflow-dependencies` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `task-group` | MAY † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `task-loops` | MAY † | ✗ | – | – |  |
| capability: `if-else-branching` | SHOULD † | ✓ | 3.4.0 | 1.0.0 | DagDef.If or @Airflow.If |
| capability: `switch-case-branching` | SHOULD † | ✓ | 3.4.0 | 1.0.0 | DagDef.Switch or @Airflow.Switch |
| capability: `trigger-dag-run` | MAY † | ✓ | 3.4.0 | 1.0.0 | TriggerDagRun |
| capability: `dynamic-dag-generation` | MAY † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `human-in-the-loop` | MAY † | ✗ | – | – |  |
| capability: `dag-test` | SHOULD † | ✗ | – | – |  |
| capability: `asset-definition` | MAY † | ✗ | – | – |  |
| capability: `asset-scheduling` | MAY † | ✗ | – | – |  |
| capability: `mapped-tasks` | MAY † | ✗ | – | – |  |
| capability: `asset-inlets-outlets` | MAY † | ✗ | – | – |  |
| capability: `task-callbacks` | MAY † | ✗ | – | – | no SDK-defined task callbacks |
| **Python Dag with stub TaskHandler** |  |  |  |  |  |
| capability: `mixed-lang-stub-target` | MUST | ✓ | 3.4.0 | 1.0.0 | @task.stub |
| capability: `taskflow-binding` | MUST | ✓ | 3.4.0 | 1.0.0 | mapped stubs excluded |
| capability: `mapped-tasks` | MAY | ✗ | – | – | Airflow does not bind arguments to mapped stubs yet |
| capability: `asset-inlets-outlets` | MAY | ✗ | – | – | runtime does not report task outlets yet |
| capability: `task-callbacks` | MAY | ✗ | – | – | a Python-declared task callback does not run for a stub task |

*Marks: ✓ supported · ✗ not supported · n/a not applicable. A tier marked † applies only when `dag-definition-authoring` is supported.*

<!-- END AUTO-GENERATED LANG-SDK COMPAT MATRIX -->
