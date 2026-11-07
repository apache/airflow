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

# Airflow Java SDK

Write Apache Airflow Dags and tasks in Java, Kotlin, Scala or any other JVM language.

**Status:** Beta · Java 11+

## Installation

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

Replace `<version>` with a release from [Maven Central](https://central.sonatype.com/artifact/org.apache.airflow/airflow-sdk). See the [guide][building-a-bundle] for Maven.

## Quick start

Declare a Dag:

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

Build the bundle into `build/bundle/`:

```bash
./gradlew bundle
```

Copy `build/bundle/` into a Dag bundle, such as the default `dags-folder` bundle, and send the `java` queue to
the Java coordinator in `airflow.cfg`:

```ini
[sdk]
coordinators = {"java": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator"}}
queue_to_coordinator = {"java": "java"}
```

Airflow reads the bundle like any other Dag file: `java_hello` shows up in the UI, runs on its schedule, and
`report` receives the value `extract` returned.

## Learn more

The [Java SDK guide][guide] covers:

| Topic                                                                                         | Guide section                                                                      |
| --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| Task inputs, options, order-only dependencies, task groups, branching, triggering another Dag | [Declaring a Dag in Java][declaring-a-dag-in-java]                                 |
| `@Airflow.TaskHandler` for the `@task.stub` tasks of a Python Dag, and how arguments bind     | [Implementing stub tasks of a Python Dag][implementing-stub-tasks-of-a-python-dag] |
| `Client`, `Context`, and what a task returns                                                  | [Writing tasks][writing-tasks]                                                     |
| Building a bundle with Gradle or Maven                                                        | [Building a bundle][building-a-bundle]                                             |
| Where to deploy a bundle, and the `JavaCoordinator` options                                   | [Deploying][deploying]                                                             |
| What a Dag declared in Java cannot do yet                                                     | [Limitations][limitations]                                                         |

See [`example/`](example/) for a working project.

[guide]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html
[declaring-a-dag-in-java]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html#declaring-a-dag-in-java
[implementing-stub-tasks-of-a-python-dag]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html#implementing-stub-tasks-of-a-python-dag
[writing-tasks]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html#writing-tasks
[building-a-bundle]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html#building-a-bundle
[deploying]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html#deploying
[limitations]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html#limitations

## Compatibility matrix

Which Airflow TaskInstance states and capabilities this SDK supports. This table is generated from
[`capabilities.yaml`](capabilities.yaml); the conformance dimensions are defined in the
[Language SDK conformance spec](https://github.com/apache/airflow/blob/main/contributing-docs/30_new_language_sdk.rst).
Do not edit the table by hand — edit `capabilities.yaml` and let the `update-java-sdk-readme-matrix`
prek hook regenerate it.

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
| capability: `asset-state-store` | MAY | ✓ | 3.4.0 | 1.0.0 | Client.getAssetStateStore(), by asset name or URI |
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

## Links

- [Java SDK guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html)
- [API reference](https://airflow.apache.org/docs/java-sdk/stable/) (generated from the sources)
- [Maven Central](https://central.sonatype.com/artifact/org.apache.airflow/airflow-sdk)
- [Source](https://github.com/apache/airflow/tree/main/java-sdk): the `java-sdk/` directory of the Apache Airflow monorepo
- [Issues](https://github.com/apache/airflow/issues): bug reports and feature requests
- [Developing this SDK](DEVELOPMENT.md) (building, testing, and the release process)
