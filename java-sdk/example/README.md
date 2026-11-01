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

# Java SDK examples

Example Dags for the Apache Airflow Java SDK. See the [Java SDK guide][guide] for the full documentation.

[guide]: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/java.html

## Build

Publish the SDK to the local Maven repository, then build the example bundle into `build/bundle/`:

```bash
cd java-sdk
./gradlew publishToMavenLocal -PskipSigning=true

cd example
../gradlew bundle
```

## Configure Airflow

Register `build/bundle/` as a Dag bundle, and route the `java` queue to the Java coordinator:

```bash
export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
  {"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}},
  {
    "name": "java-task-handlers",
    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
    "kwargs": {"path": "/absolute/path/to/airflow/java-sdk/example/build/bundle"}
  }
]'
export AIRFLOW__SDK__COORDINATORS='{
  "java": {
    "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
    "kwargs": {"task_handler_bundle_name": "java-task-handlers"}
  }
}'
export AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{"java": "java"}'
```

The workers and the Dag processor need Java 11 or later on `PATH`.

## Dag definition

These Dags are declared entirely in Java, in `src/java/org/apache/airflow/example/nativedag/`:

| Dag                              | Source                   | Shows                                                                                                  |
| -------------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------ |
| `java_native_annotation_example` | `AnnotationExample.java` | The annotation style: task inputs, a task group, `@Airflow.If`, `@Airflow.Switch`, and `TriggerDagRun` |
| `java_native_interface_example`  | `InterfaceExample.java`  | A similar Dag in the interface style, with `DagDef`                                                    |
| `java_native_target_example`     | `TargetExample.java`     | The Dag the other two trigger                                                                          |

```bash
airflow dags trigger java_native_annotation_example
airflow dags trigger java_native_interface_example
```

## Python Dag with stub TaskHandler

These Dags are declared in Python, in `src/resources/dags/java_examples.py`, and their stub tasks are
implemented in Java:

| Dag                         | Source                         | Shows                                                                  |
| --------------------------- | ------------------------------ | ---------------------------------------------------------------------- |
| `java_annotation_example`   | `AnnotationExample.java`       | `@Airflow.TaskHandler` methods, with arguments by position and by name |
| `java_interface_example`    | `InterfaceExampleBuilder.java` | `Task` and `InputTask` classes registered by Dag and task id           |
| `java_xcom_casting_example` | `XComCastingExample.java`      | How values passed between tasks convert to the declared Java types     |

Copy `src/resources/dags/java_examples.py` into your Airflow Dags folder, and create the Connection and
Variable the examples read:

```bash
export AIRFLOW_CONN_TEST_HTTP='{
    "conn_type": "http",
    "login": "user",
    "password": "pass",
    "host": "example.com",
    "port": 1234,
    "extra": {"param1": "val1", "param2": "val2"}
}'
export AIRFLOW_VAR_MY_VARIABLE=123
```

```bash
airflow dags trigger java_annotation_example
airflow dags trigger java_interface_example
airflow dags trigger java_xcom_casting_example
```
