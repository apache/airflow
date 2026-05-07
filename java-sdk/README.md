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

A **JVM** SDK for Apache Airflow. You can use any JVM-compatible language to write
workflow bundles, and have Airflow consume the result.

The SDK and execution-time logic is implemented in Kotlin.
An example is bundled showing how the SDK can be used in Java.

## Building

```bash
./gradlew build
```

## Technical Details

The Java program is launched as a subprocess by the Airflow worker and communicates
through TCP sockets. The Java program accepts flags `--comm` and `--logs` from the
command line.

The Java program "parses" DAGs on launch, and then connects to the specified TCP servers.
The rest is similar to the standard Airflow:

* DAG-parsing:
  1. On connection, the parent immediately sends a DagParsingRequest through the socket.
  2. The Java program sends back a DagParsingResult to the parent.
  3. The Java program exits.
* Execution:
  1. On connection, the parent immediately sends a StartupDetails through the socket.
  2. The Java program uses the information to find the relevant task to execute.
  3. The task is run.
  4. The Java program tells the parent to update the task's terminal state.
  5. The Java program exits.

Communication uses the same formats as the Python-based processes.

## Serialization Validation

Workflow:

```bash
# 1. Generate Java output (runs as part of normal test suite)
# More specifically, the test `sdk/src/test/kotlin/org/apache/airflow/sdk/execution/SerializationCompatibilityTest.kt` generates the output file `validation/serialization/serialized_java.json`.
./gradlew sdk:test

# 2. Generate Python output (requires Airflow env)
uv run validation/serialization/serialize_python.py \
    validation/serialization/test_dags.yaml \
    validation/serialization/serialized_python.json

# 3. Compare
uv run validation/serialization/compare.py \
    validation/serialization/serialized_python.json \
    validation/serialization/serialized_java.json
```
