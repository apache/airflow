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

# Scala Spark example bundle

A Scala + Apache Spark bundle for the Java SDK, exercised by the `java_sdk`
end-to-end test. It shows a non-Java JVM language driving Spark from a
`@task.stub` task and routing Log4j 2 logs into Airflow via `airflow-sdk-log4j2`.

The `scala_spark_example` Dag chains three tasks, each running in its own JVM
with a local `SparkSession` and passing scalar results over XCom:

- `spark_extract` - builds a DataFrame, pushes its row count.
- `spark_transform` - aggregates total revenue.
- `spark_load` - returns the persisted total.

## Build

```bash
# From java-sdk/: publish the SDK to the local Maven repository first.
./gradlew publishToMavenLocal -PskipSigning=true

cd scala_spark_example
../gradlew bundle
```

`fatJar` is disabled, so `build/bundle/` holds the bundle JAR plus every runtime
JAR (Spark included) — copy it into a Java coordinator's `jars_root`.
