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

The Apache Airflow Java SDK — author and run Airflow task implementations in JVM languages.

## Language SDK compatibility matrix

Which Airflow TaskInstance states and capabilities the Java SDK currently supports. The normative
meaning of each dimension is defined in the
[Language SDK conformance specification](https://github.com/apache/airflow/blob/main/contributing-docs/30_new_language_sdk.rst).

<!-- BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX -->

*Min. Airflow version: 3.3 · supervisor schema: 2026-10-30*

| Dimension | Tier | Supported | Since | Notes |
|---|---|---|---|---|
| **TaskInstance states** |  |  |  |  |
| state: `success` | MUST | ✓ | 3.3 |  |
| state: `failed` | MUST | ✓ | 3.3 |  |
| state: `up_for_retry` | MUST | ✓ | 3.3 | RetryTask |
| state: `skipped` | SHOULD | ✗ | – | runtime does not emit TaskState skipped yet |
| state: `deferred` | MAY | ✗ | – | runtime does not emit DeferTask yet |
| state: `up_for_reschedule` | MAY | ✗ | – | runtime does not emit RescheduleTask yet |
| state: `awaiting_input` | MAY | ✗ | – | runtime does not emit AwaitInputTask yet |
| state: `removed` | MAY | ✓ | 3.3 |  |
| **Runtime capabilities** |  |  |  |  |
| capability: `mixed-lang-stub-target` | MUST | ✓ | 3.3 | @task.stub |
| capability: `task-logging` | MUST | ✓ | 3.3 | SLF4J + JPL bridged to the task log |
| capability: `xcom-read-write` | MUST | ✓ | 3.3 |  |
| capability: `connection-read` | MUST | ✓ | 3.3 |  |
| capability: `variable-read-write` | MUST | ✗ | – | getVariable only; no write over the comm socket yet |
| capability: `self-contained-bundle` | MUST | ✓ | 3.3 | Airflow metadata embedded in the jar artifact |
| capability: `retry-policy` | MAY | ✗ | – | no task-facing retry-policy API yet |
| capability: `task-state-store` | MAY | ✗ | – | no task-facing state-store API yet |
| capability: `asset-state-store` | MAY | ✗ | – | no task-facing state-store API yet |
| capability: `asset-event-emit` | MAY | ✗ | – | runtime does not emit asset events yet |
| capability: `asset-event-read` | MAY | ✗ | – | no task-facing asset-event API yet |
| **Native-Dag authoring** |  |  |  |  |
| capability: `native-dag-authoring` | SHOULD | ✗ | – | native Dag authoring not implemented yet |
| capability: `task-args` | MUST † | n/a | – |  |
| capability: `dag-params` | MUST † | n/a | – |  |
| capability: `taskflow-dependencies` | MUST † | n/a | – |  |
| capability: `branching` | SHOULD † | n/a | – |  |
| capability: `dag-test` | SHOULD † | n/a | – |  |
| capability: `task-group` | MAY † | n/a | – |  |
| capability: `dynamic-task-mapping` | MAY † | n/a | – |  |
| capability: `asset-inlets-outlets` | MAY † | n/a | – |  |
| capability: `asset-scheduling` | MAY † | n/a | – |  |
| capability: `object-store` | MAY † | n/a | – |  |

*Marks: ✓ supported · ✗ not supported · n/a not applicable. A tier marked † applies only when `native-dag-authoring` is supported.*

<!-- END AUTO-GENERATED LANG-SDK COMPAT MATRIX -->
