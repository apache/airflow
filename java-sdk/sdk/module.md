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
| capability: `asset-state-store` | MAY | ✗ | – | – | no task-facing state-store API yet |
| capability: `asset-event-emit` | MAY | ✗ | – | – | runtime does not emit asset events yet |
| capability: `asset-event-read` | MAY | ✗ | – | – | no task-facing asset-event API yet |
| capability: `object-store` | MAY | ✗ | – | – | no object-storage API yet |
| **Dag definition** |  |  |  |  |  |
| capability: `native-dag-authoring` | SHOULD | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `dag-args` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `task-args` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `dag-params` | MUST † | ✗ | – | – |  |
| capability: `explicit-dependencies` | MUST † | ✓ | 3.4.0 | 1.0.0 | before / after, or @Airflow.Deps |
| capability: `taskflow-dependencies` | MUST † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `task-group` | MAY † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `if-else-branching` | SHOULD † | ✓ | 3.4.0 | 1.0.0 | DagDef.If or @Airflow.If |
| capability: `switch-case-branching` | SHOULD † | ✓ | 3.4.0 | 1.0.0 | DagDef.Switch or @Airflow.Switch |
| capability: `trigger-dag-run` | MAY † | ✓ | 3.4.0 | 1.0.0 | TriggerDagRun |
| capability: `dynamic-dag-generation` | MAY † | ✓ | 3.4.0 | 1.0.0 |  |
| capability: `human-in-the-loop` | MAY † | ✗ | – | – |  |
| capability: `dag-test` | SHOULD † | ✗ | – | – |  |
| capability: `asset-scheduling` | MAY † | ✗ | – | – |  |
| capability: `dynamic-task-mapping` | MAY † | ✗ | – | – |  |
| capability: `asset-inlets-outlets` | MAY † | ✗ | – | – |  |
| **Python Dag with stub TaskHandler** |  |  |  |  |  |
| capability: `mixed-lang-stub-target` | MUST | ✓ | 3.4.0 | 1.0.0 | @task.stub |
| capability: `taskflow-binding` | MUST | ✓ | 3.4.0 | 1.0.0 | mapped stubs excluded |
| capability: `dynamic-task-mapping` | MAY | ✗ | – | – | Airflow does not bind arguments to mapped stubs yet |
| capability: `asset-inlets-outlets` | MAY | ✗ | – | – | runtime does not report task outlets yet |

*Marks: ✓ supported · ✗ not supported · n/a not applicable. A tier marked † applies only when `native-dag-authoring` is supported.*

<!-- END AUTO-GENERATED LANG-SDK COMPAT MATRIX -->
