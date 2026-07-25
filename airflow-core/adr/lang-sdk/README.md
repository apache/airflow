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

# Architectural Decision Records — Language SDKs and coordinators

These ADRs record the cross-cutting architecture decisions behind running non-Python tasks in
Airflow ([AIP-108](https://cwiki.apache.org/confluence/x/pY4mGQ)): the coordinator layer, workload
execution, packaging, and how Lang SDKs touch Airflow core surfaces (Dag parsing, `DagCode`, the
REST API, and the UI). They originated in `java-sdk/adr/` and were moved here following the review
discussion on [apache/airflow#70059](https://github.com/apache/airflow/pull/70059), because they
bind core interfaces and apply to every language SDK, not just the Java SDK.

- [ADR-0001](0001-java-sdk-airflow-integration.md): Java SDK / Airflow integration and the coordinator extension point.
- [ADR-0002](0002-workload-execution.md): workload execution through coordinators.
- [ADR-0003](0003-pure-java-dags.md): pure Java Dags — build-time packaging and code visibility.
- [ADR-0004](0004-dag-parsing.md): language-specific Dag file processing.
- [ADR-0005](0005-coordinator-packaging.md): coordinator packaging, module layout, and registration.
- [ADR-0006](0006-no-lang-sdk-source-display.md): no Lang-SDK source display for mixed-language (`@task.stub`) Dags.

Decisions specific to a single SDK stay next to that SDK — for example, the Go SDK's bundle-format
decisions live in [`go-sdk/adr/`](../../../go-sdk/adr).
