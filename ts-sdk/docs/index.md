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

# Apache Airflow TypeScript SDK

The Apache Airflow TypeScript SDK provides the public TypeScript interfaces for
writing Apache Airflow task handlers and the coordinator runtime that executes
registered TypeScript handlers from an Airflow worker.

> **Note**
> This package is **1.0.0-beta1**: the API may change, it requires **Node 22+**, and
> it is **ESM-only**.

## Getting Started

Install the beta package from npm:

```bash
npm install apache-airflow-ts-sdk@1.0.0-beta1
```

Define a Dag and register its task handlers. Handlers receive a `TaskContext`
and a `TaskClient`; any non-`undefined` return value is pushed to XCom under
the `"return_value"` key by the active runtime, matching Python `@task`
behavior:

```ts
import { Dag, DagRegistry, serveDags, type TaskHandlerArgs } from "apache-airflow-ts-sdk";

export async function sayHello({ ctx, client }: TaskHandlerArgs) {
  const greeting = await client.getVariable("greeting");
  return { message: `Hello from ${ctx.taskId}: ${greeting}` };
}

const dag = new Dag("example_dag");
dag.task("say_hello", sayHello);

await serveDags(new DagRegistry(dag));
```

## Coordinators

Airflow runs TypeScript task bundles through the Python-side `NodeCoordinator`
(`airflow.sdk.coordinators.node.NodeCoordinator`). A Python Dag declares the
scheduling shape with stub tasks and owns the task dependencies between them,
and the TypeScript module registers handlers with matching task IDs. See the
[Non-Python Task SDKs guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/index.html)
for the conceptual overview of language SDKs.

## API Reference

The reference is generated directly from the TypeScript sources with
[TypeDoc](https://typedoc.org/). Use the sidebar or the search box to browse the
API.
