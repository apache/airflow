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

Write Apache Airflow Dags and tasks in TypeScript. Declare a whole Dag in TypeScript, or implement
mixed-language tasks: TypeScript bodies for the stub tasks of a Python Dag.

> **Note**
> This package is **0.1.0-beta1**: the API may change, it requires **Node 22+**, and
> it is **ESM-only**.

## Getting Started

Install the beta package from npm, and `esbuild` to build the bundle:

```bash
npm install apache-airflow-ts-sdk@0.1.0-beta1
npm install --save-dev esbuild
```

Declare a Dag with `Dag`, and its tasks with `dag.task`. Calling a task names its inputs, which is how the
tasks depend on each other:

```ts
import { Bundle, Dag } from "apache-airflow-ts-sdk";

const dag = new Dag("ts_etl", { schedule: "@daily", queue: "typescript" });

const extract = dag.task("extract", async (): Promise<number> => 42);
const load = dag.task("load", async ({ rows }: { rows: number }) => {
  console.log(`loading ${rows} rows`);
});

load({ rows: extract() });

await new Bundle(dag).serve();
```

For a mixed-language task, bind a function to the stub task's `dag_id` and `task_id` with `TaskHandler`:

```ts
import { Bundle, getClient, getContext, TaskHandler } from "apache-airflow-ts-sdk";

export async function sayHello() {
  const greeting = await getClient().getVariable("greeting");
  return { message: `Hello from ${getContext().taskId}: ${greeting}` };
}

await new Bundle(new TaskHandler("example_dag", "say_hello", sayHello)).serve();
```

In both cases a handler is a plain function. `getContext()` returns the `TaskContext` and `getClient()` the
`TaskClient` while it runs, and a value it returns becomes the task's `return_value` XCom.

See the
[TypeScript SDK guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html)
for building and deploying a bundle, and for everything a Dag declared in TypeScript can do.

## API Reference

The reference is generated directly from the TypeScript sources with
[TypeDoc](https://typedoc.org/). Use the sidebar or the search box to browse the
API.
