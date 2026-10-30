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

Write Apache Airflow Dags in TypeScript, and run them on Node.js.

> **Note**
> This package is **1.0.0-beta1**: the API may change, it requires **Node 22+**, and
> it is **ESM-only**.

## Quick Start

Install the beta package from npm, and `esbuild` to build the bundle:

```bash
npm install apache-airflow-ts-sdk@1.0.0-beta1
npm install --save-dev esbuild
```

Declare a Dag in `src/main.ts`. Calling a task names its inputs, which is how the tasks depend on each other:

```ts
import { Bundle, Dag } from "apache-airflow-ts-sdk";

const dag = new Dag("ts_hello", { schedule: "@daily", queue: "typescript" });

const extract = dag.task("extract", async (): Promise<number> => 42);
const report = dag.task("report", async ({ rows }: { rows: number }) => {
  console.log(`extracted ${rows} rows`);
});

report({ rows: extract() });

await new Bundle(dag).serve();
```

Build it into a single file:

```bash
npx airflow-ts-pack src/main.ts --outdir dist
```

Put `dist/bundle.min.mjs` in a Dag bundle, such as the default `dags-folder` bundle, and send the `typescript`
queue to the Node.js coordinator in `airflow.cfg`:

```ini
[sdk]
coordinators = {"ts": {"classpath": "airflow.sdk.coordinators.node.NodeCoordinator"}}
queue_to_coordinator = {"typescript": "ts"}
```

See the
[TypeScript SDK guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/language-sdks/typescript.html)
for everything a Dag declared in TypeScript can do, and for building and deploying a bundle.

## API Reference

The reference is generated directly from the TypeScript sources with
[TypeDoc](https://typedoc.org/). Use the sidebar or the search box to browse the
API.
