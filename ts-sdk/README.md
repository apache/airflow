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

# Airflow TypeScript SDK

A **Node.js** SDK for Apache Airflow. Run Airflow tasks as TypeScript handlers
in a Node worker, dispatched via the [Edge Executor](https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/index.html)
([AIP-69](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-69+Edge+Executor))
through the [Task Execution API](https://airflow.apache.org/docs/apache-airflow/stable/task-sdk-api.html)
([AIP-72](https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-72+Task+Execution+Interface+aka+Task+SDK)).

**Status:** 🚧 alpha. Under active development. API will change.
**Runtime:** Node 22+ only. ESM-only.

## Building

```bash
pnpm install
pnpm run build
pnpm test
```

Uses **pnpm** (declared in `package.json`'s `packageManager` field — Node 22+
with corepack enabled will pick the right version automatically).

## Architecture

Workers are **standalone Node processes** that connect to Airflow over HTTPS:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Apache Airflow                                   │
│  ┌──────────┐         ┌────────────────┐         ┌──────────────────┐   │
│  │Scheduler │────────▶│  Edge Executor │────────▶│  Edge API Server │   │
│  └──────────┘         └────────────────┘         └────────┬─────────┘   │
│                                                            │ HTTPS      │
└────────────────────────────────────────────────────────────┼────────────┘
                                                             │
                       ┌─────────────────────────────────────┘
                       ▼
                 ┌──────────────────────────┐
                 │  Node.js Worker Process  │
                 │   (uses this SDK)        │
                 │                          │
                 │  startWorker({ ... })    │
                 │   ↓ polls Edge API       │
                 │  registerTask("foo", fn) │
                 │   ↓ runs handler         │
                 └──────────────────────────┘
```

DAG authors declare TypeScript-backed tasks in Python via a companion
operator package (distributed separately):

```python
# dags/hello_typescript_dag.py
from airflow.sdk import DAG
from airflow_ts_operator import TypeScriptOperator

with DAG("hello_typescript", schedule=None, start_date=...) as dag:
    TypeScriptOperator(task_id="hello_typescript", queue="ts-tasks")
```

## Quickstart

```ts
// worker.ts
import { registerTask, startWorker } from "@apache-airflow/ts-sdk";

registerTask("hello_typescript", async ({ ctx }) => {
    console.log(`running ${ctx.dagId}/${ctx.taskId}`, {
        runId: ctx.runId,
        tryNumber: ctx.tryNumber,
    });
    return { ok: true };
});

await startWorker({
    queues: ["ts-tasks"],
    // baseUrl + secret default to env: AIRFLOW__EDGE__API_URL, AIRFLOW__API_AUTH__JWT_SECRET
});
```

Run the worker:

```bash
node worker.ts                   # Node 23.6+ (default)
# or:
npx tsx worker.ts                # any Node 22+
```

The worker registers with the Edge API, polls for jobs on `ts-tasks`,
runs the registered handler, and reports success/failure back.

## Testing

See [`TESTING.md`](TESTING.md) for the full end-to-end recipe (breeze
setup, reference workers, scenarios).
