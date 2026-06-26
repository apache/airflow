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

Public TypeScript interfaces for writing Apache Airflow task handlers.

**Status:** alpha · API will change · Node 22+ · ESM-only

This package defines the user-facing task handler contract: task registration,
runtime context types, and the `TaskClient` interface used for Airflow
Variables, Connections, and XCom. Runtime transports implement this interface
separately.

## Install

```bash
pnpm add @apache-airflow/ts-sdk
```

## Task Handlers

```ts
import { registerTask, type TaskHandlerArgs } from "@apache-airflow/ts-sdk";

export async function sayHello({ ctx, client }: TaskHandlerArgs) {
  const greeting = await client.getVariable("greeting");
  return { message: `Hello from ${ctx.taskId}: ${greeting}` };
}

registerTask({ dagId: "example_dag", taskId: "say_hello" }, sayHello);
```

Non-`undefined` return values are pushed to XCom under the `"return_value"`
key by the active runtime, matching Python `@task` behavior.

## Intended Coordinator Usage

This PR only adds the TypeScript-side public interface. The coordinator runtime
will be added separately. Declaring Airflow Dags in TypeScript is not supported
yet; the Dag is still declared in Python. The intended authoring shape matches
the other non-Python SDKs: a Python Dag declares the scheduling shape with stub
tasks, and the TypeScript module registers handlers with matching task IDs.

Python Dag:

```python
from airflow.sdk import dag, task


@dag
def sales_pipeline():
    @task.stub(queue="typescript")
    def extract(): ...

    @task.stub(queue="typescript")
    def transform(extracted): ...

    transform(extract())


sales_pipeline()
```

TypeScript handlers:

```ts
import { registerTask, type TaskHandlerArgs } from "@apache-airflow/ts-sdk";

export async function extract({ client }: TaskHandlerArgs) {
  const connection = await client.getConnection("sales_db");
  const rowCount = Number((await client.getVariable("daily_row_count")) ?? "0");

  return {
    connectionId: connection?.id ?? null,
    rowCount,
  };
}

export async function transform({ client }: TaskHandlerArgs) {
  const extracted = await client.getXCom<{ rowCount: number }>({
    key: "return_value",
    taskId: "extract",
  });

  return {
    transformedRows: extracted?.rowCount ?? 0,
  };
}

registerTask({ dagId: "sales_pipeline", taskId: "extract" }, extract);
registerTask({ dagId: "sales_pipeline", taskId: "transform" }, transform);
```

The Python stub defines the Dag dependency graph. The TypeScript handler does
the work and uses `TaskClient` for task-time Airflow data access. Register each
handler with the Python Dag's `dag_id` and the stub task's `task_id`. The
handler function is the reusable task implementation; `registerTask` binds that
handler to a Python stub Dag/task identity for coordinator mode. A future
TypeScript Dag authoring API can attach the same handlers without changing the
handler code.

## TaskClient

Every task handler receives a `TaskClient` for task-time Airflow data access:

| Method                                    | Description         |
| ----------------------------------------- | ------------------- |
| `getVariable(key)` / `getVariableOrThrow` | Airflow Variables   |
| `getXCom(opts)` / `setXCom(opts)`         | XCom read/write     |
| `getConnection(connId)`                   | Airflow Connections |

Locator fields such as `dagId`, `runId`, and `taskId` default to the
current task context when omitted.

## Cancellation

`ctx.signal` is an `AbortSignal` controlled by the active runtime. Pass it to
`fetch()`, timers, database clients, child processes, or other abortable APIs
so tasks can clean up cooperatively when Airflow terminates the task attempt.

## Development

```bash
pnpm install
pnpm test
pnpm run typecheck
pnpm run build
```
